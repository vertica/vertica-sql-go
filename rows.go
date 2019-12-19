package vertigo

// Copyright (c) 2019 Micro Focus or one of its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vertica/vertica-sql-go/common"
	"github.com/vertica/vertica-sql-go/msgs"
)

type rows struct {
	columnDefs *msgs.BERowDescMsg
	resultData []*msgs.BEDataRowMsg

	readIndex     int
	tzOffset      string
	inMemRowLimit int
	resultCache   *os.File
	cachingFailed bool
	scratch       [512]byte
}

var (
	paddingString        = "000000"
	defaultRowBufferSize = 256
)

// Columns returns the names of all of the columns
// Interface: driver.Rows
func (r *rows) Columns() []string {
	columnLabels := make([]string, len(r.columnDefs.Columns))
	for idx, cd := range r.columnDefs.Columns {
		columnLabels[idx] = cd.FieldName
	}
	return columnLabels
}

// Close closes the read cursor
// Interface: driver.Rows
func (r *rows) Close() error {
	if r.resultCache != nil {
		name := r.resultCache.Name()
		r.resultCache.Close()
		return os.Remove(name)
	}

	return nil
}

// Returns true if there was any data remaining to be loaded.
func (r *rows) reloadFromCache() bool {
	hadData := false

	r.readIndex = 0
	indexCount := 0

	for true {
		sizeBuf := r.scratch[:4]

		if _, err := r.resultCache.Read(sizeBuf); err != nil {
			if err == io.EOF {
				if indexCount == 0 {
					return false
				}
				fmt.Printf("reach EOF reading.. setting result data to %d\n", indexCount)
				r.resultData = r.resultData[0:indexCount]
				return true
			} else {
				return false
			}
		}

		rowDataSize := binary.LittleEndian.Uint32(sizeBuf)

		var rowBuf []byte
		rowBytes := r.scratch[4:]
		if rowDataSize <= uint32(len(rowBytes)) {
			rowBuf = rowBytes[:rowDataSize]
		} else {
			rowBuf = make([]byte, rowDataSize)
		}
		if _, err := r.resultCache.Read(rowBuf); err != nil {
			return false
		}

		msgBuf := msgs.NewMsgBufferFromBytes(rowBuf)

		drm := &msgs.BEDataRowMsg{}

		msg, _ := drm.CreateFromMsgBody(msgBuf)

		r.resultData[indexCount] = msg.(*msgs.BEDataRowMsg)
		indexCount++

		hadData = true

		// If we've reached the original capacity of the slice, we're done.
		if indexCount == len(r.resultData) {
			break
		}
	}

	return hadData
}

func (r *rows) Next(dest []driver.Value) error {
	if r.readIndex == len(r.resultData) {
		if r.resultCache != nil {
			if !r.reloadFromCache() {
				return io.EOF
			}
		} else {
			return io.EOF
		}
	}

	thisRow := r.resultData[r.readIndex]

	for idx, colVal := range thisRow.RowData {
		if colVal == nil {
			dest[idx] = nil
			continue
		}

		switch r.columnDefs.Columns[idx].DataTypeOID {
		case common.ColTypeBoolean: // to boolean
			dest[idx] = colVal[0] == 't'
		case common.ColTypeInt64: // to integer
			dest[idx], _ = strconv.Atoi(string(colVal))
		case common.ColTypeVarChar, common.ColTypeLongVarChar, common.ColTypeChar, common.ColTypeUUID: // stays string, convert char to string
			dest[idx] = string(colVal)
		case common.ColTypeFloat64, common.ColTypeNumeric: // to float64
			dest[idx], _ = strconv.ParseFloat(string(colVal), 64)
		case common.ColTypeTimestamp: // to time.Time from YYYY-MM-DD hh:mm:ss
			dest[idx], _ = parseTimestampTZColumn(string(colVal) + r.tzOffset)
		case common.ColTypeTimestampTZ:
			dest[idx], _ = parseTimestampTZColumn(string(colVal))
		case common.ColTypeVarBinary, common.ColTypeLongVarBinary, common.ColTypeBinary: // to []byte - this one's easy
			dest[idx] = hex.EncodeToString(colVal)
		default:
			dest[idx] = string(colVal)
		}
	}

	r.readIndex++

	return nil
}

func parseTimestampTZColumn(fullString string) (driver.Value, error) {
	var result driver.Value
	var err error

	if strings.IndexByte(fullString, '.') != -1 {
		neededPadding := 29 - len(fullString)
		if neededPadding > 0 {
			fullString = fullString[0:26-neededPadding] + paddingString[0:neededPadding] + fullString[len(fullString)-3:]
		}
		result, err = time.Parse("2006-01-02 15:04:05.000000-07", fullString)
	} else {
		result, err = time.Parse("2006-01-02 15:04:05-07", fullString)
	}

	return result, err
}

func (r *rows) finalize() {
	if r.resultCache != nil {
		name := r.resultCache.Name()
		r.resultCache.Close()

		r.resultCache, _ = os.OpenFile(name, os.O_RDONLY|os.O_EXCL, 0600)
	}
}

func (r *rows) writeCachedRow(rowData *msgs.BEDataRowMsg) {
	b := rowData.RevertToBytes()
	sizeBuf := r.scratch[:4]
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(b)))
	r.resultCache.Write(sizeBuf)
	r.resultCache.Write(b)
}

func (r *rows) addRow(rowData *msgs.BEDataRowMsg) {
	if r.resultCache != nil {
		r.writeCachedRow(rowData)
		return
	}

	if r.inMemRowLimit > 0 && !r.cachingFailed && len(r.resultData) == r.inMemRowLimit {
		var err error
		r.resultCache, err = ioutil.TempFile("", ".vertica-sql-go.*.dat")

		if err != nil {
			r.cachingFailed = true
			r.resultData = append(r.resultData, rowData)
		} else {
			r.writeCachedRow(rowData)
			return
		}
	}

	r.resultData = append(r.resultData, rowData)
}

func newRows(ctx context.Context, columnsDefsMsg *msgs.BERowDescMsg, tzOffset string) *rows {

	rowBufferSize := defaultRowBufferSize
	inMemRowLimit := 0

	if vCtx, ok := ctx.(VerticaContext); ok {
		rowBufferSize = vCtx.GetInMemoryResultRowLimit()
		inMemRowLimit = rowBufferSize
	}

	res := &rows{
		columnDefs:    columnsDefsMsg,
		resultData:    make([]*msgs.BEDataRowMsg, 0, rowBufferSize),
		tzOffset:      tzOffset,
		inMemRowLimit: inMemRowLimit,
		resultCache:   nil,
		cachingFailed: false,
	}

	return res
}

func newEmptyRows() *rows {
	cdf := make([]*msgs.BERowDescColumnDef, 0)
	be := &msgs.BERowDescMsg{Columns: cdf}
	return newRows(context.Background(), be, "")
}
