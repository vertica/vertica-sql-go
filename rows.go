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
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/vertica/vertica-sql-go/common"
	"github.com/vertica/vertica-sql-go/msgs"
)

type rows struct {
	columnDefs *msgs.BERowDescMsg
	resultData []*msgs.BEDataRowMsg

	readIndex  int
	writeIndex int
	tzOffset   string
}

var emptyRowSet *rows
var paddingString = "000000"

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
	return nil
}

// Next docs
// Interface: driver.Rows
func (r *rows) Next(dest []driver.Value) error {
	if r.readIndex == r.writeIndex {
		return io.EOF
	}

	if len(dest) != len(r.columnDefs.Columns) {
		return fmt.Errorf("rows.Next(): dest len %d is not equal to column len %d", len(dest), len(r.columnDefs.Columns))
	}

	thisRow := r.resultData[r.readIndex]

	for idx, colVal := range thisRow.RowData {

		switch r.columnDefs.Columns[idx].DataTypeOID {
		case common.ColTypeBoolean: // to boolean
			if colVal == nil {
				dest[idx] = sql.NullBool{}
			} else {
				if colVal[0] == 't' {
					dest[idx] = true
				} else {
					dest[idx] = false
				}
			}
		case common.ColTypeInt64: // to integer
			if colVal == nil {
				dest[idx] = sql.NullInt64{}
			} else {
				dest[idx], _ = strconv.Atoi(string(colVal))
			}
		case common.ColTypeVarChar, common.ColTypeLongVarChar, common.ColTypeChar, common.ColTypeUUID: // stays string, convert char to string
			if colVal == nil {
				dest[idx] = sql.NullString{}
			} else {
				dest[idx] = string(colVal)
			}
		case common.ColTypeFloat64, common.ColTypeNumeric: // to float64
			if colVal == nil {
				dest[idx] = sql.NullFloat64{}
			} else {
				dest[idx], _ = strconv.ParseFloat(string(colVal), 64)
			}
		case common.ColTypeTimestamp: // to time.Time from YYYY-MM-DD hh:mm:ss
			if colVal == nil {
				dest[idx] = sql.NullString{}
			} else {
				dest[idx], _ = parseTimestampTZColumn(string(colVal) + r.tzOffset)
			}
		case common.ColTypeTimestampTZ:
			if colVal == nil {
				dest[idx] = sql.NullString{}
			} else {
				dest[idx], _ = parseTimestampTZColumn(string(colVal))
			}
		case common.ColTypeVarBinary, common.ColTypeLongVarBinary, common.ColTypeBinary: // to []byte - this one's easy
			if colVal == nil {
				dest[idx] = sql.NullString{}
			} else {
				dest[idx] = hex.EncodeToString(colVal)
			}
		default:
			if colVal == nil {
				dest[idx] = sql.NullString{}
			} else {
				dest[idx] = string(colVal)
			}
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

func (r *rows) addRow(resultData *msgs.BEDataRowMsg) {

	if r.writeIndex == cap(r.resultData) {
		newSlice := make([]*msgs.BEDataRowMsg, 2*r.writeIndex)
		copy(newSlice, r.resultData)
		r.resultData = newSlice
	} else {
		r.resultData = r.resultData[0 : r.writeIndex+1]
	}

	r.resultData[r.writeIndex] = resultData

	r.writeIndex++
}

func newRows(columnsDefsMsg *msgs.BERowDescMsg, tzOffset string) *rows {
	res := &rows{
		columnDefs: columnsDefsMsg,
		resultData: make([]*msgs.BEDataRowMsg, 64),
		tzOffset:   tzOffset,
	}

	return res
}

func init() {
	cdf := make([]*msgs.BERowDescColumnDef, 0)
	be := &msgs.BERowDescMsg{Columns: cdf}
	emptyRowSet = newRows(be, "")
}
