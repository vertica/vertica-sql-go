package common

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

import "fmt"

const (
	ColTypeBoolean       uint32 = 5
	ColTypeInt64         uint32 = 6
	ColTypeFloat64       uint32 = 7
	ColTypeChar          uint32 = 8
	ColTypeVarChar       uint32 = 9
	ColTypeDate          uint32 = 10
	ColTypeTime          uint32 = 11
	ColTypeTimestamp     uint32 = 12
	ColTypeTimestampTZ   uint32 = 13
	ColTypeInterval      uint32 = 14
	ColTypeIntervalYM    uint32 = 114
	ColTypeTimeTZ        uint32 = 15
	ColTypeNumeric       uint32 = 16
	ColTypeVarBinary     uint32 = 17
	ColTypeUUID          uint32 = 20
	ColTypeLongVarChar   uint32 = 115
	ColTypeLongVarBinary uint32 = 116
	ColTypeBinary        uint32 = 117
)

// Authentication Response/States
const (
	AuthenticationOK                int32 = 0
	AuthenticationCleartextPassword int32 = 3
	AuthenticationMD5Password       int32 = 5
	AuthenticationSHA512Password    int32 = 66048
)

type ParameterType struct {
	TypeOID      uint32
	TypeName     string
	TypeModifier int32
	Nullable     bool
}

func ColumnTypeString(typeOID uint32) string {
	switch typeOID {
	case ColTypeBoolean:
		return "BOOL"
	case ColTypeInt64:
		return "INT"
	case ColTypeFloat64:
		return "FLOAT"
	case ColTypeChar:
		return "CHAR"
	case ColTypeVarChar:
		return "VARCHAR"
	case ColTypeDate:
		return "DATE"
	case ColTypeTime:
		return "TIME"
	case ColTypeTimestamp:
		return "TIMESTAMP"
	case ColTypeTimestampTZ:
		return "TIMESTAMPTZ"
	case ColTypeInterval:
		return "INTERVAL"
	case ColTypeIntervalYM:
		return "INTERVALYM"
	case ColTypeTimeTZ:
		return "TIMETZ"
	case ColTypeNumeric:
		return "NUMERIC"
	case ColTypeVarBinary:
		return "VARBINARY"
	case ColTypeUUID:
		return "UUID"
	case ColTypeLongVarChar:
		return "LONG VARCHAR"
	case ColTypeLongVarBinary:
		return "LONG VARBINARY"
	case ColTypeBinary:
		return "BINARY"
	}

	return fmt.Sprintf("unknown column type oid: %d", typeOID)
}
