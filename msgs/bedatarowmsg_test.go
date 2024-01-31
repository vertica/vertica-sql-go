package msgs

// Copyright (c) 2020-2024 Open Text.
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
	"bytes"
	"encoding/binary"
	"testing"
)

func mockRow() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 30))
	binary.Write(buf, binary.BigEndian, int16(4))
	binary.Write(buf, binary.BigEndian, int32(3))
	binary.Write(buf, binary.BigEndian, []byte("123"))
	binary.Write(buf, binary.BigEndian, int32(5))
	binary.Write(buf, binary.BigEndian, []byte("hello"))
	binary.Write(buf, binary.BigEndian, int32(1))
	binary.Write(buf, binary.BigEndian, false)
	binary.Write(buf, binary.BigEndian, int32(3))
	binary.Write(buf, binary.BigEndian, []byte("456"))
	return buf.Bytes()
}

func TestChunk(t *testing.T) {
	msgBuf := NewMsgBufferFromBytes(mockRow())
	var dMsg BEDataRowMsg
	msgI, _ := dMsg.CreateFromMsgBody(msgBuf)
	extractor := msgI.(*BEDataRowMsg).Columns()
	if extractor.NumCols != 4 {
		t.Errorf("Expected 4 columns but got %d", extractor.NumCols)
	}
	ch := extractor.Chunk()
	if str := string(ch); str != "123" {
		t.Errorf("Expected 123 got %s", str)
	}
	ch = extractor.Chunk()
	if str := string(ch); str != "hello" {
		t.Errorf("Expected hello got %s", str)
	}
	ch = extractor.Chunk()
	if ch[0] != byte(0) {
		t.Error("Expected false")
	}
	ch = extractor.Chunk()
	if str := string(ch); str != "456" {
		t.Errorf("Expected 456 got %s", str)
	}
}
