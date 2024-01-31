package msgs

import (
	"bytes"
	"database/sql/driver"
	"testing"
)

// Copyright (c) 2019-2024 Open Text.
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

func TestFlatten(t *testing.T) {
	var testCases = []struct {
		name     string
		value    driver.NamedValue
		expected []byte
	}{
		{
			name: "plain integer",
			value: driver.NamedValue{
				Name:    "arg1",
				Ordinal: 1,
				Value:   int64(123),
			},
			expected: []byte{0x0, 0x0, 0x0, 0x3, 0x31, 0x32, 0x33, 0x0, 0x0},
		},
		{
			name: "naked nil",
			value: driver.NamedValue{
				Name:    "arg1",
				Ordinal: 1,
				Value:   nil,
			},
			expected: []byte{0xff, 0xff, 0xff, 0xff, 0x0, 0x0},
		},
		{
			name: "string",
			value: driver.NamedValue{
				Name:    "arg1",
				Ordinal: 1,
				Value:   "hello",
			},
			expected: []byte{0x0, 0x0, 0x0, 0x5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0, 0x0},
		},
		{
			name: "float",
			value: driver.NamedValue{
				Name:    "arg1",
				Ordinal: 1,
				Value:   float64(12.6),
			},
			expected: []byte{0x0, 0x0, 0x0, 0x4, 0x31, 0x32, 0x2e, 0x36, 0x0, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := FEBindMsg{
				Portal:    "",
				Statement: "",
				NamedArgs: []driver.NamedValue{tc.value},
				OIDTypes:  nil,
			}
			result, _ := msg.Flatten()
			if bytes.Contains(result, []byte("??HELP??")) {
				t.Error("fell into a bad place, got the panic message")
			}
			// Trim those first 6 identical bytes for simplicity
			if !bytes.Equal(result[6:], tc.expected) {
				t.Errorf("got %#v expected %#v for message", result, tc.expected)
			}
		})
	}
}
