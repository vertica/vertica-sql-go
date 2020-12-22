package parse

// Copyright (c) 2020 Micro Focus or one of its affiliates.
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

import "testing"

func TestSkipUntil(t *testing.T) {
	lexer := Lexer{input: "select * from a where test = 'b'"}
	lexer.skipUntil('\'')
	if cur := lexer.current(); cur != 'b' {
		t.Errorf("Expected to be on b but got %q", cur)
	}
	lexer.skipUntil('\'')
	if lexer.pos != len(lexer.input) {
		t.Errorf("Should be on %d, on %d with %q", len(lexer.input), lexer.pos, lexer.current())
	}
}

type nameCapture struct {
	names []string
}

func (n *nameCapture) reset() {
	n.names = n.names[:0]
}

func (n *nameCapture) store(name string) {
	n.names = append(n.names, name)
}

func TestLexNamed(t *testing.T) {
	var testCases = []struct {
		name           string
		query          string
		expectedNamed  []string
		expectedOutput string
	}{
		{
			name:           "empty string",
			query:          "",
			expectedNamed:  []string{},
			expectedOutput: "",
		},
		{
			name:           "simple query, no params",
			query:          "select * from whatever where a = 'test'",
			expectedNamed:  []string{},
			expectedOutput: "select * from whatever where a = 'test'",
		},
		{
			name:           "with some named parameters",
			query:          "select * from whatever where a = @first and b = @second and c = '@fooledYou'",
			expectedNamed:  []string{"FIRST", "SECOND"},
			expectedOutput: "select * from whatever where a = ? and b = ? and c = '@fooledYou'",
		},
		{
			name:           "named params with in clause",
			query:          "select * from whatever where a in (@first, @second)",
			expectedNamed:  []string{"FIRST", "SECOND"},
			expectedOutput: "select * from whatever where a in (?, ?)",
		},
		{
			name:           "single named param with in clause",
			query:          "select * from whatever where a in (@first)",
			expectedNamed:  []string{"FIRST"},
			expectedOutput: "select * from whatever where a in (?)",
		},
		{
			name:           "with mixed case named parameters",
			query:          "select * from whatever where a = @first and b = @fIrSt",
			expectedNamed:  []string{"FIRST", "FIRST"},
			expectedOutput: "select * from whatever where a = ? and b = ?",
		},
		{
			name:           "with a pre-escaped string",
			query:          "select * from whatever where a = @first and b = 'isn''t",
			expectedNamed:  []string{"FIRST"},
			expectedOutput: "select * from whatever where a = ? and b = 'isn''t",
		},
		{
			name:           "do not choke on malformed query string",
			query:          "select * from whatever where a = @first and b = 'isn'''t",
			expectedNamed:  []string{"FIRST"},
			expectedOutput: "select * from whatever where a = ? and b = 'isn'''t",
		},
		{
			name:           "with pre-escaped email string",
			query:          "insert into table values('fo''o@bar.com', 2020)",
			expectedNamed:  []string{},
			expectedOutput: "insert into table values('fo''o@bar.com', 2020)",
		},
		{
			name: "with a comment",
			query: `select --some select stuff
			* from whatever where a = @param`,
			expectedNamed: []string{"PARAM"},
			expectedOutput: `select --some select stuff
			* from whatever where a = ?`,
		},
		{
			name: "named params on line endings",
			query: `select
			* from table
			where
			a = @param1
			and b = @param2`,
			expectedNamed: []string{"PARAM1", "PARAM2"},
			expectedOutput: `select
			* from table
			where
			a = ?
			and b = ?`,
		},
		{
			name: "named params with ending newline",
			query: `select
			* from table
			where
			a = @param1
`,
			expectedNamed: []string{"PARAM1"},
			expectedOutput: `select
			* from table
			where
			a = ?
`,
		},
	}
	var encounteredNames nameCapture
	for _, tc := range testCases {
		encounteredNames.reset()
		t.Run(tc.name, func(t *testing.T) {
			result := Lex(tc.query, WithNamedCallback(encounteredNames.store))
			if result != tc.expectedOutput {
				t.Errorf("Expected query:\n%s\nGot:\n%s", tc.expectedOutput, result)
			}
			if len(encounteredNames.names) != len(tc.expectedNamed) {
				t.Errorf("Encountered %d named params, expected %d", len(encounteredNames.names), len(tc.expectedNamed))
			}
			for i := range encounteredNames.names {
				if encounteredNames.names[i] != tc.expectedNamed[i] {
					t.Errorf("Expected name at %d to be %s but got %s", i, tc.expectedNamed[i], encounteredNames.names[i])
				}
			}
		})
	}
}

func swapPos() string {
	return "'replaced'"
}
func TestLexPositional(t *testing.T) {
	var testCases = []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "single parameter",
			query:    "select * from table where a = ? and b = 2",
			expected: "select * from table where a = 'replaced' and b = 2",
		},
		{
			name:     "end on parameter",
			query:    "select * from table where a = ?",
			expected: "select * from table where a = 'replaced'",
		},
		{
			name:     "? hidden in a string",
			query:    "select * from table where a = ? and b = '?fooledYou'",
			expected: "select * from table where a = 'replaced' and b = '?fooledYou'",
		},
		{
			name: "? hidden in a comment",
			query: `select
			* from -- maybe broken?
			where a = ?`,
			expected: `select
			* from -- maybe broken?
			where a = 'replaced'`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Lex(tc.query, WithPositionalSubstitution(swapPos))
			if result != tc.expected {
				t.Errorf("Expected query:\n%s\nGot:\n%s", tc.expected, result)
			}
		})
	}
}
