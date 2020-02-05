package vertigo

// Copyright (c) 2020 Micro Focus or one of its affiliates.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import (
	"database/sql/driver"
	"testing"
)

func testStatement(command string) *stmt {
	return &stmt{
		command:      command,
		preparedName: "TestCommand",
		parseState:   parseStateUnparsed,
	}
}

func TestInterpolate(t *testing.T) {
	var testCases = []struct {
		name     string
		command  string
		expected string
		args     []driver.NamedValue
	}{
		{
			name:     "no parameters",
			command:  "select * from something",
			expected: "select * from something",
			args:     []driver.NamedValue{},
		},
		{
			name:     "simple string",
			command:  "select * from something where value = ?",
			expected: "select * from something where value = 'taco'",
			args:     []driver.NamedValue{{Value: "taco"}},
		},
		{
			name:     "multiple values",
			command:  "select * from something where value = ? and otherVal = ?",
			expected: "select * from something where value = 'taco' and otherVal = 15.5",
			args:     []driver.NamedValue{{Value: "taco"}, {Value: 15.5}},
		},
		{
			name:     "strings with quotes",
			command:  "select * from something where value = ?",
			expected: "select * from something where value = 'it''s other''s'",
			args:     []driver.NamedValue{{Value: "it's other's"}},
		},
		{
			name:     "strings with already escaped quotes",
			command:  "select * from something where value = ?",
			expected: "select * from something where value = 'it''s other''s'",
			args:     []driver.NamedValue{{Value: "it''s other''s"}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := testStatement(tc.command)
			result, err := stmt.interpolate(tc.args)
			if result != tc.expected {
				t.Errorf("Expected query to be %s but got %s", tc.command, result)
			}
			if err != nil {
				t.Errorf("Received error from interpolate: %v", err)
			}
		})
	}
}

func TestCleanQuotes(t *testing.T) {
	var testCases = []struct {
		name     string
		val      string
		expected string
	}{
		{
			name:     "Already paired",
			val:      "isn''t",
			expected: "isn''t",
		},
		{
			name:     "Unpaired at end",
			val:      "pair it'''",
			expected: "pair it''''",
		},
		{
			name:     "Unpaired at start",
			val:      "'pair it",
			expected: "''pair it",
		},
		{
			name:     "multiple unpaired",
			val:      "isn't wasn't",
			expected: "isn''t wasn''t",
		},
		{
			name:     "simple fix",
			val:      "isn't",
			expected: "isn''t",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := testStatement("")
			result := stmt.cleanQuotes(tc.val)
			if result != tc.expected {
				t.Errorf("Expected result to be %s got %s", tc.expected, result)
			}
		})
	}
}
