package vertigo

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

import (
	"testing"
)

func TestUDSFAnalyzer_IsUDSFStatement(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "CREATE FUNCTION",
			sql:      "CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1' LANGUAGE SQL",
			expected: true,
		},
		{
			name:     "ALTER FUNCTION",
			sql:      "ALTER FUNCTION my_func() OWNER TO new_owner",
			expected: true,
		},
		{
			name:     "DROP FUNCTION",
			sql:      "DROP FUNCTION my_func()",
			expected: true,
		},
		{
			name:     "GRANT USAGE",
			sql:      "GRANT USAGE ON SCHEMA my_schema TO user1",
			expected: true,
		},
		{
			name:     "GRANT EXECUTE",
			sql:      "GRANT EXECUTE ON FUNCTION my_func() TO user1",
			expected: true,
		},
		{
			name:     "REVOKE USAGE",
			sql:      "REVOKE USAGE ON SCHEMA my_schema FROM user1",
			expected: true,
		},
		{
			name:     "REVOKE EXECUTE",
			sql:      "REVOKE EXECUTE ON FUNCTION my_func() FROM user1",
			expected: true,
		},
		{
			name:     "SELECT statement",
			sql:      "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "INSERT statement",
			sql:      "INSERT INTO users VALUES (1, 'name')",
			expected: false,
		},
		{
			name:     "UPDATE statement",
			sql:      "UPDATE users SET name = 'new' WHERE id = 1",
			expected: false,
		},
		{
			name:     "DELETE statement",
			sql:      "DELETE FROM users WHERE id = 1",
			expected: false,
		},
		{
			name:     "CREATE TABLE statement",
			sql:      "CREATE TABLE users (id INT, name VARCHAR)",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.IsUDSFStatement(tt.sql)
			if result != tt.expected {
				t.Errorf("IsUDSFStatement(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestUDSFAnalyzer_GetStatementType(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name         string
		sql          string
		expectedType UDSFStatementType
	}{
		{
			name:         "CREATE FUNCTION simple",
			sql:          "CREATE FUNCTION my_func() RETURNS INT",
			expectedType: UDSFStatementTypeCreateFunction,
		},
		{
			name:         "CREATE FUNCTION with comments",
			sql:          "-- comment\nCREATE FUNCTION my_func() RETURNS INT",
			expectedType: UDSFStatementTypeCreateFunction,
		},
		{
			name:         "ALTER FUNCTION",
			sql:          "ALTER FUNCTION my_func() OWNER TO new_owner",
			expectedType: UDSFStatementTypeAlterFunction,
		},
		{
			name:         "DROP FUNCTION",
			sql:          "DROP FUNCTION IF EXISTS my_func()",
			expectedType: UDSFStatementTypeDropFunction,
		},
		{
			name:         "GRANT USAGE",
			sql:          "GRANT USAGE ON SCHEMA my_schema TO user1",
			expectedType: UDSFStatementTypeGrantUsage,
		},
		{
			name:         "GRANT EXECUTE",
			sql:          "GRANT EXECUTE ON FUNCTION my_func() TO user1",
			expectedType: UDSFStatementTypeGrantExecute,
		},
		{
			name:         "REVOKE USAGE",
			sql:          "REVOKE USAGE ON SCHEMA my_schema FROM user1",
			expectedType: UDSFStatementTypeRevokeUsage,
		},
		{
			name:         "REVOKE EXECUTE",
			sql:          "REVOKE EXECUTE ON FUNCTION my_func() FROM user1",
			expectedType: UDSFStatementTypeRevokeExecute,
		},
		{
			name:         "SELECT (non-UDSF)",
			sql:          "SELECT * FROM users",
			expectedType: UDSFStatementTypeUnknown,
		},
		{
			name:         "INSERT (non-UDSF)",
			sql:          "INSERT INTO users VALUES (1)",
			expectedType: UDSFStatementTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := analyzer.GetStatementType(tt.sql)
			if result != tt.expectedType {
				t.Errorf("GetStatementType(%q) = %v, want %v", tt.sql, result, tt.expectedType)
			}
		})
	}
}

func TestUDSFAnalyzer_TokenizeTopLevel(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name          string
		sql           string
		expectedCount int
		checkTokens   []struct {
			index    int
			expected string
		}
	}{
		{
			name:          "Simple CREATE FUNCTION",
			sql:           "CREATE FUNCTION my_func() RETURNS INT",
			expectedCount: 5,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "CREATE"},
				{1, "FUNCTION"},
				{2, "my_func"},
				{3, "RETURNS"},
				{4, "INT"},
			},
		},
		{
			name:          "Ignore tokens in single quotes",
			sql:           "CREATE FUNCTION 'CREATE' AS 'SELECT 1'",
			expectedCount: 3,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "CREATE"},
				{1, "FUNCTION"},
				{2, "AS"},
			},
		},
		{
			name:          "Ignore tokens in double quotes",
			sql:           `CREATE FUNCTION "CREATE" AS 'SELECT 1'`,
			expectedCount: 3,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "CREATE"},
				{1, "FUNCTION"},
				{2, "AS"},
			},
		},
		{
			name:          "Ignore tokens in line comments",
			sql:           "-- CREATE TABLE\nCREATE FUNCTION my_func",
			expectedCount: 3,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "CREATE"},
				{1, "FUNCTION"},
				{2, "my_func"},
			},
		},
		{
			name:          "Ignore tokens in block comments",
			sql:           "/* CREATE TABLE */ CREATE FUNCTION my_func",
			expectedCount: 3,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "CREATE"},
				{1, "FUNCTION"},
				{2, "my_func"},
			},
		},
		{
			name:          "Handle mixed case",
			sql:           "create function my_func returns int",
			expectedCount: 5,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{0, "create"},
				{1, "function"},
				{2, "my_func"},
			},
		},
		{
			name:          "Handle underscores in identifiers",
			sql:           "CREATE FUNCTION my_func_name RETURNS INT",
			expectedCount: 5,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{2, "my_func_name"},
			},
		},
		{
			name:          "Handle numbers in identifiers",
			sql:           "CREATE FUNCTION func123 RETURNS INT",
			expectedCount: 5,
			checkTokens: []struct {
				index    int
				expected string
			}{
				{2, "func123"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := analyzer.tokenizeTopLevel(tt.sql)
			if len(tokens) != tt.expectedCount {
				t.Errorf("tokenizeTopLevel(%q) returned %d tokens, want %d. Tokens: %v",
					tt.sql, len(tokens), tt.expectedCount, tokens)
			}

			for _, check := range tt.checkTokens {
				if check.index >= len(tokens) {
					t.Errorf("token index %d out of range", check.index)
					continue
				}
				if tokens[check.index] != check.expected {
					t.Errorf("token[%d] = %q, want %q", check.index, tokens[check.index], check.expected)
				}
			}
		})
	}
}

func TestUDSFAnalyzer_ShouldTreatAsAtomicUnit(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "CREATE FUNCTION should be atomic",
			sql:      "CREATE FUNCTION my_func() RETURNS INT",
			expected: true,
		},
		{
			name:     "ALTER FUNCTION should be atomic",
			sql:      "ALTER FUNCTION my_func() OWNER TO new_owner",
			expected: true,
		},
		{
			name:     "DROP FUNCTION should be atomic",
			sql:      "DROP FUNCTION my_func()",
			expected: true,
		},
		{
			name:     "GRANT USAGE should be atomic",
			sql:      "GRANT USAGE ON SCHEMA my_schema TO user1",
			expected: true,
		},
		{
			name:     "GRANT EXECUTE should be atomic",
			sql:      "GRANT EXECUTE ON FUNCTION my_func() TO user1",
			expected: true,
		},
		{
			name:     "REVOKE USAGE should be atomic",
			sql:      "REVOKE USAGE ON SCHEMA my_schema FROM user1",
			expected: true,
		},
		{
			name:     "REVOKE EXECUTE should be atomic",
			sql:      "REVOKE EXECUTE ON FUNCTION my_func() FROM user1",
			expected: true,
		},
		{
			name:     "SELECT should not be atomic",
			sql:      "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "INSERT should not be atomic",
			sql:      "INSERT INTO users VALUES (1)",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.ShouldTreatAsAtomicUnit(tt.sql)
			if result != tt.expected {
				t.Errorf("ShouldTreatAsAtomicUnit(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestUDSFAnalyzer_ComplexFunctionBody(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	// Test with complex multi-line function body containing semicolons, comments, and CASE logic
	complexFunction := `CREATE FUNCTION calculate_value(int_param INT)
	RETURNS INT AS
	$$
	DECLARE
	  result INT := 0;
	BEGIN
	  -- Check parameter value
	  result := CASE
	    WHEN int_param < 0 THEN -1
	    WHEN int_param = 0 THEN 0
	    ELSE 1
	  END;
	  RETURN result;
	END;
	$$
	LANGUAGE PLPGSQL;`

	result := analyzer.IsUDSFStatement(complexFunction)
	if !result {
		t.Errorf("IsUDSFStatement should recognize complex function body as UDSF statement")
	}

	stmtType, err := analyzer.GetStatementType(complexFunction)
	if err != nil {
		t.Errorf("GetStatementType returned unexpected error: %v", err)
	}
	if stmtType != UDSFStatementTypeCreateFunction {
		t.Errorf("GetStatementType = %v, want %v", stmtType, UDSFStatementTypeCreateFunction)
	}

	shouldBeAtomic := analyzer.ShouldTreatAsAtomicUnit(complexFunction)
	if !shouldBeAtomic {
		t.Errorf("Complex function body should be treated as atomic unit")
	}
}

func TestUDSFAnalyzer_FunctionWithStringLiterals(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	// Test with function containing string literals with semicolons
	functionWithSemicolons := `CREATE FUNCTION string_test()
	RETURNS VARCHAR AS
	$$
	SELECT 'value with; semicolon' AS col;
	$$
	LANGUAGE SQL;`

	result := analyzer.IsUDSFStatement(functionWithSemicolons)
	if !result {
		t.Errorf("IsUDSFStatement should recognize function with semicolons in strings")
	}

	stmtType, _ := analyzer.GetStatementType(functionWithSemicolons)
	if stmtType != UDSFStatementTypeCreateFunction {
		t.Errorf("Failed to detect CREATE FUNCTION with string semicolons")
	}
}

func TestUDSFAnalyzer_FunctionWithDollarQuoting(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	// Test with dollar-quoted string
	dollarQuotedFunction := `CREATE FUNCTION dollar_test()
	RETURNS VARCHAR AS
	$tag$
	SELECT 'value with; semicolon' AS col;
	$tag$
	LANGUAGE SQL;`

	result := analyzer.IsUDSFStatement(dollarQuotedFunction)
	if !result {
		t.Errorf("IsUDSFStatement should recognize function with dollar quoting")
	}

	stmtType, _ := analyzer.GetStatementType(dollarQuotedFunction)
	if stmtType != UDSFStatementTypeCreateFunction {
		t.Errorf("Failed to detect CREATE FUNCTION with dollar quoting")
	}
}

func TestUDSFAnalyzer_GrantWithMultipleUsers(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	grantMultiple := `GRANT EXECUTE ON FUNCTION my_func(INT, VARCHAR)
	TO user1, user2, user3
	WITH GRANT OPTION;`

	result := analyzer.IsUDSFStatement(grantMultiple)
	if !result {
		t.Errorf("IsUDSFStatement should recognize GRANT with multiple users")
	}

	stmtType, _ := analyzer.GetStatementType(grantMultiple)
	if stmtType != UDSFStatementTypeGrantExecute {
		t.Errorf("Failed to detect GRANT EXECUTE")
	}
}

func TestUDSFAnalyzer_RevokeWithCascade(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	revokeCascade := `REVOKE USAGE ON SCHEMA my_schema
	FROM user1 CASCADE;`

	result := analyzer.IsUDSFStatement(revokeCascade)
	if !result {
		t.Errorf("IsUDSFStatement should recognize REVOKE USAGE")
	}

	stmtType, _ := analyzer.GetStatementType(revokeCascade)
	if stmtType != UDSFStatementTypeRevokeUsage {
		t.Errorf("Failed to detect REVOKE USAGE")
	}
}

func TestUDSFAnalyzer_EmptyStatement(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	_, err := analyzer.GetStatementType("")
	if err == nil {
		t.Errorf("GetStatementType should return error for empty statement")
	}

	result := analyzer.IsUDSFStatement("")
	if result {
		t.Errorf("IsUDSFStatement should return false for empty statement")
	}
}

func TestUDSFAnalyzer_OnlyComments(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	_, err := analyzer.GetStatementType("-- just a comment")
	if err == nil {
		t.Errorf("GetStatementType should return error for comment-only statement")
	}

	result := analyzer.IsUDSFStatement("-- just a comment")
	if result {
		t.Errorf("IsUDSFStatement should return false for comment-only statement")
	}
}

func TestUDSFAnalyzer_CaseInsensitivity(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name     string
		sql      string
		expected UDSFStatementType
	}{
		{
			name:     "lowercase create function",
			sql:      "create function my_func() returns int",
			expected: UDSFStatementTypeCreateFunction,
		},
		{
			name:     "mixed case alter function",
			sql:      "AlTeR FuNcTiOn my_func() owner to new_owner",
			expected: UDSFStatementTypeAlterFunction,
		},
		{
			name:     "uppercase drop function",
			sql:      "DROP FUNCTION my_func()",
			expected: UDSFStatementTypeDropFunction,
		},
		{
			name:     "mixed case grant usage",
			sql:      "GrAnT UsAgE On ScHeMA my_schema to user1",
			expected: UDSFStatementTypeGrantUsage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := analyzer.GetStatementType(tt.sql)
			if result != tt.expected {
				t.Errorf("GetStatementType(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestUDSFAnalyzer_QuotedIdentifiers(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name     string
		sql      string
		expected UDSFStatementType
	}{
		{
			name:     `double-quoted function name`,
			sql:      `CREATE FUNCTION "MyFunc"() RETURNS INT`,
			expected: UDSFStatementTypeCreateFunction,
		},
		{
			name:     `schema-qualified function`,
			sql:      `CREATE FUNCTION "MySchema"."MyFunc"() RETURNS INT`,
			expected: UDSFStatementTypeCreateFunction,
		},
		{
			name:     `double-quoted schema in grant`,
			sql:      `GRANT USAGE ON SCHEMA "MySchema" TO user1`,
			expected: UDSFStatementTypeGrantUsage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := analyzer.GetStatementType(tt.sql)
			if result != tt.expected {
				t.Errorf("GetStatementType(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestUDSFAnalyzer_LeadingWhitespace(t *testing.T) {
	analyzer := NewUDSFAnalyzer()

	tests := []struct {
		name     string
		sql      string
		expected UDSFStatementType
	}{
		{
			name:     "leading spaces",
			sql:      "   CREATE FUNCTION my_func() RETURNS INT",
			expected: UDSFStatementTypeCreateFunction,
		},
		{
			name:     "leading newlines and tabs",
			sql:      "\n\t\n\tCREATE FUNCTION my_func() RETURNS INT",
			expected: UDSFStatementTypeCreateFunction,
		},
		{
			name:     "multiple blank lines",
			sql:      "\n\n\nALTER FUNCTION my_func() OWNER TO new_owner",
			expected: UDSFStatementTypeAlterFunction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := analyzer.GetStatementType(tt.sql)
			if result != tt.expected {
				t.Errorf("GetStatementType(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}
