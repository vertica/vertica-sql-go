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
	"fmt"
	"strings"

	"github.com/vertica/vertica-sql-go/logger"
)

// UDSFStatementType represents the type of UDSF-related SQL statement.
type UDSFStatementType int

const (
	UDSFStatementTypeUnknown UDSFStatementType = iota
	UDSFStatementTypeCreateFunction
	UDSFStatementTypeAlterFunction
	UDSFStatementTypeDropFunction
	UDSFStatementTypeGrantUsage
	UDSFStatementTypeGrantExecute
	UDSFStatementTypeRevokeUsage
	UDSFStatementTypeRevokeExecute
)

// String returns the string representation of the UDSF statement type.
func (t UDSFStatementType) String() string {
	switch t {
	case UDSFStatementTypeCreateFunction:
		return "CREATE FUNCTION"
	case UDSFStatementTypeAlterFunction:
		return "ALTER FUNCTION"
	case UDSFStatementTypeDropFunction:
		return "DROP FUNCTION"
	case UDSFStatementTypeGrantUsage:
		return "GRANT USAGE"
	case UDSFStatementTypeGrantExecute:
		return "GRANT EXECUTE"
	case UDSFStatementTypeRevokeUsage:
		return "REVOKE USAGE"
	case UDSFStatementTypeRevokeExecute:
		return "REVOKE EXECUTE"
	default:
		return "UNKNOWN"
	}
}

var udsfLogger = logger.New("udsf")

// UDSFAnalyzer provides analysis and detection of UDSF-related SQL statements.
type UDSFAnalyzer struct{}

// NewUDSFAnalyzer creates a new UDSF analyzer instance.
func NewUDSFAnalyzer() *UDSFAnalyzer {
	return &UDSFAnalyzer{}
}

// IsUDSFStatement determines whether the given SQL statement is a UDSF-related statement
// that should be treated as an atomic unit and not split by the statement splitter.
// This includes CREATE FUNCTION, ALTER FUNCTION, DROP FUNCTION, GRANT/REVOKE statements
// for functions and schemas.
func (ua *UDSFAnalyzer) IsUDSFStatement(sql string) bool {
	stmtType, err := ua.GetStatementType(sql)
	if err != nil {
		return false
	}
	return stmtType != UDSFStatementTypeUnknown
}

// GetStatementType determines the type of the given SQL statement.
// Returns UDSFStatementTypeUnknown if the statement is not a recognized UDSF statement.
func (ua *UDSFAnalyzer) GetStatementType(sql string) (UDSFStatementType, error) {
	tokens := ua.tokenizeTopLevel(sql)
	if len(tokens) == 0 {
		return UDSFStatementTypeUnknown, fmt.Errorf("empty statement")
	}

	firstToken := strings.ToUpper(tokens[0])
	secondToken := ""
	if len(tokens) > 1 {
		secondToken = strings.ToUpper(tokens[1])
	}

	// Detect CREATE FUNCTION
	if firstToken == "CREATE" && secondToken == "FUNCTION" {
		return UDSFStatementTypeCreateFunction, nil
	}

	// Detect CREATE OR REPLACE FUNCTION
	if firstToken == "CREATE" && len(tokens) > 3 {
		thirdToken := strings.ToUpper(tokens[2])
		fourthToken := strings.ToUpper(tokens[3])
		if secondToken == "OR" && thirdToken == "REPLACE" && fourthToken == "FUNCTION" {
			return UDSFStatementTypeCreateFunction, nil
		}
	}

	// Detect ALTER FUNCTION
	if firstToken == "ALTER" && secondToken == "FUNCTION" {
		return UDSFStatementTypeAlterFunction, nil
	}

	// Detect DROP FUNCTION
	if firstToken == "DROP" && secondToken == "FUNCTION" {
		return UDSFStatementTypeDropFunction, nil
	}

	// Detect GRANT/REVOKE with USAGE or EXECUTE
	if firstToken == "GRANT" && (secondToken == "USAGE" || secondToken == "EXECUTE") {
		if secondToken == "USAGE" {
			return UDSFStatementTypeGrantUsage, nil
		}
		return UDSFStatementTypeGrantExecute, nil
	}

	if firstToken == "REVOKE" && (secondToken == "USAGE" || secondToken == "EXECUTE") {
		if secondToken == "USAGE" {
			return UDSFStatementTypeRevokeUsage, nil
		}
		return UDSFStatementTypeRevokeExecute, nil
	}

	return UDSFStatementTypeUnknown, nil
}

// tokenizeTopLevel extracts top-level SQL tokens while respecting quoted strings,
// comments, and dollar-quoted literals. This is a non-regex implementation that
// does not interpret or modify the SQL content.
func (ua *UDSFAnalyzer) tokenizeTopLevel(statement string) []string {
	tokens := make([]string, 0, 16)
	var current strings.Builder
	tokenStart := -1

	// Tokenize only top-level SQL words and ignore quoted/commented regions
	flushCurrent := func() {
		if current.Len() == 0 {
			return
		}
		tokens = append(tokens, current.String())
		current.Reset()
		tokenStart = -1
	}

	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false
	var dollarTag string

	for i := 0; i < len(statement); i++ {
		ch := statement[i]

		// Handle line comments: skip until newline
		if inLineComment {
			if ch == '\n' || ch == '\r' {
				inLineComment = false
			}
			continue
		}

		// Handle block comments: skip until */
		if inBlockComment {
			if ch == '*' && i+1 < len(statement) && statement[i+1] == '/' {
				i++
				inBlockComment = false
			}
			continue
		}

		// Handle single-quoted strings: doubled quotes are escaped
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(statement) && statement[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}

		// Handle double-quoted identifiers: doubled quotes are escaped
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(statement) && statement[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		// Handle dollar-quoted strings ($tag$...$tag$)
		if dollarTag != "" {
			if i+len(dollarTag) <= len(statement) && statement[i:i+len(dollarTag)] == dollarTag {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}

		// Detect start of single-quoted string
		if ch == '\'' {
			flushCurrent()
			inSingleQuote = true
			continue
		}

		// Detect start of double-quoted identifier
		if ch == '"' {
			flushCurrent()
			inDoubleQuote = true
			continue
		}

		// Detect line comment: --
		if ch == '-' && i+1 < len(statement) && statement[i+1] == '-' {
			flushCurrent()
			i++
			inLineComment = true
			continue
		}

		// Detect block comment: /* */ or line comment: //
		if ch == '/' && i+1 < len(statement) {
			next := statement[i+1]
			if next == '*' {
				flushCurrent()
				i++
				inBlockComment = true
				continue
			}
			if next == '/' {
				flushCurrent()
				i++
				inLineComment = true
				continue
			}
		}

		// Detect dollar-quoted string: $tag$
		if ch == '$' {
			if tag, length, ok := ua.readDollarTag(statement, i); ok {
				flushCurrent()
				dollarTag = tag
				i += length - 1
				continue
			}
		}

		// Accumulate alphanumeric characters and underscores as token characters
		if isTokenChar(ch) {
			if tokenStart < 0 {
				tokenStart = i
			}
			current.WriteByte(ch)
			continue
		}

		// Non-token character: flush current token
		flushCurrent()
	}

	flushCurrent()
	return tokens
}

// readDollarTag attempts to read a dollar-quoted string tag starting at position i.
// Returns the tag (including $), its length in characters, and a boolean indicating success.
func (ua *UDSFAnalyzer) readDollarTag(statement string, start int) (string, int, bool) {
	if start >= len(statement) || statement[start] != '$' {
		return "", 0, false
	}

	end := start + 1
	for end < len(statement) {
		if statement[end] == '$' {
			return statement[start : end+1], end + 1 - start, true
		}
		r := rune(statement[end])
		// Check if character is valid in dollar-quote tag
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_') {
			break
		}
		end++
	}

	return "", 0, false
}

// isTokenChar reports whether a character is valid in a SQL token (letter, digit, or underscore).
func isTokenChar(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') || ch == '_'
}

// ShouldTreatAsAtomicUnit determines whether a statement should not be split
// by the statement splitter. UDSF statements containing function bodies with
// embedded semicolons, comments, and nested constructs must be treated as
// atomic units to preserve their integrity.
func (ua *UDSFAnalyzer) ShouldTreatAsAtomicUnit(sql string) bool {
	// Only UDSF statements that contain bodies (CREATE, ALTER) or privilege statements
	// require atomic treatment.
	stmtType, err := ua.GetStatementType(sql)
	if err != nil {
		return false
	}

	switch stmtType {
	case UDSFStatementTypeCreateFunction,
		UDSFStatementTypeAlterFunction,
		UDSFStatementTypeDropFunction,
		UDSFStatementTypeGrantUsage,
		UDSFStatementTypeGrantExecute,
		UDSFStatementTypeRevokeUsage,
		UDSFStatementTypeRevokeExecute:
		return true
	default:
		return false
	}
}
