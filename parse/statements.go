package parse

import (
	"strings"
	"unicode"
)

// Copyright (c) 2020-2026 Open Text.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// SplitStatements breaks a SQL string into individual statements separated by semicolons
// that are not contained within literals, comments, or procedural blocks.
func SplitStatements(query string) []string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil
	}

	var statements []string
	var current strings.Builder
	// Track our current lexical state so we can ignore semicolons that live
	// inside literals or comments.
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false
	var dollarTag string
	var nesting StatementNesting
	statementHasContent := false
	var currentToken strings.Builder

	markNonWhitespace := func(b byte) {
		if !unicode.IsSpace(rune(b)) {
			statementHasContent = true
		}
	}
	flushToken := func(nextTokenStart int) {
		if currentToken.Len() == 0 {
			return
		}

		token := strings.ToUpper(currentToken.String())
		nesting.Consume(token, query, nextTokenStart)
		currentToken.Reset()
	}
	flush := func() {
		statement := strings.TrimSpace(current.String())
		current.Reset()
		currentToken.Reset()
		nesting = StatementNesting{}
		if statement != "" && statementHasContent {
			statements = append(statements, statement)
		}
		statementHasContent = false
	}

	i := 0
	for i < len(query) {
		ch := query[i]

		if inLineComment {
			// Swallow comment text but keep the newline terminator so tokens remain separated.
			if ch == '\n' || ch == '\r' {
				current.WriteByte(ch)
				inLineComment = false
			}
			i++
			continue
		}

		if inBlockComment {
			// Traditional /* ... */ comments block statement splitting
			// until the closing marker is found.
			current.WriteByte(ch)
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				current.WriteByte('/')
				i += 2
				inBlockComment = false
				continue
			}
			i++
			continue
		}

		if inSingleQuote {
			// Stay inside the literal, handling doubled single quotes.
			current.WriteByte(ch)
			markNonWhitespace(ch)
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					current.WriteByte('\'')
					markNonWhitespace('\'')
					i += 2
					continue
				}
				inSingleQuote = false
			}
			i++
			continue
		}

		if inDoubleQuote {
			// Identifiers can be quoted with double quotes; treat them like
			// strings for splitter purposes.
			current.WriteByte(ch)
			markNonWhitespace(ch)
			if ch == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					current.WriteByte('"')
					markNonWhitespace('"')
					i += 2
					continue
				}
				inDoubleQuote = false
			}
			i++
			continue
		}

		if dollarTag != "" {
			statementHasContent = true
			// Inside a dollar-quoted literal; exit only when the exact tag is
			// observed again.
			if i+len(dollarTag) <= len(query) && query[i:i+len(dollarTag)] == dollarTag {
				current.WriteString(dollarTag)
				markNonWhitespace(dollarTag[0])
				i += len(dollarTag)
				dollarTag = ""
				continue
			}
			current.WriteByte(ch)
			i++
			continue
		}

		if isStatementTokenChar(ch) {
			current.WriteByte(ch)
			currentToken.WriteByte(ch)
			markNonWhitespace(ch)
			i++
			continue
		}

		flushToken(i)

		if ch == '\'' {
			inSingleQuote = true
			current.WriteByte(ch)
			markNonWhitespace(ch)
			i++
			continue
		}

		if ch == '"' {
			inDoubleQuote = true
			current.WriteByte(ch)
			markNonWhitespace(ch)
			i++
			continue
		}

		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			i += 2
			inLineComment = true
			continue
		}

		if ch == '/' && i+1 < len(query) {
			next := query[i+1]
			if next == '*' {
				current.WriteByte('/')
				current.WriteByte('*')
				i += 2
				inBlockComment = true
				continue
			}
		}

		if ch == '$' {
			if tag, length, ok := readDollarTag(query, i); ok {
				dollarTag = tag
				current.WriteString(tag)
				markNonWhitespace(tag[0])
				i += length
				continue
			}
		}

		if ch == ';' {
			if nesting.Depth() == 0 {
				flush()
			} else {
				current.WriteByte(ch)
				markNonWhitespace(ch)
				nesting.EndStatement()
			}
			i++
			continue
		}

		current.WriteByte(ch)
		markNonWhitespace(ch)
		i++
	}

	flush()
	return statements
}

// StatementNesting tracks procedural blocks while splitting SQL. An IF that
// is not the first token of a statement and is followed by [NOT] EXISTS is a
// DDL existence modifier (for example DROP TABLE IF EXISTS or ADD COLUMN IF
// NOT EXISTS), not the start of a procedural IF block. Its zero value is ready
// for use.
type StatementNesting struct {
	depth            int
	prevToken        string
	statementStarted bool
}

// Consume processes an uppercase token and its position in the SQL text.
func (n *StatementNesting) Consume(token, query string, nextTokenStart int) {
	midStatement := n.statementStarted
	n.statementStarted = true

	switch token {
	case "BEGIN":
		n.depth++
	case "CASE", "LOOP", "WHILE":
		if n.prevToken != "END" {
			n.depth++
		}
	case "IF":
		ddlModifier := midStatement && isExistenceModifier(query, nextTokenStart)
		if n.prevToken != "END" && !ddlModifier {
			n.depth++
		}
	case "END":
		if n.depth > 0 {
			n.depth--
		}
	}

	n.prevToken = token
	if token == "BEGIN" || token == "THEN" || token == "ELSE" || token == "LOOP" {
		n.EndStatement()
	}
}

// EndStatement starts a new statement within the current procedural block.
func (n *StatementNesting) EndStatement() {
	n.statementStarted = false
}

// Depth returns the number of open procedural blocks.
func (n *StatementNesting) Depth() int {
	return n.depth
}

func isExistenceModifier(query string, start int) bool {
	token, next := nextModifierToken(query, start)
	if strings.EqualFold(token, "NOT") {
		token, _ = nextModifierToken(query, next)
	}
	return strings.EqualFold(token, "EXISTS")
}

func nextModifierToken(query string, start int) (string, int) {
	i := start
	for i < len(query) {
		switch {
		case unicode.IsSpace(rune(query[i])):
			i++
		case i+1 < len(query) && query[i:i+2] == "--":
			i += 2
			for i < len(query) && query[i] != '\n' && query[i] != '\r' {
				i++
			}
		case i+1 < len(query) && query[i:i+2] == "/*":
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return "", len(query)
			}
			i += end + 4
		default:
			start = i
			for i < len(query) && isStatementTokenChar(query[i]) {
				i++
			}
			return query[start:i], i
		}
	}
	return "", i
}

func isStatementTokenChar(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') || ch == '_'
}

func readDollarTag(query string, start int) (string, int, bool) {
	if query[start] != '$' {
		return "", 0, false
	}

	end := start + 1
	for end < len(query) {
		r := rune(query[end])
		if query[end] == '$' {
			return query[start : end+1], end + 1 - start, true
		}
		if !isDollarTagRune(r) {
			break
		}
		end++
	}

	return "", 0, false
}

func isDollarTagRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
