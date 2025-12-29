package parse

import (
	"strings"
	"unicode"
)

// Copyright (c) 2020-2024 Open Text.

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
// that are not contained within literals or comments.
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

	flush := func() {
		statement := strings.TrimSpace(current.String())
		current.Reset()
		if statement != "" {
			statements = append(statements, statement)
		}
	}

	i := 0
	for i < len(query) {
		ch := query[i]

		if inLineComment {
			// Consume everything until the newline terminator.
			current.WriteByte(ch)
			if ch == '\n' || ch == '\r' {
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
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					current.WriteByte('\'')
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
			if ch == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					current.WriteByte('"')
					i += 2
					continue
				}
				inDoubleQuote = false
			}
			i++
			continue
		}

		if dollarTag != "" {
			// Inside a dollar-quoted literal; exit only when the exact tag is
			// observed again.
			if i+len(dollarTag) <= len(query) && query[i:i+len(dollarTag)] == dollarTag {
				current.WriteString(dollarTag)
				i += len(dollarTag)
				dollarTag = ""
				continue
			}
			current.WriteByte(ch)
			i++
			continue
		}

		if ch == '\'' {
			inSingleQuote = true
			current.WriteByte(ch)
			i++
			continue
		}

		if ch == '"' {
			inDoubleQuote = true
			current.WriteByte(ch)
			i++
			continue
		}

		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			current.WriteByte('-')
			current.WriteByte('-')
			i += 2
			inLineComment = true
			continue
		}

		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			current.WriteByte('/')
			current.WriteByte('*')
			i += 2
			inBlockComment = true
			continue
		}

		if ch == '$' {
			if tag, length, ok := readDollarTag(query, i); ok {
				dollarTag = tag
				current.WriteString(tag)
				i += length
				continue
			}
		}

		if ch == ';' {
			flush()
			i++
			continue
		}

		current.WriteByte(ch)
		i++
	}

	flush()
	return statements
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
