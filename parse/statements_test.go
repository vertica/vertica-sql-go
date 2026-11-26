package parse

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "single statement",
			query:    "SELECT 1",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "multiple statements",
			query:    "SELECT 1; SELECT 2; SELECT 3;",
			expected: []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		},
		{
			name:     "multiple schemas with if not exists",
			query:    "CREATE SCHEMA IF NOT EXISTS banking;CREATE SCHEMA IF NOT EXISTS domain;",
			expected: []string{"CREATE SCHEMA IF NOT EXISTS banking", "CREATE SCHEMA IF NOT EXISTS domain"},
		},
		{
			name:     "single schema without trailing semicolon",
			query:    "CREATE SCHEMA IF NOT EXISTS banking",
			expected: []string{"CREATE SCHEMA IF NOT EXISTS banking"},
		},
		{
			name:     "schema and table with if not exists",
			query:    "create schema /* comment */ if not exists banking;\nCREATE TABLE IF NOT EXISTS banking.branches (id INT);",
			expected: []string{"create schema /* comment */ if not exists banking", "CREATE TABLE IF NOT EXISTS banking.branches (id INT)"},
		},
		{
			name:     "quoted names and comments",
			query:    "CREATE SCHEMA -- comment;\nIF NOT EXISTS \"bank;ing\"; SELECT 'IF NOT EXISTS;';",
			expected: []string{"CREATE SCHEMA \nIF NOT EXISTS \"bank;ing\"", "SELECT 'IF NOT EXISTS;'"},
		},
		{
			name:     "drop schema and table if exists",
			query:    "DROP TABLE IF EXISTS banking.branches; DROP SCHEMA IF EXISTS banking;",
			expected: []string{"DROP TABLE IF EXISTS banking.branches", "DROP SCHEMA IF EXISTS banking"},
		},
		{
			name:     "function if exists followed by schema",
			query:    "DROP FUNCTION IF EXISTS foo(); CREATE SCHEMA IF NOT EXISTS banking;",
			expected: []string{"DROP FUNCTION IF EXISTS foo()", "CREATE SCHEMA IF NOT EXISTS banking"},
		},
		{
			name:     "sequence modifiers",
			query:    "CREATE SEQUENCE IF NOT EXISTS s; DROP SEQUENCE IF EXISTS s; SELECT 1;",
			expected: []string{"CREATE SEQUENCE IF NOT EXISTS s", "DROP SEQUENCE IF EXISTS s", "SELECT 1"},
		},
		{
			name:     "projection and procedure modifiers",
			query:    "DROP PROJECTION IF EXISTS p; DROP PROCEDURE IF EXISTS proc(INT); SELECT 1;",
			expected: []string{"DROP PROJECTION IF EXISTS p", "DROP PROCEDURE IF EXISTS proc(INT)", "SELECT 1"},
		},
		{
			name:     "alter table column modifiers",
			query:    "ALTER TABLE t ADD COLUMN IF NOT EXISTS c INT; ALTER TABLE t DROP COLUMN IF EXISTS c; SELECT 1;",
			expected: []string{"ALTER TABLE t ADD COLUMN IF NOT EXISTS c INT", "ALTER TABLE t DROP COLUMN IF EXISTS c", "SELECT 1"},
		},
		{
			name:     "other ddl with comments and quoted name",
			query:    "drop user /* comment */ if /* comment */ exists \"user;name\"; SELECT 1;",
			expected: []string{"drop user /* comment */ if /* comment */ exists \"user;name\"", "SELECT 1"},
		},
		{
			name:     "comments inside if not exists",
			query:    "CREATE SEQUENCE IF -- comment\nNOT /* comment */ EXISTS s; SELECT 1;",
			expected: []string{"CREATE SEQUENCE IF \nNOT /* comment */ EXISTS s", "SELECT 1"},
		},
		{
			name:     "ddl and procedural if in a block",
			query:    "BEGIN DROP SEQUENCE IF EXISTS s; IF NOT EXISTS (SELECT 1) THEN SELECT 2; END IF; SELECT 3; END; SELECT 4;",
			expected: []string{"BEGIN DROP SEQUENCE IF EXISTS s; IF NOT EXISTS (SELECT 1) THEN SELECT 2; END IF; SELECT 3; END", "SELECT 4"},
		},
		{
			name:     "explain and profile prefixes",
			query:    "EXPLAIN DROP TABLE IF EXISTS t; PROFILE CREATE TABLE IF NOT EXISTS t (a INT); SELECT 1;",
			expected: []string{"EXPLAIN DROP TABLE IF EXISTS t", "PROFILE CREATE TABLE IF NOT EXISTS t (a INT)", "SELECT 1"},
		},
		{
			name:     "if not exists after case expression in same statement",
			query:    "ALTER TABLE t ADD COLUMN c INT DEFAULT CASE WHEN 1 = 1 THEN 1 END, ADD COLUMN IF NOT EXISTS d INT; SELECT 1;",
			expected: []string{"ALTER TABLE t ADD COLUMN c INT DEFAULT CASE WHEN 1 = 1 THEN 1 END, ADD COLUMN IF NOT EXISTS d INT", "SELECT 1"},
		},
		{
			name:     "procedural if exists after case branch and else",
			query:    "CREATE FUNCTION f(x INT) RETURN INT AS BEGIN CASE x WHEN 1 THEN IF EXISTS (SELECT 1) THEN RETURN 1; END IF; ELSE IF NOT EXISTS (SELECT 1) THEN RETURN 2; END IF; END CASE; RETURN 0; END; SELECT 1;",
			expected: []string{"CREATE FUNCTION f(x INT) RETURN INT AS BEGIN CASE x WHEN 1 THEN IF EXISTS (SELECT 1) THEN RETURN 1; END IF; ELSE IF NOT EXISTS (SELECT 1) THEN RETURN 2; END IF; END CASE; RETURN 0; END", "SELECT 1"},
		},
		{
			name:     "procedural if exists as first statement in loop",
			query:    "BEGIN LOOP IF NOT EXISTS (SELECT 1) THEN EXIT; END IF; END LOOP; END; SELECT 2;",
			expected: []string{"BEGIN LOOP IF NOT EXISTS (SELECT 1) THEN EXIT; END IF; END LOOP; END", "SELECT 2"},
		},
		{
			name:     "ddl inside procedural if and else",
			query:    "BEGIN IF EXISTS (SELECT 1) THEN DROP SEQUENCE IF EXISTS s; ELSE CREATE SEQUENCE IF NOT EXISTS s; END IF; SELECT 2; END; SELECT 3;",
			expected: []string{"BEGIN IF EXISTS (SELECT 1) THEN DROP SEQUENCE IF EXISTS s; ELSE CREATE SEQUENCE IF NOT EXISTS s; END IF; SELECT 2; END", "SELECT 3"},
		},
		{
			name:     "if not condition in ddl context is not an existence modifier",
			query:    "CREATE FUNCTION f() RETURN INT AS IF NOT (1 = 0) THEN RETURN 1; END IF; SELECT 2;",
			expected: []string{"CREATE FUNCTION f() RETURN INT AS IF NOT (1 = 0) THEN RETURN 1; END IF", "SELECT 2"},
		},
		{
			name:     "schema followed by function with nested if",
			query:    "CREATE SCHEMA IF NOT EXISTS banking; CREATE FUNCTION f() RETURN INT AS BEGIN IF NOT (1 = 0) THEN RETURN 1; END IF; RETURN 0; END; SELECT 1;",
			expected: []string{"CREATE SCHEMA IF NOT EXISTS banking", "CREATE FUNCTION f() RETURN INT AS BEGIN IF NOT (1 = 0) THEN RETURN 1; END IF; RETURN 0; END", "SELECT 1"},
		},
		{
			name:     "procedural if not exists remains nested",
			query:    "BEGIN IF NOT EXISTS (SELECT 1) THEN SELECT 2; END IF; SELECT 3; END; SELECT 4;",
			expected: []string{"BEGIN IF NOT EXISTS (SELECT 1) THEN SELECT 2; END IF; SELECT 3; END", "SELECT 4"},
		},
		{
			name:     "ignore quoted semicolons",
			query:    "SELECT ';' AS txt; SELECT 'still;literal';",
			expected: []string{"SELECT ';' AS txt", "SELECT 'still;literal'"},
		},
		{
			name: "ignore comments",
			query: `-- leading comment;
SELECT 1; /* block;comment */ SELECT 2;`,
			expected: []string{"SELECT 1", "/* block;comment */ SELECT 2"},
		},
		{
			name:     "dollar quoted",
			query:    "SELECT $$value;inside$$; SELECT $tag$semi;colon$tag$;",
			expected: []string{"SELECT $$value;inside$$", "SELECT $tag$semi;colon$tag$"},
		},
		{
			name:     "mixed whitespace",
			query:    "  SELECT 1;\n\n ; SELECT 2  ;",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "line comment only",
			query:    "-- just a comment",
			expected: nil,
		},
		{
			name:     "double slash is not line comment",
			query:    "SELECT 1; // comment about next\nSELECT 2;",
			expected: []string{"SELECT 1", "// comment about next\nSELECT 2"},
		},
		{
			name: "create function with nested control flow followed by select",
			query: `CREATE FUNCTION test_multistmt_udsf_batch(x INT)
	RETURN INT AS
	BEGIN
		/* This semicolon; must stay inside the function body. */
		RETURN CASE
			WHEN x >= 0 THEN
				CASE
					WHEN x >= 10 THEN x + 1
					ELSE x + 2
				END
			ELSE
				CASE
					WHEN x <= -10 THEN x - 1
					ELSE x - 2
				END
		END;
	END;
	SELECT CURRENT_USER();`,
			expected: []string{
				`CREATE FUNCTION test_multistmt_udsf_batch(x INT)
	RETURN INT AS
	BEGIN
		/* This semicolon; must stay inside the function body. */
		RETURN CASE
			WHEN x >= 0 THEN
				CASE
					WHEN x >= 10 THEN x + 1
					ELSE x + 2
				END
			ELSE
				CASE
					WHEN x <= -10 THEN x - 1
					ELSE x - 2
				END
		END;
	END`,
				`SELECT CURRENT_USER()`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitStatements(tc.query)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}
