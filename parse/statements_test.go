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
