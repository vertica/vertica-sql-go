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
