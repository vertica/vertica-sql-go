package rowcache

import (
	"testing"

	"github.com/vertica/vertica-sql-go/msgs"
)

func TestFileCache(t *testing.T) {
	t.Run("without enough rows to switch to a file", func(t *testing.T) {
		rowCount := 100
		cache, err := NewFileCache(1000)
		if err != nil {
			t.Fatalf("Unable to create temp file")
		}
		for i := 0; i < rowCount; i++ {
			row := msgs.BEDataRowMsg([]byte("testRow"))
			cache.AddRow(&row)
		}
		cache.Finalize()
		if cache.Peek() == nil {
			t.Error("Expected a row with Peek")
		}
		for i := 0; i < rowCount; i++ {
			row := cache.GetRow()
			if row == nil {
				t.Errorf("Ran out of rows at %d", i)
			}
		}
		cache.Close()
	})
	t.Run("with file writes", func(t *testing.T) {
		rowCount := 10000
		cache, err := NewFileCache(100)
		if err != nil {
			t.Fatalf("Unable to create temp file")
		}
		for i := 0; i < rowCount; i++ {
			row := msgs.BEDataRowMsg([]byte("testRow"))
			cache.AddRow(&row)
		}
		cache.Finalize()
		if cache.Peek() == nil {
			t.Error("Expected a row with Peek")
		}
		for i := 0; i < rowCount; i++ {
			row := cache.GetRow()
			if row == nil {
				t.Errorf("Ran out of rows at %d", i)
			}
		}
		cache.Close()
	})

}
