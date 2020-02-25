package rowcache

import (
	"testing"

	"github.com/vertica/vertica-sql-go/msgs"
)

func TestMemoryCache(t *testing.T) {
	rowCount := 100
	cache := NewMemoryCache(16)
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
}
