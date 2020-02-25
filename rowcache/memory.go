package rowcache

import (
	"github.com/vertica/vertica-sql-go/msgs"
)

// MemoryCache is a simple in memory row store
type MemoryCache struct {
	resultData []*msgs.BEDataRowMsg
	readIdx    int
}

// NewMemoryCache initializes the memory store with a given size, but it can continue
// to grow
func NewMemoryCache(size int) *MemoryCache {
	return &MemoryCache{
		resultData: make([]*msgs.BEDataRowMsg, 0, size),
	}
}

// AddRow adds a row to the store
func (m *MemoryCache) AddRow(msg *msgs.BEDataRowMsg) {
	m.resultData = append(m.resultData, msg)
}

// Finalize signals the end of new rows, a noop for the memory cache
func (m *MemoryCache) Finalize() error {
	return nil
}

// GetRow pulls a row from the cache, returning nil if none remain
func (m *MemoryCache) GetRow() *msgs.BEDataRowMsg {
	if m.readIdx >= len(m.resultData) {
		return nil
	}
	result := m.resultData[m.readIdx]
	m.readIdx++
	return result
}

// Peek returns the next row without changing the state
func (m *MemoryCache) Peek() *msgs.BEDataRowMsg {
	if len(m.resultData) == 0 {
		return nil
	}
	return m.resultData[0]
}

// Close provides an opportunity to free resources, a noop for the memory cache
func (m *MemoryCache) Close() error {
	return nil
}
