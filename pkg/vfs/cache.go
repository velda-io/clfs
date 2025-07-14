package vfs

import (
	"sort"
)

// Interval represents a cache interval with data starting at a specific offset
type Interval struct {
	Offset int64
	Data   []byte
}

// Cache stores data as sorted intervals (offset, data)
type Cache struct {
	intervals []Interval
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{
		intervals: []Interval{},
	}
}

// Insert adds or updates data at the specified offset
// It handles merging intervals when they overlap or connect
func (c *Cache) Insert(offset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	if len(c.intervals) == 0 {
		data := append([]byte{}, data...) // Copy data to avoid aliasing
		c.intervals = append(c.intervals, Interval{Offset: offset, Data: data})
		return
	}

	mergeBegin := sort.Search(len(c.intervals), func(i int) bool {
		return c.intervals[i].Offset+int64(len(c.intervals[i].Data)) >= offset
	})
	mergeEnd := sort.Search(len(c.intervals), func(i int) bool {
		return c.intervals[i].Offset > offset+int64(len(data))
	})

	// Replacing [mergeBegin, mergeEnd) with the new interval
	var newData []byte
	newOffset := offset
	if mergeBegin < len(c.intervals) && c.intervals[mergeBegin].Offset < offset {
		newOffset = c.intervals[mergeBegin].Offset
		newData = append(newData, c.intervals[mergeBegin].Data[:offset-c.intervals[mergeBegin].Offset]...)
	}
	newData = append(newData, data...)
	if mergeEnd > 0 && c.intervals[mergeEnd-1].Offset+int64(len(c.intervals[mergeEnd-1].Data)) > offset+int64(len(data)) {
		newData = append(newData, c.intervals[mergeEnd-1].Data[offset+int64(len(data))-c.intervals[mergeEnd-1].Offset:]...)
	}
	if mergeBegin == mergeEnd {
		c.intervals = append(c.intervals, Interval{})
	}
	if mergeEnd != mergeBegin+1 {
		copy(c.intervals[mergeBegin+1:], c.intervals[mergeEnd:])
	}
	c.intervals[mergeBegin] = Interval{
		Offset: newOffset,
		Data:   newData,
	}
	if mergeEnd > mergeBegin {
		c.intervals = c.intervals[:len(c.intervals)-(mergeEnd-mergeBegin-1)]
	}
}

// Read retrieves data from the cache starting at the specified offset
// with the specified length
func (c *Cache) Read(offset int64, length int) []byte {
	if length <= 0 {
		return nil
	}

	ind := sort.Search(len(c.intervals), func(i int) bool {
		return c.intervals[i].Offset+int64(len(c.intervals[i].Data)) > offset
	})
	if ind >= len(c.intervals) {
		return nil
	}
	if c.intervals[ind].Offset > offset {
		return nil
	}

	start := offset - c.intervals[ind].Offset
	hit_size := min(int64(length), int64(c.intervals[ind].Offset+int64(len(c.intervals[ind].Data))-offset))
	return c.intervals[ind].Data[start : start+hit_size]
}

// Clear removes all intervals from the cache
func (c *Cache) Clear() {
	c.intervals = []Interval{}
}

// Size returns the number of intervals in the cache
func (c *Cache) Size() int {
	return len(c.intervals)
}

// Helper functions
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
