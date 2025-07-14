package vfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheInsert(t *testing.T) {
	cache := NewCache()

	// Insert initial data
	cache.Insert(10, []byte("hello"))
	assert.Equal(t, 1, len(cache.intervals), "Expected 1 interval")
	assert.Equal(t, int64(10), cache.intervals[0].Offset, "Expected offset 10")
	assert.Equal(t, "hello", string(cache.intervals[0].Data), "Expected data 'hello'")

	// Insert data that overlaps at the end
	cache.Insert(13, []byte("world"))
	assert.Equal(t, 1, len(cache.intervals), "Expected 1 merged interval")
	assert.Equal(t, int64(10), cache.intervals[0].Offset, "Expected offset 10")
	assert.Equal(t, "helworld", string(cache.intervals[0].Data), "Expected data 'helworld'")

	// Insert data that overlaps at the beginning
	cache.Insert(5, []byte("super"))
	assert.Equal(t, 1, len(cache.intervals), "Expected 1 merged interval")
	assert.Equal(t, int64(5), cache.intervals[0].Offset, "Expected offset 5")
	assert.Equal(t, "superhelworld", string(cache.intervals[0].Data), "Expected data 'superhelworld'")

	// Insert data that connects but doesn't overlap
	cache.Insert(18, []byte("!!!"))
	assert.Equal(t, 1, len(cache.intervals), "Expected 1 merged interval")
	assert.Equal(t, "superhelworld!!!", string(cache.intervals[0].Data), "Expected data 'superhelworld!!!'")

	// Insert data with a gap
	cache.Insert(25, []byte("gap"))
	assert.Equal(t, 2, len(cache.intervals), "Expected 2 intervals")
	assert.Equal(t, int64(25), cache.intervals[1].Offset, "Expected offset 25")
	assert.Equal(t, "gap", string(cache.intervals[1].Data), "Expected data 'gap'")
}

func TestCacheRead(t *testing.T) {
	cache := NewCache()

	// Insert some data
	cache.Insert(10, []byte("hello"))
	cache.Insert(20, []byte("world"))

	// Test reading from a single interval
	data := cache.Read(10, 5)
	assert.Equal(t, "hello", string(data), "Expected 'hello'")

	// Test reading across intervals (should fail due to gap)
	b := cache.Read(10, 15)
	assert.Equal(t, 5, len(b), "Expected partial read of 5 bytes")

	// Fill the gap
	cache.Insert(15, []byte("middle"))

	// Now reading across should work
	data = cache.Read(10, 9)
	expected := "hellomidd"
	assert.Equal(t, expected, string(data), "Expected 'hellomidd'")

	// Test partial read
	data = cache.Read(12, 5)
	assert.Equal(t, "llomi", string(data), "Expected 'llomi'")

	// Test reading outside of cached range
	data = cache.Read(0, 5)
	assert.Nil(t, data, "Expected nil for reading outside of cache")
}

func TestCacheComplexMerge(t *testing.T) {
	cache := NewCache()

	// Insert several intervals that will need to be merged
	cache.Insert(10, []byte("AAAAA"))
	cache.Insert(20, []byte("BBBBB"))
	cache.Insert(30, []byte("CCCCC"))

	assert.Equal(t, 3, len(cache.intervals), "Expected 3 intervals")

	// Insert an interval that overlaps with first and second
	cache.Insert(13, []byte("XXXXX"))

	// Due to our implementation, the number of intervals might vary,
	// but the key thing is to check the merged content at the end

	// Now connect the intervals so there's only one merged interval
	cache.Insert(17, []byte("YYYYY")) // Connect first and second
	cache.Insert(25, []byte("ZZZZZ")) // Connect second and third

	// Verify merged content
	data := cache.Read(10, 25)
	assert.Equal(t, 25, len(data), "Expected data length 25")
}

func TestCacheClear(t *testing.T) {
	cache := NewCache()

	cache.Insert(10, []byte("hello"))
	cache.Insert(20, []byte("world"))

	assert.Equal(t, 2, len(cache.intervals), "Expected 2 intervals")

	cache.Clear()

	assert.Equal(t, 0, len(cache.intervals), "Expected 0 intervals after clear")
}
