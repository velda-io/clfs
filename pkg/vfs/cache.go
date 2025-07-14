package vfs

import (
	"errors"
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

	// Calculate the end offset of the new data
	newStart := offset
	newEnd := offset + int64(len(data))

	// If no intervals exist, simply add the new one
	if len(c.intervals) == 0 {
		c.intervals = append(c.intervals, Interval{
			Offset: offset,
			Data:   append([]byte{}, data...),
		})
		return
	}

	// Find all intervals that overlap or connect with the new data
	overlapIndices := []int{}
	var minStart int64 = newStart
	var maxEnd int64 = newEnd

	for i, interval := range c.intervals {
		intervalStart := interval.Offset
		intervalEnd := interval.Offset + int64(len(interval.Data))

		// Check for overlap or connection (adjacent intervals are considered connected)
		if newEnd >= intervalStart && newStart <= intervalEnd {
			overlapIndices = append(overlapIndices, i)

			// Update boundaries of the merged interval
			if intervalStart < minStart {
				minStart = intervalStart
			}
			if intervalEnd > maxEnd {
				maxEnd = intervalEnd
			}
		}
	}

	// If no overlaps, insert the new interval at the correct position
	if len(overlapIndices) == 0 {
		newInterval := Interval{
			Offset: offset,
			Data:   append([]byte{}, data...),
		}

		// Find insertion point to maintain sorted order
		insertPos := 0
		for insertPos < len(c.intervals) && c.intervals[insertPos].Offset < offset {
			insertPos++
		}

		// Insert the new interval
		c.intervals = append(c.intervals[:insertPos], append([]Interval{newInterval}, c.intervals[insertPos:]...)...)
		return
	}

	// Create a new merged interval
	mergedLength := int(maxEnd - minStart)
	mergedData := make([]byte, mergedLength)

	// Fill in data from overlapping intervals
	for _, idx := range overlapIndices {
		interval := c.intervals[idx]
		copyOffset := int(interval.Offset - minStart)
		copy(mergedData[copyOffset:], interval.Data)
	}

	// Add the new data (potentially overwriting some existing data)
	copyOffset := int(offset - minStart)
	copy(mergedData[copyOffset:], data)

	// Create the merged interval
	mergedInterval := Interval{
		Offset: minStart,
		Data:   mergedData,
	}

	// Remove all overlapped intervals (in reverse order to maintain indices)
	sort.Slice(overlapIndices, func(i, j int) bool {
		return overlapIndices[i] > overlapIndices[j]
	})

	for _, idx := range overlapIndices {
		c.intervals = append(c.intervals[:idx], c.intervals[idx+1:]...)
	}

	// Insert the merged interval at the correct position
	insertPos := 0
	for insertPos < len(c.intervals) && c.intervals[insertPos].Offset < minStart {
		insertPos++
	}

	if insertPos >= len(c.intervals) {
		c.intervals = append(c.intervals, mergedInterval)
	} else {
		c.intervals = append(c.intervals[:insertPos], append([]Interval{mergedInterval}, c.intervals[insertPos:]...)...)
	}
}

// Read retrieves data from the cache starting at the specified offset
// with the specified length
func (c *Cache) Read(offset int64, length int) ([]byte, error) {
	if length <= 0 {
		return nil, errors.New("invalid read length")
	}

	result := make([]byte, length)
	resultFilled := make([]bool, length)

	// Find all intervals that overlap with the requested range
	readEnd := offset + int64(length)

	for _, interval := range c.intervals {
		intervalStart := interval.Offset
		intervalEnd := intervalStart + int64(len(interval.Data))

		// Skip if no overlap
		if readEnd <= intervalStart || offset >= intervalEnd {
			continue
		}

		// Calculate overlap region
		overlapStart := max(offset, intervalStart)
		overlapEnd := min(readEnd, intervalEnd)

		// Copy data from interval to result
		resultStartIdx := int(overlapStart - offset)
		sourceStartIdx := int(overlapStart - intervalStart)
		copyLength := int(overlapEnd - overlapStart)

		copy(result[resultStartIdx:resultStartIdx+copyLength],
			interval.Data[sourceStartIdx:sourceStartIdx+copyLength])

		// Mark the filled portions
		for i := 0; i < copyLength; i++ {
			resultFilled[resultStartIdx+i] = true
		}
	}

	// Check if all bytes were filled
	for _, filled := range resultFilled {
		if !filled {
			return nil, errors.New("cache miss")
		}
	}

	return result, nil
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
