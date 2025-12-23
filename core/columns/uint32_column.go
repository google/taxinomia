package columns

import "fmt"

// Uint32Column is optimized for uint32 numeric data.
// It stores uint32 values directly without key mapping overhead.
type Uint32Column struct {
	columnDef *ColumnDef
	data      []uint32
}

// NewUint32Column creates a new uint32 column
func NewUint32Column(columnDef *ColumnDef) *Uint32Column {
	return &Uint32Column{
		columnDef: columnDef,
		data:      make([]uint32, 0),
	}
}

func (c *Uint32Column) Append(value uint32) {
	c.data = append(c.data, value)
}

func (c *Uint32Column) Length() int {
	return len(c.data)
}

func (c *Uint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

// GetString returns the string representation of the value at index i
func (c *Uint32Column) GetString(i int) (string, error) {
	if i < 0 || i >= len(c.data) {
		return "", nil
	}
	return fmt.Sprintf("%d", c.data[i]), nil
}

// Filter returns indices where the predicate returns true
func (c *Uint32Column) Filter(predicate func(uint32) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// Contains returns true if the column contains the value
func (c *Uint32Column) Contains(value uint32) bool {
	for _, v := range c.data {
		if v == value {
			return true
		}
	}
	return false
}

// Unique returns all unique values in the column
func (c *Uint32Column) Unique() []uint32 {
	seen := make(map[uint32]bool)
	unique := make([]uint32, 0)

	for _, v := range c.data {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}

	return unique
}

// Stats returns statistics about the column
func (c *Uint32Column) Stats() Uint32ColumnStats {
	uniqueCount := len(c.Unique())

	// For uint32, instead of average length, we calculate average value
	var sum uint64
	for _, v := range c.data {
		sum += uint64(v)
	}
	avg := 0.0
	if len(c.data) > 0 {
		avg = float64(sum) / float64(len(c.data))
	}

	return Uint32ColumnStats{
		Count:       len(c.data),
		UniqueCount: uniqueCount,
		AvgValue:    avg,
	}
}

// Uint32ColumnStats contains statistics about a Uint32Column
type Uint32ColumnStats struct {
	Count       int
	UniqueCount int
	AvgValue    float64
}