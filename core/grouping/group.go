package grouping

import (
	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
)

// define a list of function, one function per group
// to make things easy, default to the list of values
// pass the list of functions to the column, which then returns the list of indices per group
//   this allows the different implementations to optimize the grouping logic depending their implementation
// must also think about how to group on joined columns: grouping itself is the same, this will require a reverse mapping from joined column to base column

// A grouped column consists of a list of blocks, each block consists of a list of groups
// The next column in the hierarchy has one block per group in the parent column
// Each group contains a list of indices of rows that belong to that group
// The list of indices should only be represented once.

// Terminology:
// * the list of columns that are part of the grouping hierarchy are called grouped columns
// * the remaining columns are called aggregated columns or leaf columns
// * the grouping of the last grouped column is shared by all aggregated columns
// The difference between the last grouped column and the aggregated columns is that the all rows of each group's share the same value.

type Group struct {
	GroupKey uint32
	// Indices is the group's full membership list.
	//
	// Deprecated: full membership is O(rows) state and does not scale; the
	// grouping build keeps it only transiently and releases it before
	// returning, so it is nil on groups produced by TableView grouping. Use
	// Length(), First and the column's IGroupOps operations instead. The
	// field remains so existing constructors keep compiling; accessors fall
	// back to it when it is set.
	Indices     []uint32
	ParentGroup *Group
	Block       *Block
	ChildBlock  *Block
	// Count is the number of rows in this group (at its own level).
	Count uint32
	// First is the group's representative row: the first selected row that
	// belongs to it. Group value rendering and value sorting read this row.
	First uint32
	// Aggregates stores computed aggregates for each leaf column.
	// Keys are column names, values are aggregate states.
	Aggregates map[string]aggregates.AggregateState
	// IsComplete indicates whether this group has been fully processed.
	// When false, the group was truncated due to display row limits,
	IsComplete bool
}

func (g *Group) GetValue() string {
	idx := g.First
	if len(g.Indices) > 0 {
		idx = g.Indices[0]
	}
	valueStr, _ := g.Block.GroupedColumn.DataColumn.GetString(idx)
	return valueStr
}

func (g *Group) Length() int {
	if g.Indices != nil {
		return len(g.Indices)
	}
	return int(g.Count)
}

func (g *Group) Height() int {
	// here sum the heights of child groups
	if g.ChildBlock == nil {
		return 1
	}
	height := 0
	for _, childGroup := range g.ChildBlock.Groups {
		height += childGroup.Height()
	}
	return height
}

func (g *Group) AsciiHeight() int {
	if g.ChildBlock == nil {
		return 2
	}
	height := 0
	for _, childGroup := range g.ChildBlock.Groups {
		height += childGroup.AsciiHeight()
	}
	return height
}

// NumSubgroups returns the number of direct child groups
// Returns 0 if this is a leaf group (no children)
func (g *Group) NumSubgroups() int {
	if g.ChildBlock == nil {
		return 0
	}
	return len(g.ChildBlock.Groups)
}

type Block struct {
	Groups        []*Group
	ParentGroup   *Group
	GroupedColumn *GroupedColumn
}

type GroupedColumn struct {
	DataColumn columns.IDataColumn
	ColumnView *columns.ColumnView
	Level      int
	Blocks     []*Block
	Tag        string
}

func (gc *GroupedColumn) GetGroupCount() int {
	count := 0
	for _, b := range gc.Blocks {
		count += len(b.Groups)
	}
	return count
}
