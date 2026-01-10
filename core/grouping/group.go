package grouping

import (
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
	GroupKey    uint32
	Indices     []uint32
	ParentGroup *Group
	Block       *Block
	ChildBlock  *Block
	// the total number of grouped rows is len(indices)
	// number of child groups is len(childBlock.groups)
}

func (g *Group) Length() int {
	return len(g.Indices)
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
	// here sum the heights of child groups
	if g.ChildBlock == nil {
		return 2
	}
	height := 0
	for _, childGroup := range g.ChildBlock.Groups {
		height += childGroup.AsciiHeight()
	}
	return height
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
