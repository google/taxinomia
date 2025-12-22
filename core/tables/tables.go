package tables

import (
	"github.com/google/taxinomia/core/columns"
)

type DataTable struct {
	columns map[string]columns.IDataColumn
}

func NewDataTable() *DataTable {
	return &DataTable{
		columns: make(map[string]columns.IDataColumn),
	}
}

func (dt *DataTable) AddColumn(col columns.IDataColumn) {
	// Initialize the column with empty data
	def := col.ColumnDef()
	name := def.Name()
	dt.columns[name] = col
}

func (dt *DataTable) GetColumn(name string) columns.IDataColumn {
	return dt.columns[name]
}
