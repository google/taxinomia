# Adding a New Column Type

This checklist covers all the places that need to be updated when adding a new column type to Taxinomia.

## Core Column Implementation

- [ ] **Create column file** `core/columns/<type>_column.go`
  - [ ] Column struct with `columnDef`, `data`, `isKey`, `valueIndex` fields
  - [ ] `NewXxxColumn(columnDef *ColumnDef)` constructor
  - [ ] `ColumnDef()` method
  - [ ] `Length()` method
  - [ ] `GetString(i uint32)` method - format value for display
  - [ ] `GetValue(i uint32)` method - return typed value
  - [ ] `GetIndex(v T)` method - reverse lookup for key columns
  - [ ] `Append(value T)` method
  - [ ] `IsKey()` method
  - [ ] `FinalizeColumn()` method - detect uniqueness, build index
  - [ ] `GroupIndices(indices []uint32, columnView *ColumnView)` method
  - [ ] `Filter(predicate func(T) bool)` method
  - [ ] `CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner)` method

- [ ] **Add joined column** in `core/columns/joined_columns.go`
  - [ ] `JoinedXxxColumn` struct
  - [ ] `NewJoinedXxxColumn()` constructor
  - [ ] Implement `IDataColumn` interface methods

- [ ] **Add comparison support** in `core/columns/compare.go`
  - [ ] Add `case *XxxColumn:` in `CompareAtIndex()`
  - [ ] Add helper comparison function if needed (e.g., `compareXxx()`)

## CSV Import

- [ ] **Update proto enum** in `core/csvimport/table_source.proto`
  - [ ] Add `COLUMN_TYPE_XXX = N;` to `ColumnType` enum

- [ ] **Regenerate proto**
  ```bash
  protoc --go_out=. --go_opt=paths=source_relative core/csvimport/table_source.proto
  ```

- [ ] **Update CSV column type** in `core/csvimport/csv_import.go`
  - [ ] Add `CsvColumnTypeXxx` constant
  - [ ] Add `xxxCols` map in `ImportFromReader()`
  - [ ] Add column creation case in the column type switch
  - [ ] Add value parsing case in the population loop
  - [ ] Add finalization loop for the new column type
  - [ ] Add case in `detectColumnTypes()` for explicit type

- [ ] **Update textproto mapping** in `core/csvimport/textproto.go`
  - [ ] Add `case ColumnType_COLUMN_TYPE_XXX:` in `protoTypeToCsvColumnType()`

## Table View & Display

- [ ] **Update type name display** in `core/tables/table_view.go`
  - [ ] Add `case *columns.XxxColumn:` in `GetColumnTypeName()`

- [ ] **Update system tables** in `core/models/system_tables.go`
  - [ ] Add `case *columns.XxxColumn:` in `getColumnType()`

## Aggregation (if numeric type)

- [ ] **Check aggregate handling** in `core/tables/table_view.go`
  - [ ] Verify `addNumericValue()` handles the type (via interface or explicit case)
  - [ ] Add specific case if interface matching doesn't work

## Joins (if joinable type)

- [ ] **Update joiner creation** in `core/models/datamodel.go`
  - [ ] Add case in `createJoiner()` if the type can be used for entity joins

## Expression Evaluation (if special handling needed)

- [ ] **Check expression evaluation** in `core/server/server.go`
  - [ ] Add case if the type needs special expression handling (like datetime/duration)

## Benchmarks

- [ ] **Add benchmarks** in `core/columns/column_bench_test.go`
  - [ ] `BenchmarkXxxColumn_Append_1M`
  - [ ] `BenchmarkXxxColumn_FinalizeColumn_1M`
  - [ ] `BenchmarkXxxColumn_GetString_1M`
  - [ ] `BenchmarkXxxColumn_GroupIndices_1M_100Groups`
  - [ ] Add to cross-column comparison benchmark

## Tests

- [ ] **Add unit tests** in `core/columns/<type>_column_test.go`
  - [ ] Basic operations test
  - [ ] Edge cases (empty, single value, duplicates)
  - [ ] Special value handling (if applicable)
  - [ ] GroupIndices test
  - [ ] IsKey/FinalizeColumn test

## Documentation

- [ ] **Update column types doc** in `docs/column_types.md`
  - [ ] Add to overview table
  - [ ] Add detailed section with value range, features, display format
  - [ ] Update proto configuration example
  - [ ] Update computed columns table if applicable

## Demo Data (optional)

- [ ] **Add demo data** using the new column type
  - [ ] Update `demo/data/sources.textproto` if using CSV
  - [ ] Or add programmatic demo in `demo/demo_data.go`

## Verification

- [ ] Run `go build ./...` - no compile errors
- [ ] Run `go test ./...` - all tests pass
- [ ] Verify column type displays correctly in UI
- [ ] Verify sorting works correctly
- [ ] Verify grouping works correctly
- [ ] Verify aggregation works (if numeric)

## Quick Reference: Files to Touch

| File | What to add |
|------|-------------|
| `core/columns/<type>_column.go` | New file - column implementation |
| `core/columns/joined_columns.go` | JoinedXxxColumn type |
| `core/columns/compare.go` | CompareAtIndex case |
| `core/csvimport/table_source.proto` | COLUMN_TYPE_XXX enum value |
| `core/csvimport/table_source.pb.go` | Regenerate from proto |
| `core/csvimport/csv_import.go` | CsvColumnTypeXxx, import handling |
| `core/csvimport/textproto.go` | Proto to CSV type mapping |
| `core/tables/table_view.go` | GetColumnTypeName case |
| `core/models/system_tables.go` | getColumnType case |
| `core/columns/column_bench_test.go` | Benchmarks |
| `docs/column_types.md` | Documentation |
