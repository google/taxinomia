# Interpunctus

Interpunctus is a lightweight, in-memory columnar data analysis tool with hierarchical grouping and aggregation capabilities.

## Features

- **Columnar Storage**: Memory-efficient storage using generic uint8/16/32 types
- **Hierarchical Grouping**: Drill-down analysis with multiple grouping levels
- **Filtering**: Custom DSL with exact match (`"value"`), contains (`'value'`), and boolean operators (`!`, `&`, `|`)
- **Aggregation**: Sum and count aggregations across grouped data
- **Interactive Web UI**: Real-time exploration via URL parameters

## Getting Started

### Download GitHub Issues Data

To analyze VS Code GitHub issues:

```bash
gh issue list --repo microsoft/vscode --limit 1000 --state all --json number,title,labels,state,createdAt,closedAt,comments,author,assignees,body > vscode_issues.json
```

### Run the Server

```bash
go run .
```

Server will start on http://127.0.0.1:8090

### Example Queries

**Hierarchical grouping by year, author type, and assignment status:**
```
http://127.0.0.1:8090/grouped%20?columns=label_count+,year+,has_assignee+,author_type+,state+,label+&groupby=label_count,year,has_assignee&sortby=&filterby=
```

## Available Columns

From GitHub issues data:

- `year` - Issue creation year
- `month` - Issue creation month
- `state` - OPEN/CLOSED
- `author` - Issue author username
- `author_type` - top-10 contributors vs community
- `label` - Primary label
- `label_count` - Number of labels (0, 1, 2, 3+)
- `has_assignee` - assigned/unassigned
- `comments` - Number of comments (aggregatable)

## URL Parameters

- `columns` - Column order with sort direction (`+` asc, `-` desc)
- `groupby` - Comma-separated list of columns to group by
- `sortby` - Custom sort specifications
- `filterby` - Filter expressions

## Architecture

- **Table**: Columnar storage with string-to-key compression
- **ColumnView**: View-specific metadata (grouping, filtering, sorting)
- **Group**: Hierarchical structure for drill-down analysis
- **Renderer**: HTML table generation with interactive controls

## How Filtering Works

Filtering uses a custom DSL supporting:

### Syntax
- `"exact"` - Exact match (e.g., `"bug"` matches only "bug")
- `'contains'` - Substring match (e.g., `'feat'` matches "feature-request")
- `!` - NOT operator (e.g., `!"bug"` excludes "bug")
- `&` - AND operator (e.g., `'a'&'b'` matches values containing both "a" and "b")
- `|` - OR operator (e.g., `"bug"|"feature"` matches either)

### Operator Precedence
From highest to lowest: `!` (NOT) → `&` (AND) → `|` (OR)

### Examples
```
'open'&!'assigned'           # Contains "open" but not "assigned"
"bug"|"feature-request"      # Exactly "bug" OR "feature-request"
'2024'&'critical'            # Contains both "2024" and "critical"
```

### Implementation
- **Match()** function in `filtering.go` processes filter strings
- Splits by `|` for OR conditions
- Splits each OR term by `&` for AND conditions
- Applies NOT prefix `!` to invert matches
- Returns boolean result for each value

## How Grouping Works

Grouping creates hierarchical data structures for drill-down analysis.

### Custom Group Definitions

Instead of grouping by all distinct values, you can define custom groups using the filter syntax with `||` as a separator:

**URL Format:**
```
groupon=column:filter1||filter2||filter3
```

**Examples:**
```
groupon=year:"2023"|"2024"||"2025"
# Creates 2 groups:
#   - Group 1: years matching "2023" OR "2024"
#   - Group 2: years matching "2025"

groupon=label:'bug'||'feature'||'docs'
# Creates 3 groups:
#   - Group 1: labels containing "bug"
#   - Group 2: labels containing "feature"
#   - Group 3: labels containing "docs"

groupon=author_type:"top-10"||!"top-10"
# Creates 2 groups:
#   - Group 1: exactly "top-10"
#   - Group 2: NOT "top-10" (i.e., "community")
```

Each filter can use the full filter DSL (`"exact"`, `'contains'`, `!`, `&`, `|`), and values not matching any filter are assigned to group 0 (hidden).

### Data Flow

1. **Table.Apply()** - Entry point that orchestrates the grouping process
   - Creates ColumnViews for all columns in the view
   - Populates `keyToGroupKey` mappings (which values belong to which groups)
   - Calls Group() or Sort() based on view configuration

2. **ColumnView Setup** - Maps values to groups
   - `keyToGroupKey`: Maps each data value key → group key
   - `groupKeyToKey`: Reverse mapping (group key → representative value key)
   - `groupKeyToFilter`: Maps group key → filter/label string
   - `groupKeyToOrder`: Maps group key → sort order position

3. **MakeGroup()** - Initializes the grouping
   - Collects all row indices that pass the mask (filter)
   - Calls recursive `group()` function with the first column

4. **group()** - Recursive grouping function
   - **Base case** (last column): Creates leaf groups
     - `indices` map: `groupKey → [row indices]`
     - Calculates sums for aggregatable columns
     - Returns Group with populated indices
   - **Recursive case**: Creates intermediate groups
     - Groups current column's indices by value
     - Recursively groups remaining columns
     - Aggregates counts and sums from child groups

5. **Group Structure**
   ```go
   type Group struct {
       columnDef  *ColumnDef
       level      int
       value      uint32           // This group's value
       groups     map[uint32]*Group // Child groups (intermediate levels)
       indices    map[uint32][]uint32 // Row indices (leaf level only)
       counts     []int             // [groups at this level, groups at child levels..., total rows]
       sums       map[string]int    // Aggregated sums per column
   }
   ```

### Example: 3-Level Grouping

**Data**: 1000 GitHub issues
**Grouping**: `year → state → label`

```
Year 2024 (Group)
├── counts: [2, 145, 1000]  // 2 states, 145 labels total, 1000 rows
├── groups:
│   ├── OPEN (Group)
│   │   ├── counts: [73, 500]  // 73 labels, 500 rows
│   │   └── groups:
│   │       ├── bug (Group - leaf)
│   │       │   └── indices: [5, 23, 47, ...]  // Actual row numbers
│   │       └── feature-request (Group - leaf)
│   │           └── indices: [12, 89, ...]
│   └── CLOSED (Group)
│       └── groups: ...
```

### Rendering

1. **CellBuilder.cells()** - Recursive rendering
   - For each group, creates a cell with `rowspan` = number of child rows
   - Leaf groups: Creates one row per distinct value
   - Intermediate groups: Recursively renders children
   - Merges aggregated sums for display

2. **Row Spanning Logic**
   - Grouped column cells span multiple rows (using HTML `rowspan`)
   - Non-grouped columns show individual values or aggregated sums
   - Header shows: `ColumnName (groups/distinct/visible)`

### Performance

- **Memory efficient**: Values stored as uint8/16/32 keys, not strings
- **Zero-allocation sorting**: Pre-computed order maps
- **Bitmap indexing**: Fast filtering on columnar data
- **Lazy evaluation**: Groups only computed when needed
