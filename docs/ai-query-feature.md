# Natural Language Query Feature

## Overview

Enable users to query Taxinomia tables using plain English. An AI model interprets the query and proposes or directly displays the appropriate table view with the right columns, filters, groupings, and sorts.

## User Experience

### Search Interface

A prominent search bar on the landing page and table views:

```
┌─────────────────────────────────────────────────────────────┐
│ 🔍 Ask a question about your data...                        │
└─────────────────────────────────────────────────────────────┘
```

### Example Queries

**Infrastructure queries:**
- "Show machines with high GPU count grouped by zone"
- "Which clusters have the most failed tasks?"
- "Find all machines in us-east1 with low free CPU"
- "Compare memory usage across jobs by priority"

**Analytics queries:**
- "Show top 10 transactions by amount"
- "Which users have the most orders?"
- "Group orders by region and show totals"

### Response Flow

```
User types query
       ↓
AI interprets with schema context
       ↓
┌─────────────────────────────────────────────────────────────┐
│ AI Proposal:                                                │
│                                                             │
│ Table: google_machines                                      │
│ Columns: machine, zone, gpu_count, status, cpu_cores        │
│ Filter: gpu_count > 2                                       │
│ Group by: zone                                              │
│ Sort: gpu_count (descending)                                │
│                                                             │
│ "Showing machines with more than 2 GPUs, grouped by zone"   │
│                                                             │
│ [Apply]  [Modify]  [Cancel]                                 │
└─────────────────────────────────────────────────────────────┘
       ↓
User confirms → Navigate to table view
```

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Frontend   │────▶│   Backend    │────▶│   LLM API    │
│  (search UI) │     │ (proxy+ctx)  │     │ (Claude/etc) │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │Schema Context│
                    │ - Tables     │
                    │ - Columns    │
                    │ - Types      │
                    │ - Joins      │
                    │ - Domains    │
                    └──────────────┘
```

### Components

1. **Schema Exporter** - Generates JSON representation of available tables/columns
2. **Backend Proxy** - Adds schema context, calls LLM, validates response
3. **Frontend UI** - Search bar, proposal display, apply/modify actions

## Schema Context Format

The schema context sent to the LLM should be concise but complete:

```json
{
  "tables": [
    {
      "name": "google_machines",
      "description": "Physical servers with CPU, memory, disk, GPU resources",
      "domain": "google",
      "row_count": 12000,
      "columns": [
        {
          "name": "machine",
          "display_name": "Machine",
          "type": "string",
          "is_key": true,
          "entity_type": "google.machine",
          "description": "Unique machine identifier"
        },
        {
          "name": "zone",
          "display_name": "Zone",
          "type": "string",
          "entity_type": "google.zone",
          "description": "Data center zone containing this machine"
        },
        {
          "name": "cpu_cores",
          "display_name": "CPU Cores",
          "type": "uint32",
          "description": "Number of CPU cores"
        },
        {
          "name": "gpu_count",
          "display_name": "GPUs",
          "type": "uint32",
          "description": "Number of GPU accelerators"
        }
      ],
      "joins_to": ["google_zones.zone", "google_racks.rack", "google_clusters.cluster"]
    }
  ],
  "entity_types": [
    {
      "name": "google.zone",
      "description": "Data center zone",
      "tables_with_column": ["google_zones", "google_clusters", "google_machines"]
    }
  ],
  "query_capabilities": {
    "filters": ["equals", "contains", "greater_than", "less_than", "range"],
    "aggregates": ["count", "sum", "avg", "min", "max"],
    "grouping": true,
    "sorting": true,
    "joins": true
  }
}
```

## LLM Prompt Structure

```
You are a query assistant for Taxinomia, a data exploration tool.

Given the user's natural language query, determine:
1. Which table best answers the question
2. Which columns should be visible
3. Any filters to apply
4. Whether to group by any columns
5. How to sort the results
6. Which aggregates to show (if grouped)

Available tables and their schemas:
{schema_json}

User query: "{user_query}"

Respond with JSON:
{
  "table": "table_name",
  "columns": ["col1", "col2", ...],
  "filters": {
    "column_name": "filter_expression"
  },
  "group_by": ["col1", ...],
  "sort": [
    {"column": "col1", "direction": "desc"}
  ],
  "aggregates": {
    "column_name": ["sum", "avg"]
  },
  "explanation": "Brief explanation of what this query shows"
}
```

## LLM Response Format

```json
{
  "table": "google_machines",
  "columns": ["machine", "zone", "gpu_count", "status", "cpu_cores"],
  "filters": {
    "gpu_count": ">2"
  },
  "group_by": ["zone"],
  "sort": [
    {"column": "gpu_count", "direction": "desc"}
  ],
  "aggregates": {
    "gpu_count": ["sum", "count"],
    "cpu_cores": ["avg"]
  },
  "explanation": "Showing machines with more than 2 GPUs, grouped by zone, sorted by GPU count"
}
```

## URL Generation

The backend converts the LLM response to a Taxinomia URL:

```
/table?table=google_machines
  &columns=machine,zone,gpu_count,status,cpu_cores
  &filter:gpu_count=>2
  &group=zone
  &sort=gpu_count:desc
  &agg:gpu_count=sum,count
  &agg:cpu_cores=avg
```

## Implementation Phases

### Phase 1: Schema Export API

Add endpoint to export schema as JSON for LLM context.

**New files:**
- `core/api/schema_export.go` - Schema export logic
- `core/api/schema_export_test.go` - Tests

**Endpoint:** `GET /api/schema`

**Response:**
```json
{
  "tables": [...],
  "entity_types": [...],
  "query_capabilities": {...}
}
```

### Phase 2: Backend Proxy

Create `/api/ai-query` endpoint that:
1. Receives natural language query
2. Fetches schema context
3. Calls LLM API with prompt
4. Parses and validates response
5. Returns structured proposal

**New files:**
- `core/api/ai_query.go` - AI query handler
- `core/api/ai_query_test.go` - Tests

**Configuration:**
```go
type AIQueryConfig struct {
    Provider    string // "claude", "openai", "local"
    APIKey      string
    Model       string
    MaxTokens   int
    Temperature float64
}
```

### Phase 3: Frontend UI

**Components:**
1. Search bar with AI icon
2. Loading state with streaming feedback
3. Proposal display panel
4. Apply/Modify/Cancel buttons
5. Error handling

**Files to modify:**
- `core/rendering/templates/landing.html` - Add search bar to landing
- `core/rendering/templates/table.html` - Add search bar to table view
- New CSS for AI query components
- New JavaScript for API calls and UI updates

### Phase 4: Refinements

1. **Query history** - Store and display recent queries
2. **Favorites** - Save frequently used queries
3. **Learning** - Track user modifications to improve suggestions
4. **Streaming** - Stream LLM response for faster feedback
5. **Fallback** - Graceful degradation when AI unavailable

## Configuration Options

```yaml
ai_query:
  enabled: true
  provider: claude  # claude, openai, local
  api_key: ${AI_API_KEY}
  model: claude-3-haiku  # Fast model for queries

  # Behavior
  auto_apply: false  # Require user confirmation
  show_explanation: true
  max_schema_tables: 50  # Limit for large schemas

  # Rate limiting
  requests_per_minute: 30
  cache_ttl: 300  # Cache identical queries for 5 min
```

## Security Considerations

1. **API Key Management**
   - Store keys in environment variables
   - Never expose keys to frontend
   - Use backend proxy for all LLM calls

2. **Input Validation**
   - Sanitize user queries before sending to LLM
   - Validate LLM response against actual schema
   - Reject queries referencing non-existent tables/columns

3. **Rate Limiting**
   - Limit requests per user/session
   - Cache repeated queries

4. **Output Validation**
   - Verify all table/column names exist
   - Check filter syntax is valid
   - Prevent injection of malicious query params

## Error Handling

| Error | User Message | Action |
|-------|--------------|--------|
| LLM API unavailable | "AI search temporarily unavailable. Try table search." | Fall back to table list |
| Invalid LLM response | "Couldn't understand that query. Try rephrasing." | Show suggestions |
| No matching table | "No tables match that query." | Suggest similar tables |
| Rate limited | "Too many requests. Please wait." | Show cooldown timer |

## Metrics to Track

1. **Usage metrics**
   - Queries per day/user
   - Apply vs. Modify vs. Cancel rates
   - Most common query patterns

2. **Quality metrics**
   - User modification rate (lower = better)
   - Time to apply (faster = better)
   - Repeat query rate

3. **Performance metrics**
   - LLM response time
   - End-to-end latency
   - Cache hit rate

## Future Enhancements

1. **Multi-table queries** - "Compare machines and tasks by cluster"
2. **Follow-up queries** - "Now filter to just us-east1"
3. **Saved queries** - "Run my daily machine health check"
4. **Natural language filters** - "machines added in the last week"
5. **Visualization hints** - "Show this as a bar chart"
