# Hierarchies in Taxinomia

Hierarchies define containment relationships between entity types, enabling navigation and filtering across related tables even when they don't share direct columns.

## Overview

A hierarchy defines a tree structure where each level contains the next. For example, the Google infrastructure hierarchy:

```
region → zone → cluster → rack → machine
```

This means:
- A region contains multiple zones
- A zone contains multiple clusters
- A cluster contains multiple racks
- A rack contains multiple machines

## Defining Hierarchies

Hierarchies are defined in the `data_sources.textproto` configuration:

```protobuf
hierarchies {
  name: "google.infrastructure"
  description: "Physical infrastructure hierarchy from region down to individual machines"
  levels: "google.region"
  levels: "google.zone"
  levels: "google.cluster"
  levels: "google.rack"
  levels: "google.machine"
}
```

## Hierarchy Ancestor Columns

When a table's primary key is part of a hierarchy, Taxinomia can add **joined ancestor columns** for missing ancestor entity types. These columns enable filtering and grouping by ancestor values.

### Column Naming Convention

Ancestor columns use simple names derived from the entity type:

| Entity Type | Column Name |
|-------------|-------------|
| `google.region` | `region` |

The `§` prefix is only used if there's a naming collision (e.g., a column named "region" already exists with a different entity type).

### Current Implementation

The current implementation uses `Manager.AddHierarchyAncestorColumns()` to create joined columns that chain through intermediate tables.

**However, this approach may be overcomplicating things.** In practice:

1. **Tables already have most ancestor columns directly.** The demo tables are created with columns for most of the hierarchy:
   - machines table has: rack, cluster, zone (only missing region)
   - racks table has: cluster, zone (only missing region)
   - clusters table has: zone, region (complete)

2. **Only single-hop joins are needed.** Since tables already have most ancestors, we typically only need to join one level up to get the missing value (e.g., machines.zone → zones.region).

3. **The chained joiner logic is rarely exercised.** The complex multi-hop join chain code exists but is almost never used because tables are already denormalized.

### Simpler Alternatives

Consider these simpler approaches:

1. **Add columns at table creation time.** When creating tables, include all ancestor columns directly. This is what the demo already does for most columns.

2. **Use standard joined columns.** Instead of special `§`-prefixed columns, use the regular join infrastructure that already exists for cross-table lookups.

3. **Rely on existing columns.** Since tables already have most hierarchy columns, filtering by ancestors mostly works without any special infrastructure.

## Filtering by Ancestors

Ancestor columns enable filtering descendant tables by ancestor values. For example, to show all machines in a specific region:

```
table?table=google_machines&filter:region="us-east"
```

Or filter by zone (which machines table already has):
```
table?table=google_machines&filter:zone="us-east-a"
```

## Hierarchy Navigation in Detail Pane

When viewing an item's details, the hierarchy context shows:

1. **Ancestors** - Items above in the hierarchy (with links to navigate up)
2. **Current** - The selected item
3. **Descendants** - Links to list items below in the hierarchy

For example, viewing cluster `us-east-a-c0`:

```
Ancestors:
  Region: us-east → [click to view region]
  Zone: us-east-a → [click to view zone]

Current:
  Cluster: us-east-a-c0

Descendants:
  Racks → [click to list racks in this cluster]
  Machines → [click to list machines in this cluster]
```

## Implementation Notes

### Current Approach

The `Manager.AddHierarchyAncestorColumns()` method:
1. Finds hierarchies containing the entity type
2. For each missing ancestor, builds a join chain through intermediate tables
3. Creates a `§`-prefixed joined column

### Limitations

- **Requires globally unique primary keys.** The join lookup requires that entity values be unique across all tables. This fails for the workload hierarchy where job names are only unique within a cell.

- **Complex for little benefit.** The chained joiner logic handles multi-hop joins, but in practice tables already have most columns directly.

### Recommendation

For new hierarchies, consider simply adding all ancestor columns when creating the table. This is simpler, faster (no join lookups), and more predictable. The join-based approach is only needed when you can't modify the source tables.
