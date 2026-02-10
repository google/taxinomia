# Google Infrastructure Demo Tables

This document outlines demo tables modeling Google's internal infrastructure, focusing on the physical hierarchy (machine → rack → cluster → zone → region) and workload hierarchy (cell → job → task → alloc).

## Overview

Two parallel hierarchies:

```
Physical Hierarchy          Workload Hierarchy
─────────────────          ──────────────────
region                     cell
  └── zone                   └── job
        └── cluster              └── task
              └── rack               └── alloc
                    └── machine
```

## Entity Types

| Entity Type | Description | Primary Table |
|-------------|-------------|---------------|
| `google.region` | Geographic region (us-east, europe-west) | regions |
| `google.zone` | Data center within a region | zones |
| `google.cluster` | Group of machines managed together | clusters |
| `google.rack` | Physical rack of servers | racks |
| `google.machine` | Individual server | machines |
| `google.cell` | Borg cell managing workloads | cells |
| `google.job` | Workload definition | jobs |
| `google.task` | Running instance of a job | tasks |
| `google.alloc` | Resource allocation | allocs |

## Tables

### Physical Hierarchy

#### 1. regions
Geographic regions containing data centers.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| region | string | google.region | Region identifier (e.g., us-east, europe-west) |
| name | string | | Full name (US East, Europe West) |
| continent | string | | Americas, Europe, Asia-Pacific |
| zone_count | int | | Number of zones |
| total_machines | int | | Total machines in region |
| network_capacity_tbps | float | | Network backbone capacity |

**Sample data**: 8 regions.

#### 2. zones
Data centers within regions.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| zone | string | google.zone | Zone identifier (e.g., us-east-a) |
| region | string | google.region | Parent region |
| name | string | | Data center name |
| status | string | | operational, degraded, maintenance |
| cluster_count | int | | Number of clusters |
| total_machines | int | | Machines in zone |
| power_capacity_mw | float | | Power capacity in megawatts |
| pue | float | | Power Usage Effectiveness |

**Sample data**: 24 zones (3 per region).

#### 3. clusters
Groups of machines managed together within a zone.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| cluster | string | google.cluster | Cluster identifier |
| zone | string | google.zone | Parent zone |
| region | string | google.region | Parent region |
| purpose | string | | serving, batch, storage, ml |
| generation | string | | Hardware generation (gen3, gen4, gen5) |
| rack_count | int | | Number of racks |
| total_machines | int | | Machines in cluster |
| status | string | | active, draining, offline |

**Sample data**: 120 clusters (5 per zone).

#### 4. racks
Physical racks of servers within clusters.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| rack | string | google.rack | Rack identifier |
| cluster | string | google.cluster | Parent cluster |
| zone | string | google.zone | Parent zone |
| position | string | | Row and position (e.g., A-12) |
| machine_count | int | | Machines in rack |
| power_draw_kw | float | | Current power draw |
| network_switch | string | | Top-of-rack switch ID |
| status | string | | healthy, degraded, offline |

**Sample data**: 2400 racks (20 per cluster).

#### 5. machines
Individual servers.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| machine | string | google.machine | Machine identifier |
| rack | string | google.rack | Parent rack |
| cluster | string | google.cluster | Parent cluster |
| zone | string | google.zone | Parent zone |
| cpu_cores | int | | Total CPU cores |
| memory_gb | int | | Total RAM in GB |
| disk_tb | float | | Total disk in TB |
| gpu_count | int | | Number of GPUs (0 for most) |
| cpu_arch | string | | x86_64, arm64 |
| status | string | | healthy, degraded, dead, repair |
| cpu_free_cores | float | | Available CPU |
| memory_free_gb | float | | Available memory |
| uptime_days | int | | Days since last reboot |

**Sample data**: 10000 machines (4-5 per rack average).

### Workload Hierarchy

#### 6. cells
Borg cells that manage workloads (typically 1:1 with clusters).

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| cell | string | google.cell | Cell identifier |
| cluster | string | google.cluster | Backing cluster |
| zone | string | google.zone | Zone |
| version | string | | Borg version |
| job_count | int | | Active jobs |
| task_count | int | | Running tasks |
| cpu_allocated | float | | Total CPU allocated |
| memory_allocated_gb | float | | Total memory allocated |
| utilization_pct | float | | Overall utilization |

**Sample data**: 120 cells (1 per cluster).

#### 7. jobs
Workload definitions (like Kubernetes Deployments).

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| job | string | google.job | Job identifier (user/jobname) |
| cell | string | google.cell | Running in cell |
| cluster | string | google.cluster | Running in cluster |
| zone | string | google.zone | Running in zone |
| owner | string | | Team owning the job |
| priority | string | | production, batch, best-effort |
| state | string | | running, pending, stopped |
| task_count | int | | Number of tasks |
| cpu_request | float | | CPU cores per task |
| memory_request_gb | float | | Memory per task |
| created_at | datetime | | Job creation time |
| binary | string | | Binary path/name |

**Sample data**: 5000 jobs across cells.

#### 8. tasks
Running instances of jobs (like Kubernetes Pods).

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| task | string | google.task | Task identifier (job/index) |
| job | string | google.job | Parent job |
| cell | string | google.cell | Running in cell |
| machine | string | google.machine | Running on machine |
| rack | string | google.rack | Machine's rack |
| cluster | string | google.cluster | Machine's cluster |
| state | string | | running, starting, dead, preempted |
| cpu_usage | float | | Current CPU usage (cores) |
| memory_usage_gb | float | | Current memory usage |
| start_time | datetime | | Task start time |
| restarts | int | | Number of restarts |
| exit_code | int | | Last exit code (null if running) |

**Sample data**: 50000 tasks.

#### 9. allocs
Resource allocations for tasks.

| Column | Type | Entity Type | Description |
|--------|------|-------------|-------------|
| alloc | string | google.alloc | Allocation identifier |
| task | string | google.task | Parent task |
| job | string | google.job | Parent job |
| machine | string | google.machine | Allocated on machine |
| resource_type | string | | cpu, memory, disk, gpu |
| requested | float | | Requested amount |
| limit | float | | Hard limit |
| used | float | | Current usage |
| unit | string | | cores, gb, count |

**Sample data**: 150000 allocs (3 per task: cpu, memory, disk).

## Join Relationships

```
Physical:
regions ──[region]──> zones ──[zone]──> clusters ──[cluster]──> racks ──[rack]──> machines

Workload:
cells ──[cell]──> jobs ──[job]──> tasks ──[task]──> allocs

Cross-hierarchy:
cells ──[cluster]──> clusters
tasks ──[machine]──> machines
```

## Use Cases

1. **Capacity planning**: Find racks with degraded machines, clusters with low utilization
2. **Job debugging**: Trace job → tasks → machines to find hardware issues
3. **Resource analysis**: Aggregate CPU/memory by job priority, cell, or zone
4. **Failure correlation**: Group dead tasks by rack/cluster to detect hardware failures
5. **Scheduling analysis**: Compare requested vs used resources across jobs

## Sample Queries

- "Show all tasks from job X, grouped by machine status"
- "Find racks with >20% degraded machines"
- "Aggregate CPU utilization by zone and priority"
- "List jobs with tasks that were preempted, grouped by cell"
