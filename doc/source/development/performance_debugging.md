# Performance Debugging with EXPLAIN and PROFILE

NeuG provides powerful built-in tools for query performance analysis and debugging: **EXPLAIN** and **PROFILE** modes. These features allow you to understand query execution plans and measure per-operator performance metrics without leaving your client.

## Overview

### EXPLAIN Mode

**EXPLAIN** shows the physical execution plan of your query without actually executing it. This is useful for:
- Understanding how the query optimizer plans your query
- Identifying suboptimal query structures before execution
- Debugging complex joins and aggregations

**Results**: Returns the operator tree structure with 0 data rows.

### PROFILE Mode

**PROFILE** executes your query and collects detailed per-operator metrics. This is useful for:
- Identifying performance bottlenecks in slow queries
- Comparing execution times across different query structures
- Understanding data distribution impact on execution

**Results**:
1. **Query data rows** - The actual result set from your query
2. **Operator tree structure** - The execution plan (same as EXPLAIN)
3. **Per-operator metrics** - Execution time and row count for each operator

## CLI Usage

Start the interactive shell with a local database:

```bash
./neug-cli open /path/to/db
```

Then in the interactive shell:

```sql
-- Create schema
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
CREATE NODE TABLE software(id INT64, title STRING, lang STRING, PRIMARY KEY(id));
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
CREATE REL TABLE created(FROM person TO software, weight DOUBLE, since INT64);

-- Load data
COPY person FROM '${NEUG_ROOT}/example_dataset/modern_graph/person.csv';
COPY software FROM '${NEUG_ROOT}/example_dataset/modern_graph/software.csv';
COPY knows FROM '${NEUG_ROOT}/example_dataset/modern_graph/person_knows_person.csv' (from="person", to="person");
COPY created FROM '${NEUG_ROOT}/example_dataset/modern_graph/person_created_software.csv' (from="person", to="software");
```

### EXPLAIN Example

#### Simple Node Scan
```sql
neug > EXPLAIN MATCH (n:person) RETURN n.name;

No results (total records: 0)

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

#### Multi-hop Traversal
```sql
neug > EXPLAIN MATCH (n:person)-[e1:knows]->(m:person), (m)-[e2:created]->(s:software) RETURN n.name, s.title;

No results (total records: 0)

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandEOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│           GetVFromEdgesOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

#### Example with Join

```sql
neug > EXPLAIN MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name;

No results (total records: 0)

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│                JoinOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
├─ child 0
│    ┌───────────────────────────────────────┐
│    │           ScanWithGPredOpr            │
│    ├───────────────────────────────────────┤
│    │   time: 0.000s | rows:     0 tuples   │
│    └───────────────────────────────────────┘
└─ child 1
     ┌───────────────────────────────────────┐
     │           ScanWithGPredOpr            │
     ├───────────────────────────────────────┤
     │   time: 0.000s | rows:     0 tuples   │
     └───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               SelectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

Example with Aggregation

```sql
neug > EXPLAIN MATCH (n:person) RETURN COUNT(*) as person_count;

No results (total records: 0)

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

### PROFILE Examples

#### Simple Node Scan

```sql
neug > PROFILE MATCH (n:person) RETURN n.name;
+-------------+
| _0_n.name   |
+=============+
| marko       |
+-------------+
| vadas       |
+-------------+
| josh        |
+-------------+
| peter       |
+-------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 4
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
```

#### Multi-hop Traversal

```sql
neug > PROFILE MATCH (n:person)-[e1:knows]->(m:person), (m)-[e2:created]->(s:software) RETURN n.name, m.name, s.title;
+-------------+-------------+--------------+
| _0_n.name   | _2_m.name   | _6_s.title   |
+=============+=============+==============+
| marko       | josh        | lop          |
+-------------+-------------+--------------+
| marko       | josh        | ripple       |
+-------------+-------------+--------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 2
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandEOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     2 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│           GetVFromEdgesOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     2 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     2 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     2 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     2 tuples   │
└───────────────────────────────────────┘
```

#### Example with Join

```sql
neug > PROFILE MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name;
+--------------+--------------+
| _0_p1.name   | _2_p2.name   |
+==============+==============+
| marko        | vadas        |
+--------------+--------------+
| marko        | josh         |
+--------------+--------------+
| marko        | peter        |
+--------------+--------------+
| vadas        | josh         |
+--------------+--------------+
| vadas        | peter        |
+--------------+--------------+
| josh         | peter        |
+--------------+--------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 6
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│                JoinOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:    16 tuples   │
└───────────────────────────────────────┘
├─ child 0
│    ┌───────────────────────────────────────┐
│    │           ScanWithGPredOpr            │
│    ├───────────────────────────────────────┤
│    │   time: 0.000s | rows:     4 tuples   │
│    └───────────────────────────────────────┘
└─ child 1
     ┌───────────────────────────────────────┐
     │           ScanWithGPredOpr            │
     ├───────────────────────────────────────┤
     │   time: 0.000s | rows:     4 tuples   │
     └───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               SelectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     6 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     6 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     6 tuples   │
└───────────────────────────────────────┘
```

#### Example with Aggregation

```sql
neug > PROFILE MATCH (n:person) RETURN COUNT(*) as person_count;
+----------------+
|   person_count |
+================+
|              4 |
+----------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 1
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     1 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     1 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     1 tuples   │
└───────────────────────────────────────┘
```

## Python Client Usage

### Basic Setup

```python
from neug.database import Database

# Local mode (AP - embedded)
db = Database("/path/to/db", mode="w")
conn = db.connect()
```

Or for remote mode (TP - HTTP service):

```python
from neug.session import Session

# Remote mode (TP)
session = Session.open("http://localhost:10000")
conn = session  # Session object has same interface as Connection
```

### PROFILE Mode in Python

```python
# Execute with PROFILE
result = conn.execute("PROFILE MATCH (p:person) RETURN p.name, p.age")

# Consume query results
rows = list(result)

print("--- Query Results ---")
print(f"Columns: {result.column_names()}")
for i, row in enumerate(rows, 1):
    print(f"  Row {i}: {row}")
print(f"Total rows returned: {len(rows)}\n")

# Check if profile data exists
if result.has_profile_result():
    # Option 1: Get formatted text for display
    print("--- Formatted Text Output ---")
    print(result.get_profile_text())
    
    # Option 2: Get structured dict for programmatic access
    print("\n--- Structured Metrics ---")
    metrics = result.get_profile_metrics()
    print(f"Total time: {metrics['total_elapsed_ms']:.2f} ms")
    print(f"Output rows: {metrics['total_output_rows']}")
    print(f"Number of operators: {len(metrics['operators'])}")
    
    # Analyze per-operator metrics
    for op in metrics['operators']:
        print(f"{  - op['operator_name']}: {op['elapsed_ms']:.3f} ms, {op['output_rows']} rows")
```

Sample output:
```
--- Query Results ---
Columns: ['_0_p.name', '_0_p.age']
  Row 1: ['marko', 29]
  Row 2: ['vadas', 27]
  Row 3: ['josh', 32]
  Row 4: ['peter', 35]
Total rows returned: 4

--- Formatted Text Output (suitable for CLI) ---

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 4
Total elapsed time: 0.001 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.001s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     4 tuples   │
└───────────────────────────────────────┘


--- Structured Metrics (suitable for programming) ---
Total time: 0.91 ms
Output rows: 4
Number of operators: 3
  - ScanWithGPredOpr: 0.91 ms, 4 rows
  - ProjectOpr: 0.43 ms, 4 rows
  - SinkOpr: 0.00 ms, 4 rows
```

### EXPLAIN Mode in Python

```python
# Execute with EXPLAIN (shows plan, no execution)
result = conn.execute("EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name")

# EXPLAIN returns no data rows
print("--- Query Results ---")
rows = list(result)

# But still provides plan structure
if result.has_profile_result():
    plan_text = result.get_profile_text()
    print("--- Execution Plan ---")
    print(plan_text)
```

Sample Output:
```
--- Query Results ---
Rows returned: 0

--- Execution Plan ---

╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.000 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

## Python Interface Reference

NeuG provides three methods for accessing query execution metrics through the Python API:

### has_profile_result()

Check if profile/explain result is available for the query. Returns `True` if the query was executed with `PROFILE` or `EXPLAIN` mode, `False` for normal queries.

### get_profile_text()

Get human-readable formatted output of the execution plan and metrics. Shows operator names, execution times, row counts and the tree structure of operators. 

### get_profile_metrics()

Get complete profile metrics as a structured Python dictionary for programmatic analysis.

#### Return Value Structure

Returns a dictionary with this structure:

```python
{
    "total_elapsed_ms": float,      # Total query execution time
    "total_output_rows": int,       # Total rows returned
    "operators": [
        {
            "operator_id": int,     # Unique operator ID in plan tree
            "parent_id": int,       # Parent operator ID (-1 for root)
            "operator_name": str,   # e.g., "NodeScan", "EdgeTraversal", "Project"
            "elapsed_ms": float,    # Time spent in this operator
            "output_rows": int,     # Rows produced by this operator
            "child_ids": [int],     # IDs of child operators
        },
        # ... more operators
    ]
}
```