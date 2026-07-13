# Performance Debugging with EXPLAIN and PROFILE

EXPLAIN and PROFILE are Cypher query execution commands that allow you to inspect query execution plans and collect performance metrics during query execution.

## Quick Reference

- **EXPLAIN**: View the execution plan without executing the query
- **PROFILE**: Execute the query and collect per-operator timing and row count statistics

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
CREATE NODE TABLE person(
    id INT64,
    firstName STRING,
    lastName STRING,
    gender STRING,
    birthday DATE,
    creationDate TIMESTAMP,
    locationIP STRING,
    browserUsed STRING,
    PRIMARY KEY(id)
);

CREATE NODE TABLE place(
    id INT64,
    name STRING,
    url STRING,
    type STRING,
    PRIMARY KEY(id)
);

CREATE NODE TABLE organisation(
    id INT64,
    type STRING,
    name STRING,
    url STRING,
    PRIMARY KEY(id)
);

CREATE REL TABLE workAt(
    FROM person TO organisation,
    workFrom INT32
);

CREATE REL TABLE isLocatedIn(
    FROM person TO place
);

CREATE REL TABLE organisationIsLocatedIn(
    FROM organisation TO place
);

-- Load data
COPY person FROM '/{neug_dir}/example_dataset/ldbc/person_0_0.csv' (delim='|', header=true);
COPY place FROM '/{neug_dir}/example_dataset/ldbc/place_0_0.csv' (delim='|', header=true);
COPY organisation FROM '/{neug_dir}/example_dataset/ldbc/organisation_0_0.csv' (delim='|', header=true);
COPY workAt FROM '/{neug_dir}/example_dataset/ldbc/person_workAt_organisation_0_0.csv' (from="person", to="organisation", delim='|', header=true);
COPY isLocatedIn FROM '/{neug_dir}/example_dataset/ldbc/person_isLocatedIn_place_0_0.csv' (from="person", to="place", delim='|', header=true);
```

### EXPLAIN Example

#### Simple Node Scan
```
neug > EXPLAIN MATCH (p:person) RETURN p.firstName, p.lastName;
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

#### Edge Traversal

```
neug > EXPLAIN MATCH (p:person)-[w:workAt]->(o:organisation) RETURN p.firstName, p.lastName, o.name, w.workFrom;
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

```
neug > EXPLAIN MATCH (p:person)-[w:workAt]->(o:organisation)-[ol:organisationIsLocatedIn]->(pl:place)
         RETURN p.firstName, p.lastName, o.name, pl.name;
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

```
neug > EXPLAIN MATCH (p1:person), (p2:person)
         WHERE p1.id < p2.id
         RETURN p1.firstName, p1.lastName, p2.firstName, p2.lastName LIMIT 10;
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
│               LimitOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:     0 tuples   │
└───────────────────────────────────────┘
```

#### Example with Aggregation

```
neug > EXPLAIN MATCH (p:person)-[w:workAt]->(o:organisation)
         RETURN o.name,
                COUNT(p.id) as emp_count,
                MIN(p.id) as min_emp_id,
                MAX(p.id) as max_emp_id,
                AVG(p.id) as avg_emp_id;
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

```
neug > PROFILE MATCH (p:person) RETURN p.firstName, p.lastName;
+------------------+-----------------+
| _0_p.firstName   | _0_p.lastName   |
+==================+=================+
| Mahinda          | Perera          |
+------------------+-----------------+
| Eli              | Peretz          |
+------------------+-----------------+
| Joseph           | Anderson        |
+------------------+-----------------+
| Yacine           | Abdelli         |
+------------------+-----------------+
| Jose             | Alonso          |
+------------------+-----------------+
| ...              | ...             |
+------------------+-----------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 903
Total elapsed time: 0.001 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.001s | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:   903 tuples   │
└───────────────────────────────────────┘
```

### Simple Edge Traversal 

```
neug > PROFILE MATCH (p:person)-[w:workAt]->(o:organisation) RETURN p.firstName, p.lastName, o.name, w.workFrom;
+------------------+-----------------+---------------------------------------+-----------------+
| _0_p.firstName   | _0_p.lastName   | _2_o.name                             | _4_w.workFrom   |
+==================+=================+=======================================+=================+
| Mahinda          | Perera          | SriLankan_Airlines                    | 2013            |
+------------------+-----------------+---------------------------------------+-----------------+
| Mahinda          | Perera          | Aero_Lanka                            | 2013            |
+------------------+-----------------+---------------------------------------+-----------------+
| Mahinda          | Perera          | Deccan_Lanka                          | 2013            |
+------------------+-----------------+---------------------------------------+-----------------+
| Eli              | Peretz          | Atlas_Blue                            | 2007            |
+------------------+-----------------+---------------------------------------+-----------------+
| Eli              | Peretz          | Jet4you                               | 2007            |
+------------------+-----------------+---------------------------------------+-----------------+
| ...              | ...             | ...                                   | ...             |
+------------------+-----------------+---------------------------------------+-----------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 1953
Total elapsed time: 0.002 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.001s | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandEOpr             │
├───────────────────────────────────────┤
│   time: 0.001s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│           GetVFromEdgesOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
```

#### Multi-hop Traversal

```
neug > PROFILE MATCH (p:person)-[w:workAt]->(o:organisation)-[ol:organisationIsLocatedIn]->(pl:place)
         RETURN p.firstName, p.lastName, o.name, pl.name;
+------------------+-----------------+---------------------------------------+---------------+
| _0_p.firstName   | _0_p.lastName   | _2_o.name                             | _6_pl.name    |
+==================+=================+=======================================+===============+
| Mahinda          | Perera          | SriLankan_Airlines                    | Sri_Lanka     |
+------------------+-----------------+---------------------------------------+---------------+
| Mahinda          | Perera          | Aero_Lanka                            | Sri_Lanka     |
+------------------+-----------------+---------------------------------------+---------------+
| Mahinda          | Perera          | Deccan_Lanka                          | Sri_Lanka     |
+------------------+-----------------+---------------------------------------+---------------+
| Eli              | Peretz          | Atlas_Blue                            | Morocco       |
+------------------+-----------------+---------------------------------------+---------------+
| Eli              | Peretz          | Jet4you                               | Morocco       |
+------------------+-----------------+---------------------------------------+---------------+
| ...              | ...             | ...                                   | ...           |
+------------------+-----------------+---------------------------------------+---------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 1953
Total elapsed time: 0.002 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.001s | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandEOpr             │
├───────────────────────────────────────┤
│   time: 0.001s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│           GetVFromEdgesOpr            │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:  1953 tuples   │
└───────────────────────────────────────┘
```

#### Example with Join

```
neug > PROFILE MATCH (p1:person), (p2:person)
         WHERE p1.id < p2.id
         RETURN p1.firstName, p1.lastName, p2.firstName, p2.lastName LIMIT 10;
+-------------------+------------------+-------------------+------------------+
| _0_p1.firstName   | _0_p1.lastName   | _2_p2.firstName   | _2_p2.lastName   |
+===================+==================+===================+==================+
| Mahinda           | Perera           | Eli               | Peretz           |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Joseph            | Anderson         |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Yacine            | Abdelli          |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Jose              | Alonso           |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Steve             | Moore            |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | John              | Rao              |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Jun               | Wang             |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | A. C.             | Bos              |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Karim             | Akhmadiyeva      |
+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Hermann           | Becker           |
+-------------------+------------------+-------------------+------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 10
Total elapsed time: 0.134 s

┌───────────────────────────────────────┐
│                JoinOpr                │
├───────────────────────────────────────┤
│  time: 0.062s | rows: 815409 tuples   │
└───────────────────────────────────────┘
├─ child 0
│    ┌───────────────────────────────────────┐
│    │           ScanWithGPredOpr            │
│    ├───────────────────────────────────────┤
│    │   time: 0.000s | rows:   903 tuples   │
│    └───────────────────────────────────────┘
└─ child 1
     ┌───────────────────────────────────────┐
     │           ScanWithGPredOpr            │
     ├───────────────────────────────────────┤
     │   time: 0.000s | rows:   903 tuples   │
     └───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               SelectOpr               │
├───────────────────────────────────────┤
│  time: 0.055s | rows: 407253 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│  time: 0.015s | rows: 407253 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               LimitOpr                │
├───────────────────────────────────────┤
│   time: 0.002s | rows:    10 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:    10 tuples   │
└───────────────────────────────────────┘
```

#### Example with Aggregation

```
neug > PROFILE MATCH (p:person)-[w:workAt]->(o:organisation)
         RETURN o.name,
                COUNT(p.id) as emp_count,
                MIN(p.id) as min_emp_id,
                MAX(p.id) as max_emp_id,
                AVG(p.id) as avg_emp_id;
+---------------------------------------+-------------+----------------+----------------+--------------------+
| _2_o.name                             | emp_count   | min_emp_id     | max_emp_id     | avg_emp_id         |
+=======================================+=============+================+================+====================+
| SriLankan_Airlines                    | 2           | 933            | 19791209300504 | 9895604650718.5    |
+---------------------------------------+-------------+----------------+----------------+--------------------+
| Aero_Lanka                            | 4           | 933            | 28587302322689 | 13743895347790.0   |
+---------------------------------------+-------------+----------------+----------------+--------------------+
| Deccan_Lanka                          | 2           | 933            | 15393162788911 | 7696581394922.0    |
+---------------------------------------+-------------+----------------+----------------+--------------------+
| Atlas_Blue                            | 5           | 6597069767117  | 19791209300562 | 12754334882790.2   |
+---------------------------------------+-------------+----------------+----------------+--------------------+
| Jet4you                               | 2           | 6597069767117  | 15393162789162 | 10995116278139.5   |
+---------------------------------------+-------------+----------------+----------------+--------------------+
| ...                                   | ...         | ...            | ...            | ...                |
+---------------------------------------+-------------+----------------+----------------+--------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 793
Total elapsed time: 0.006 s

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.002s | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.002s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.001s | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.001s | rows:   793 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.000s | rows:   793 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.000s | rows:   793 tuples   │
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
