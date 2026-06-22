# Load As (Temporary Graph)

`LOAD NODE TABLE AS` / `LOAD REL TABLE AS` reads data from external files (CSV, JSON, JSONL, Parquet, etc.) and creates **temporary node/relationship tables** that participate in standard Cypher queries alongside persistent graph data. Temporary tables exist only for the lifetime of the Connection and are automatically cleaned up when the Connection is closed.

Use cases:

- **Data analysis** — quickly load external data into the graph for exploration without defining a schema upfront.
- **Data blending** — query temporary data together with persistent data in the same Cypher statement.
- **Prototyping** — test import results without modifying persistent storage.

---

## Basic Syntax

### Load Node Table

```cypher
LOAD NODE TABLE FROM "<file_path>" (
    primary_key = '<column_name>',
    ...
)
[WHERE <condition>]
[RETURN <column_list>]
AS <label_name>;
```

### Load Relationship Table

```cypher
LOAD REL TABLE FROM "<file_path>" (
    from = '<source_label>',
    to = '<target_label>',
    from_col = '<source_key_column>',
    to_col = '<target_key_column>',
    ...
)
[WHERE <condition>]
[RETURN <column_list>]
AS <label_name>;
```

---

## Options

LOAD AS uses the same file reading pipeline as [LOAD FROM](load_data), so it supports the same format-specific options (CSV, JSON/JSONL, Parquet) and performance tuning parameters.

### LOAD AS-specific Options

**Node Table:**

| Option          | Type   | Default      | Description                                                                           |
| --------------- | ------ | ------------ | ------------------------------------------------------------------------------------- |
| `primary_key` | string | First column | Name of the primary key column. Must appear in `RETURN` if `RETURN` is specified. |

**Relationship Table:**

| Option       | Type   | Default            | Description                                                                                                                                                                                                                                                                                                                                        |
| ------------ | ------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from`     | string | **Required** | Source node label name (must already exist).                                                                                                                                                                                                                                                                                                       |
| `to`       | string | **Required** | Target node label name (must already exist).                                                                                                                                                                                                                                                                                                       |
| `from_col` | string | First column       | Name of the column containing source node keys. If omitted, defaults to the first column in the file. |
| `to_col`   | string | Second column      | Name of the column containing target node keys. If omitted, defaults to the second column in the file. |

### Format and Performance Options

For format-specific options (CSV `header`, `delim`, JSON `newlines_in_values`, Parquet `buffered_stream`, etc.) and performance tuning (`parallel `, `batch_read `, `batch_size`), see the [LOAD FROM](load_data) documentation.

---

## WHERE Filtering

Use the `WHERE` clause to filter rows during loading. The filter predicate is pushed down to the data source, so non-matching rows are skipped at read time:

```cypher
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id',
    header = true
)
WHERE age >= 18 AND age <= 65
AS AdultWorker;
```

Relationship tables also support WHERE filtering:

```cypher
LOAD REL TABLE FROM "friendships.csv" (
    from = 'Person',
    to = 'Person',
    from_col = 'src_id',
    to_col = 'dst_id',
    header = true
)
WHERE weight > 0.5
AS StrongFriendships;
```

---

## RETURN Projection

Use the `RETURN` clause to select which columns become properties of the temporary table. **Columns not listed in `RETURN` are not loaded**:

```cypher
// Only load id and name; the age column is discarded.
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id',
    header = true
)
RETURN id, name
AS PersonSlim;
```

### Relationship Table Key Column Ordering

For relationship tables, the first two columns of the temporary table schema are always treated as the source and destination key columns. Column order is determined as follows:

1. **With `from_col`/`to_col` (recommended)**: the system automatically places the specified key columns at positions [0/1], regardless of file order or RETURN order:

   ```cypher
   // File columns: weight(0), src_id(1), dst_id(2)
   // from_col/to_col auto-reorder → schema: [src_id, dst_id, weight]
   LOAD REL TABLE FROM "edges.csv"
       (from = 'Person', to = 'Person',
        from_col = 'src_id', to_col = 'dst_id')
   AS Edges;

   // With RETURN: from_col/to_col still force keys to [0/1]
   // RETURN selects columns, from_col/to_col determines key positions
   LOAD REL TABLE FROM "edges.csv"
       (from = 'Person', to = 'Person',
        from_col = 'src_id', to_col = 'dst_id')
   RETURN weight, src_id, dst_id AS Edges;
   // → schema: [src_id, dst_id, weight] (auto-reordered)
   ```

2. **Without `from_col`/`to_col`**: the first two columns in the output order are used as keys. **No automatic reordering occurs** — the user is responsible for ensuring keys are at [0/1]:

   ```cypher
   // File columns: src_id(0), dst_id(1), weight(2) — keys already at [0/1], OK.
   LOAD REL TABLE FROM "edges.csv"
       (from = 'Person', to = 'Person', header = true) AS Edges;

   // With RETURN but no from_col/to_col:
   // RETURN order determines key positions — first two columns become keys.
   // User MUST place key columns first in RETURN.
   LOAD REL TABLE FROM "edges.csv"
       (from = 'Person', to = 'Person')
   RETURN src_id, dst_id, weight AS Edges;  // ✓ keys at [0/1]

   // ✗ WRONG: weight becomes key column [0] — will fail or produce wrong results
   LOAD REL TABLE FROM "edges.csv"
       (from = 'Person', to = 'Person')
   RETURN weight, src_id, dst_id AS Edges;
   ```

> **Best practice**: Always specify `from_col`/`to_col` for relationship tables. This ensures keys are correctly placed at [0/1] regardless of file layout or RETURN order, eliminating positional ambiguity.

### WHERE + RETURN Combined

`WHERE` and `RETURN` can be used together. Columns referenced only in `WHERE` (but not in `RETURN`) are used for filtering but do not become properties:

```cypher
// age is used for filtering but does not become a property of TempAdult.
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id',
    header = true
)
WHERE age >= 25
RETURN id, name
AS TempAdult;

// Query succeeds — id and name are properties.
MATCH (n:TempAdult) RETURN n.id, n.name;

// Query fails — age is not a property.
MATCH (n:TempAdult) RETURN n.age;
```

---

## Supported File Formats

### CSV

```cypher
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'ID',
    header = true,
    delim = ','
) AS TempPerson;
```

### JSON / JSONL

```cypher
LOAD NODE TABLE FROM "person.json" (
    primary_key = 'ID'
) AS Person;

LOAD NODE TABLE FROM "person.jsonl" (
    primary_key = 'ID'
) AS TempPerson;
```

### Parquet

Parquet requires the PARQUET extension to be installed and loaded first:

```cypher
INSTALL PARQUET;
LOAD PARQUET;

LOAD NODE TABLE FROM "person.parquet" (
    primary_key = 'ID'
) AS TempPerson;
```

---

## Automatic Cleanup

Temporary tables are **automatically deleted when the Connection is closed**. No manual cleanup is needed:

```python
from neug import Database

db = Database(db_path="/tmp/mydb", mode="w")
conn = db.connect()

# Create a temporary table.
conn.execute("""
    LOAD NODE TABLE FROM "person.csv"
    (primary_key = 'id', header = true) AS TempPerson;
""")

# The temporary table is accessible.
result = conn.execute("MATCH (n:TempPerson) RETURN count(n);")

# Close the connection — temporary tables are cleaned up.
conn.close()

# A new connection cannot see the temporary table.
conn2 = db.connect()
conn2.execute("MATCH (n:TempPerson) RETURN n.id;")  # Error: label not found
```

## Manual Cleanup

You can also remove a temporary table early using the `DROP TABLE` statement (the same syntax used for persistent tables):

```cypher
LOAD NODE TABLE FROM "person.csv"
    (primary_key = 'id', header = true) AS TempPerson;

// Use the temporary table...
MATCH (n:TempPerson) RETURN count(n);

// Drop it before closing the connection.
DROP TABLE TempPerson;

// The label is no longer accessible.
MATCH (n:TempPerson) RETURN n.id;  // Error: label not found
```

The same syntax works for temporary edge tables:

```cypher
DROP TABLE TempEdges;
```

After dropping a temporary table, the same label name can be reused in another LOAD AS statement on the same Connection.

---

## Mixed Queries with Persistent Data

Temporary tables can be used in the same Cypher query as persistent tables:

```cypher
// Create a persistent table and import data.
CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
COPY Person FROM "persistent_person.csv" (header=true);

// Load a temporary relationship table.
LOAD REL TABLE FROM "extra_edges.csv" (
    from = 'Person',
    to = 'Person',
    from_col = 'src_id',
    to_col = 'dst_id',
    header = true
) AS TempEdges;

// Mixed query: persistent nodes + temporary edges.
MATCH (a:Person)-[r:TempEdges]->(b:Person)
RETURN a.name, b.name, r.weight;
```

---

## Comparison with COPY FROM

| Feature             | LOAD AS                                   | COPY FROM                           |
| ------------------- | ----------------------------------------- | ----------------------------------- |
| Data persistence    | Temporary (deleted on Connection close)   | Persistent (written to disk)        |
| Schema definition   | Automatically inferred                    | Auto-inferred or manually defined   |
| Use case            | Data analysis, prototyping                | Production data import              |
| Performance options | Inherits LOAD FROM performance parameters | Supports batch_read, parallel, etc. |
| WHERE filtering     | Supported                                 | Via LOAD FROM subquery              |
| RETURN projection   | Supported                                 | Via LOAD FROM subquery              |

---

## Error Handling

### Common Errors

**"RETURN must include primary key column"**

```cypher
// Error: RETURN is missing 'id'.
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id',
    header = true
)
RETURN name, age
AS Person;
```

**Fix**: Include the primary key column in `RETURN`:

```cypher
RETURN id, name, age
```

**"LOAD REL TABLE requires `from` and `to` options"**

```cypher
// Error: from/to not specified.
LOAD REL TABLE FROM "edges.csv" (
    header = true
) AS Edges;
```

**Fix**: Specify source and target node labels. Use `from_col`/`to_col` to identify key columns (they also trigger automatic reordering to [0/1]):

```cypher
// Minimal: columns[0] and [1] are assumed to be src/dst keys.
LOAD REL TABLE FROM "edges.csv" (
    from = 'Person',
    to = 'Person',
    header = true
) AS Edges;

// Recommended: specify key columns explicitly.
// from_col/to_col auto-reorder keys to [0/1] regardless of file position.
LOAD REL TABLE FROM "edges.csv" (
    from = 'Person',
    to = 'Person',
    from_col = 'src_id',
    to_col = 'dst_id',
    header = true
) AS Edges;
```

If the file has keys at non-[0/1] positions, use `from_col`/`to_col` which automatically reorders keys to [0/1]:

```cypher
// File: weight(0), src_id(1), dst_id(2) — keys not at [0/1]
// from_col/to_col auto-reorder: schema becomes [src_id, dst_id, weight]
LOAD REL TABLE FROM "edges.csv" (
    from = 'Person',
    to = 'Person',
    from_col = 'src_id',
    to_col = 'dst_id',
    header = true
) AS Edges;
```

---

## Full Examples

### Social Network Analysis

```cypher
// 1. Load user data.
LOAD NODE TABLE FROM "users.csv" (
    primary_key = 'user_id',
    header = true
) RETURN user_id, name, age AS TempUser;

// 2. Load follow relationships (keep only strong ties).
LOAD REL TABLE FROM "follows.csv" (
    from = 'TempUser',
    to = 'TempUser',
    from_col = 'follower_id',
    to_col = 'followee_id',
    header = true
) WHERE strength > 0.7
RETURN follower_id, followee_id, strength
AS TempFollows;

// 3. Find the most followed users.
MATCH (u:TempUser)<-[f:TempFollows]-(:TempUser)
RETURN u.name, count(f) AS followers
ORDER BY followers DESC
LIMIT 10;
```

---

## Known Limitations

1. **No ACID guarantees**: LOAD AS does not guarantee atomicity. If the operation fails after the schema is created but before data insertion completes, the empty temporary schema remains registered until the Connection is closed. To retry with the same label name, use `DROP TABLE <label>` first or choose a different label.
2. **Temporary edge constraints**: Temporary edges can connect temporary nodes to persistent nodes, but persistent edges cannot reference temporary nodes.
3. **Single connection**: LOAD AS is only supported in read-write mode, which means only one Connection exists at a time. Temporary tables are visible only to the Connection that created them and are automatically cleaned up when that Connection is closed.
