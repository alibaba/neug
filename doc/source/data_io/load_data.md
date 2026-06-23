# Load Data

`LOAD FROM` is the foundation of NeuG's data ingestion pipeline. It reads external files, **automatically infers the schema** (column names and types), and produces a temporary result set that exists only during query execution. No upfront schema definition is needed.

You can apply standard relational operations — projection, filtering, type casting, aggregation, sorting — directly on the loaded data. This makes `LOAD FROM` ideal for data exploration, validation, and ad-hoc analysis.

With the `AS` clause (`LOAD NODE TABLE ... AS` / `LOAD REL TABLE ... AS`), the loaded data is materialized as **temporary graph tables** (nodes and edges) that persist for the lifetime of the Connection. Once loaded as a graph, you can run full Cypher graph queries — `MATCH`, path traversal, pattern matching — on the temporary data, and mix it with persistent graph data in the same query. See [LOAD AS (Temporary Graph)](#load-as-temporary-graph) below.

---

## LOAD FROM

### Basic Syntax

```cypher
LOAD FROM "<file_path>" (<options>)
[WHERE <condition>]
RETURN <column_list>
[ORDER BY <column> [ASC|DESC]]
[LIMIT <n>];
```

### Parameters

- **`<file_path>`** — Path to the external data source. Currently only local file system paths are supported.
- **`<options>`** — Format-specific and performance-related options (see below).
- **`RETURN <column_list>`** — Columns to return. Use `*` to return all columns.

## Format Options

### CSV

CSV is the built-in format. The following options control how CSV files are parsed:

| Option     | Type | Default | Description |
| ---------- | ---- | ------- | ----------- |
| `delim`    | char | `\|`    | Field delimiter. Can be a single character (e.g. `','`) or an escape character (e.g. `'\t'`) |
| `header`   | bool | `true`  | Whether the first row contains column names |
| `quote`    | char | `"`     | Quote character used to enclose field values |
| `escape`   | char | `\`     | Escape character for special characters |
| `quoting`  | bool | `true`  | Whether to enable quote processing |
| `escaping` | bool | `true`  | Whether to enable escape character processing |

Example:

```cypher
LOAD FROM "person.csv" (delim=',', header=true)
RETURN name, age;
```

### JSON / JSONL

Since NeuG v0.1.2, JSON/JSONL is a built-in format — no extension installation is needed. You can use `LOAD FROM` to read `.json` and `.jsonl` files directly:

```cypher
LOAD FROM "person.json"
RETURN *;
```

> **Version Note:** Since version v0.1.2, we made JSON support a built-in functionality, so you do not need to install the JSON extension before using it. For NeuG version < 0.1.2, JSON support was provided via the JSON Extension and required `INSTALL json; LOAD json;` before use.

See the [JSON Extension](../extensions/load_json) page for format-specific options and examples.

### Parquet

Parquet is supported via the PARQUET extension (available since v0.1.1). After a one-time install and load, you can use `LOAD FROM` to read `.parquet` files directly:

```cypher
INSTALL PARQUET;
LOAD PARQUET;

LOAD FROM "person.parquet"
RETURN *;
```

See the [Parquet Extension](../extensions/load_parquet) page for format-specific options (`buffered_stream`, `pre_buffer`, `enable_io_coalescing`, `parquet_batch_rows`) and examples, including how to export query results to Parquet via `COPY TO`.

## Relational Operations

`LOAD FROM` supports a rich set of relational operations on the loaded data. All the following examples use the [Modern dataset](../cypher_manual/dml_clause.md#loading-node-data).

### Column Projection and Reordering

Columns can be returned in any order, independent of their order in the source file:

```cypher
LOAD FROM "knows.csv" (delim=',')
RETURN weight, dst_name, src_name;
```

### Column Aliases

Use `AS` to assign aliases to columns:

```cypher
LOAD FROM "knows.csv" (delim=',')
RETURN src_name AS src, dst_name AS dst, weight AS score;
```

### Distinct Values

Use `RETURN DISTINCT` to remove duplicate rows from the result:

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN DISTINCT name;
```

You can also use `DISTINCT` with multiple columns:

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN DISTINCT name, age;
```

### Type Casting

Use the `CAST` function to convert column values to a specific type:

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, CAST(age, 'DOUBLE') AS double_age;
```

### WHERE Filtering

Filter rows using the `WHERE` clause. Multiple conditions can be combined using `AND`, `OR`, and `NOT`:

```cypher
LOAD FROM "person.csv" (delim=',')
WHERE age > 25 AND age < 40
RETURN name, age;
```

### Aggregation

`LOAD FROM` supports common aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and grouped aggregation:

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN
    COUNT(*) AS total,
    AVG(age) AS avg_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age;
```

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, AVG(age) AS avg_age;
```

### Sorting and Limiting

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, age
ORDER BY age DESC, name ASC
LIMIT 10;
```

## Performance Options

For large files, the following option can improve read performance:

| Option       | Type  | Default        | Description |
| ------------ | ----- | -------------- | ----------- |
| `parallel`   | bool  | `false`        | Enable parallel reading using multiple threads (max core number). |

> **Note:** Batch reading options (`batch_read`, `batch_size`) are currently supported in [`COPY FROM`](import_data#performance-options), not in `LOAD FROM`. 

Example:

```cypher
LOAD FROM "large_person.csv" (
    delim = ',',
    header = true,
    parallel = true
)
RETURN name, age;
```

---

## LOAD AS (Temporary Graph)

`LOAD NODE TABLE AS` / `LOAD REL TABLE AS` creates **temporary graph tables** from external files. Unlike `LOAD FROM` which produces a flat result set for relational operations, `LOAD AS` materializes the data as graph nodes and edges, enabling full Cypher graph queries (`MATCH`, path traversal, pattern matching) on the loaded data.

Use cases:

- **Graph analysis** — load external data as graph structure for exploration without defining a persistent schema.
- **Data blending** — query temporary tables together with persistent graph data in the same Cypher statement.
- **Prototyping** — test graph queries on external data without modifying persistent storage.

### Syntax

**Node Table:**

```cypher
LOAD NODE TABLE FROM "<file_path>" (
    primary_key = '<column_name>',
    ...
)
[WHERE <condition>]
[RETURN <column_list>]
AS <label_name>;
```

**Relationship Table:**

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

> **Note:** Unlike `LOAD FROM`, `LOAD AS` does not support `ORDER BY`, `LIMIT`, aggregation, or `DISTINCT` — these are relational operations on result sets. `LOAD AS` creates an unordered graph table; use subsequent `MATCH` queries for ordering and aggregation.

### LOAD AS-specific Options

**Node Table:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `primary_key` | string | First column | Primary key column name. Must appear in `RETURN` if specified. |

**Relationship Table:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `from` | string | **Required** | Source node label name (must already exist). |
| `to` | string | **Required** | Target node label name (must already exist). |
| `from_col` | string | First column | Column containing source node keys. Auto-reorders keys to schema positions [0/1]. |
| `to_col` | string | Second column | Column containing target node keys. Auto-reorders keys to schema positions [0/1]. |

Format and performance options (CSV `header`, `delim`, JSON, Parquet, `parallel`) are the same as `LOAD FROM` described above.

### WHERE Filtering

Filter rows during loading. Non-matching rows are skipped at read time:

```cypher
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id', header = true
)
WHERE age >= 18
AS AdultPerson;
```

### RETURN Projection

Select which columns become properties. Columns not in `RETURN` are not loaded:

```cypher
LOAD NODE TABLE FROM "person.csv" (
    primary_key = 'id', header = true
)
RETURN id, name
AS PersonSlim;
```

Columns referenced only in `WHERE` (but not in `RETURN`) are used for filtering but do not become properties.

### Relationship Table Key Column Ordering

The first two columns of the relationship table schema are always treated as source/destination keys:

1. **With `from_col`/`to_col` (recommended)**: keys are automatically placed at positions [0/1] regardless of file order or RETURN order.
2. **Without `from_col`/`to_col`**: first two columns in output order become keys. User must ensure keys are first.

```cypher
// File: weight(0), src_id(1), dst_id(2)
// from_col/to_col auto-reorder: schema becomes [src_id, dst_id, weight]
LOAD REL TABLE FROM "edges.csv" (
    from = 'Person', to = 'Person',
    from_col = 'src_id', to_col = 'dst_id', header = true
) AS Knows;
```

> **Best practice**: Always specify `from_col`/`to_col` for relationship tables to avoid positional ambiguity.

### Lifecycle Management

**Automatic cleanup:** Temporary tables are deleted when the Connection is closed.

```python
conn.execute('LOAD NODE TABLE FROM "person.csv" (primary_key=\'id\', header=true) AS Temp;')
conn.execute("MATCH (n:Temp) RETURN count(n);")  # works
conn.close()
# Temp is gone
```

**Manual cleanup:** Use `DROP TABLE` to release a temporary table early:

```cypher
DROP TABLE TempPerson;
```

**Mixed queries:** Temporary tables can be used alongside persistent tables:

```cypher
MATCH (a:Person)-[r:TempEdges]->(b:Person)
RETURN a.name, b.name, r.weight;
```

### Full Example

```cypher
// 1. Load temporary nodes and edges.
LOAD NODE TABLE FROM "users.csv" (
    primary_key = 'user_id', header = true
) AS TempUser;

LOAD REL TABLE FROM "follows.csv" (
    from = 'TempUser', to = 'TempUser',
    from_col = 'follower_id', to_col = 'followee_id', header = true
) WHERE strength > 0.7
RETURN follower_id, followee_id, strength
AS TempFollows;

// 2. Run graph queries on temporary data.
MATCH (u:TempUser)<-[f:TempFollows]-(:TempUser)
RETURN u.name, count(f) AS followers
ORDER BY followers DESC
LIMIT 10;
```

### Known Limitations

1. **No ACID guarantees**: If loading fails mid-way, the empty temporary schema remains until Connection close. Use `DROP TABLE` to retry.
2. **Temporary edge constraints**: Temporary edges can connect to persistent nodes, but persistent edges cannot reference temporary nodes.
3. **Single connection**: Only supported in read-write mode (one Connection at a time).
