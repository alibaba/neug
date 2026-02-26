# Load Data

`LOAD FROM` is the foundation of NeuG's data ingestion pipeline. It reads external files, **automatically infers the schema** (column names and types), and produces a temporary result set that exists only during query execution. No upfront schema definition is needed.

You can apply standard relational operations — projection, filtering, type casting, aggregation, sorting — directly on the loaded data. This makes `LOAD FROM` ideal for data exploration, validation, and ad-hoc analysis.

## Basic Syntax

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

JSON support is provided via the [JSON Extension](../extensions/load_json). After installing and loading the extension, `LOAD FROM` can read `.json` and `.jsonl` files:

```cypher
INSTALL json;
LOAD EXTENSION json;

LOAD FROM "person.json"
RETURN *;
```

See the [JSON Extension](../extensions/load_json) page for format-specific options and examples.

### Parquet

Parquet support is planned for v0.2.

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
