# LOAD FROM

The `LOAD FROM` clause loads external data sources as **temporary tables** and executes relational operations directly on them.
This mechanism is designed for **lightweight, on-the-fly analysis** of external data without importing it into persistent graph storage.

`LOAD FROM` is particularly suitable for exploratory queries, data validation, and one-time analytical workloads.


## Basic Syntax

```cypher
LOAD FROM "<file_path>" (<options>)
RETURN <column_list>;
```

### Parameters

* **`<file_path>`**
  Specifies the external data source path.
  Currently, only local file system paths are supported.

* **`<options>`**
  Specifies format-related and performance-related options.
  Supported options depend on the data format.

* **`RETURN <column_list>`**
  Specifies the columns to return.
  Use `*` to return all columns.


## LOAD FROM CSV

CSV (Comma-Separated Values) is the most commonly used format for temporary table operations.

### CSV Format Options

The following options control how CSV files are parsed:

| Option     | Type | Default | Description                                                                                  |
| ---------- | ---- | ------- | -------------------------------------------------------------------------------------------- |
| `delim`    | char | `\|`    | Field delimiter. Can be a single character (e.g. `','`) or an escape character (e.g. `'\t'`) |
| `header`   | bool | `true`  | Whether the first row contains column names                                                  |
| `quote`    | char | `"`     | Quote character used to enclose field values                                                 |
| `escape`   | char | `\`     | Escape character for special characters                                                      |
| `quoting`  | bool | `true`  | Whether to enable quote processing                                                           |
| `escaping` | bool | `true`  | Whether to enable escape character processing                                                |


### Query Examples

All the following query examples use the [Modern dataset](./dml_clause.md#loading-node-data).

#### Specifying CSV Options

```cypher
LOAD FROM "person.csv" (
    delim = ',',
    header = true
)
RETURN name, age;
```

#### Column Reordering

Columns can be returned in any order, independent of their order in the source file:

```cypher
LOAD FROM "knows.csv" (delim=',')
RETURN weight, dst_name, src_name;
```

#### Column Aliases

Use `AS` to assign aliases to columns:

```cypher
LOAD FROM "knows.csv" (delim=',')
RETURN src_name AS src, dst_name AS dst, weight AS score;
```

#### Type Conversion

Use the `CAST` function to convert column values to a specific type:

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, CAST(age, 'DOUBLE') AS double_age;
```


## LOAD FROM JSON

`LOAD JSON` is supported by extension framework in NeuG, please refer to [JSON Extension](./extensions/load_json.md) for more details.


## LOAD FROM PARQUET

<!-- TODO -->


## Relational Operators with LOAD FROM

In addition to `RETURN`, `LOAD FROM` can be combined with standard relational operators to express more complex queries.


### WHERE Filtering

Filter rows using the `WHERE` clause:

```cypher
LOAD FROM "person.csv" (delim=',')
WHERE age > 30
RETURN name, age;
```

Multiple conditions can be combined using `AND`, `OR`, and `NOT`:

```cypher
LOAD FROM "person.csv" (delim=',')
WHERE age > 25 AND age < 40
RETURN name, age;
```


### Aggregate Functions

`LOAD FROM` supports common aggregate functions.

#### Row Count

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN COUNT(*) AS total_count;
```

#### Aggregation Functions

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN
    SUM(age) AS total_age,
    AVG(age) AS avg_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age;
```

#### Grouped Aggregation

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, AVG(age) AS avg_age;
```


### Sorting and Limiting

#### Sorting Results

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, age
ORDER BY age DESC, name ASC;
```

#### Top-K Queries

```cypher
LOAD FROM "person.csv" (delim=',')
RETURN name, age
ORDER BY age DESC
LIMIT 10;
```


## Advanced Features

### Performance Options

For large files, the following options can be enabled to improve performance and control memory usage:

| Option       | Type  | Default   | Description                                      |
| ------------ | ----- | --------- | ------------------------------------------------ |
| `batch_read` | bool  | `false`   | Read data incrementally in batches. If disabled, all data will be loaded into memory at once. |
| `batch_size` | int64 | `1048576`(1MB) | Batch size in bytes when `batch_read` is enabled |
| `parallel`   | bool  | `false`   | Enable parallel reading using multiple threads, maximum available CPU cores on the machine is utilized by default. |

#### Example

```cypher
LOAD FROM "large_person.csv" (
    delim = ',',
    header = true,
    batch_read = true,
    batch_size = 2097152, // 2MB
    parallel = true
)
RETURN name, age;
```