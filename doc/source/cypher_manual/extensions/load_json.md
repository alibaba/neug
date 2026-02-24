# Json Extension

JSON (JavaScript Object Notation) is a widely used data format for web APIs and data exchange. NeuG supports JSON file import functionality through the Extension framework. After loading the JSON Extension, users can directly load external JSON files using the `LOAD FROM` syntax.

## Install Extension

```cypher
INSTALL JSON;
```

## Load Extension

```cypher
LOAD JSON;
```

## Using Json Extension

`LOAD FROM` supports two JSON formats: **JSON Array** and **JSONL** (JSON Lines).

### JSON Format Options

The following options control how JSON files are parsed:

| Option              | Type | Default | Description                                                                                  |
| ------------------- | ---- | ------- | -------------------------------------------------------------------------------------------- |
| `newline_delimited` | bool | `false` | If `true`, treats the file as JSONL format (one JSON object per line). If `false`, treats the file as a JSON array. |

### Supported Formats

#### JSON Array Format

A JSON array contains multiple objects in a single array structure:

```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

When `newline_delimited` is `false` (default), the system parses the entire file as a single JSON array.

#### JSONL Format (JSON Lines)

JSONL format contains one JSON object per line:

```jsonl
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
```

When `newline_delimited` is `true`, the system parses each line as a separate JSON object. This format is particularly efficient for large datasets as it enables streaming processing.


### Query Examples

#### Basic JSON Array Loading

Load all columns from a JSON array file:

```cypher
LOAD FROM "person.json"
RETURN *;
```

#### JSONL Format Loading

Load data from a JSONL file by specifying `newline_delimited=true`:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN *;
```

#### Column Projection

Return only specific columns from JSON data:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN fName, age;
```

#### Column Aliases

Use `AS` to assign aliases to columns:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN fName AS name, age AS years;
```

#### Type Conversion

Use the `CAST` function to convert column values to a specific type:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN fName, CAST(age, 'DOUBLE') AS double_age;
```

#### WHERE Filtering

Filter rows using the `WHERE` clause:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
WHERE age > 30
RETURN name, age;
```

Multiple conditions can be combined using `AND`, `OR`, and `NOT`:

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
WHERE age > 25 AND age < 40
RETURN name, age;
```

#### Aggregate Functions

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN COUNT(*) AS total_count;
```

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN
    SUM(age) AS total_age,
    AVG(age) AS avg_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age;
```

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN name, AVG(age) AS avg_age;
```


#### Sorting and Limiting

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN name, age
ORDER BY age DESC, name ASC;
```

```cypher
LOAD FROM "person.jsonl" (newline_delimited=true)
RETURN name, age
ORDER BY age DESC
LIMIT 10;
```
