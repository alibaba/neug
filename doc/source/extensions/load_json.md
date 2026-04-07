# JSON Extension

JSON (JavaScript Object Notation) is a widely used data format for web APIs and data exchange. NeuG supports JSON file import functionality through the Extension framework. After loading the JSON Extension, users can directly load external JSON files using the `LOAD FROM` syntax, or export query results to JSON files using the `COPY TO` syntax.

## Install Extension

```cypher
INSTALL JSON;
```

## Load Extension

```cypher
LOAD JSON;
```

## Using JSON Extension

### Supported Formats

Both import (`LOAD FROM`) and export (`COPY TO`) support two JSON formats: **JSON Array** and **JSONL** (JSON Lines). The format is inferred automatically from the file extension, so no explicit configuration is required.

| Extension | Format | Description |
| --------- | ------ | ----------- |
| `.json`   | JSON array | One JSON array containing all result rows as objects. |
| `.jsonl`  | JSON Lines | One JSON object per line (same as the JSONL import format). |

#### JSON Array Format

A JSON array contains multiple objects in a single array structure:

```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

For paths with a `.json` extension (e.g. `person.json`), NeuG automatically treats the file as a JSON array for both import and export.

#### JSONL Format (JSON Lines)

JSONL format contains one JSON object per line:

```jsonl
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
```

For paths with a `.jsonl` extension (e.g. `person.jsonl`), NeuG automatically treats the file as JSONL (one JSON object per line) for both import and export.

### Load from JSON

#### Basic JSON Array Loading

Load all columns from a JSON array file:

```cypher
LOAD FROM "person.json"
RETURN *;
```

#### JSONL Format Loading

Load data from a JSONL file. When the path has a `.jsonl` extension, the format is auto-detected;

```cypher
LOAD FROM "person.jsonl"
RETURN *;
```

#### Column Projection

Return only specific columns from JSON data:

```cypher
LOAD FROM "person.jsonl"
RETURN fName, age;
```

#### Column Aliases

Use `AS` to assign aliases to columns:

```cypher
LOAD FROM "person.jsonl"
RETURN fName AS name, age AS years;
```

> **Note:** All relational operations supported by `LOAD FROM` — including type conversion, WHERE filtering, aggregation, sorting, and limiting — work the same way with JSON files. See the [LOAD FROM reference](../data_io/load_data) for the complete list of operations.

### Export to JSON

With the JSON extension loaded, you can export query results to JSON or JSONL using the `COPY TO` syntax.

#### Export as JSON Array

Export the result of a query to a single JSON array file:

```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.json';
```

This produces a file such as:

```json
[{"id": 1, "name": "marko", "age": 29},{"id": 2, "name": "vadas", "age": 27}]
```

#### Export as JSONL

Export to JSONL (one object per line) by using a `.jsonl` path:

```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.jsonl';
```

Example output:

```jsonl
{"id": 1, "name": "marko", "age": 29}
{"id": 2, "name": "vadas", "age": 27}
```

JSONL is well-suited for large result sets and streaming. You can control how many rows are written per batch with the `BATCH_SIZE` parameter:

| Parameter   | Description                                              | Default |
| ----------- | --------------------------------------------------------- | ------- |
| `BATCH_SIZE` | Maximum number of rows to write in a single batch.        | `1024`  |

For more on export options and best practices, see [Export Data](../data_io/export_data).
