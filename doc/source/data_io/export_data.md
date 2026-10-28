# Export Data

The `COPY TO` command exports query results directly to CSV, JSON, or JSONL files. Additional formats are available through the [Extension](../extensions/index) framework.

## Copy to CSV

The COPY TO clause can export query results to a CSV file and is used as follows:
```cypher
COPY (MATCH (p:Person) RETURN p.*) TO 'person.csv' (header=true);
```

The CSV file consists of the following fields:
```csv
p.id|p.name|p.age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
```
Complex types, such as vertices and edges, will be output in their JSON-formatted strings.

Available parameters are:
|Parameter|Description|Default|
|---|---|---|
|`HEADER`|Whether to output a header row.|`true`|
|`DELIM` or `DELIMITER`|Character that separates fields in the CSV.|`\|`|
|`BATCH_SIZE`|Maximum number of rows to write in a single batch.|`1024`|

Another example is shown below.
```cypher
COPY (MATCH (:Person)-[e:KNOWS]->(:Person) RETURN e) TO 'person_knows_person.csv' (header=true);
```
This outputs the following results to `person_knows_person.csv`:
```csv
e
{"_SRC":"0:0","_DST":"0:1","_SRC_LABEL":"Person","_DST_LABEL":"Person","_LABEL":"KNOWS","weight":0.5}
{"_SRC":"0:0","_DST":"0:2","_SRC_LABEL":"Person","_DST_LABEL":"Person","_LABEL":"KNOWS","weight":1.0}
```

## Copy to JSON

JSON export is built in. You can export query results to JSON or JSONL format:

```cypher
COPY (MATCH (p:Person) RETURN p.*) TO 'person.json';
COPY (MATCH (p:Person) RETURN p.*) TO 'person.jsonl';
```

The output format is determined by the file extension:
- `.json` — JSON array format (all rows in a single array)
- `.jsonl` — JSON Lines format (one JSON object per line)

JSON output looks like this:

```json
[{"id": 1, "name": "marko", "age": 29},{"id": 2, "name": "vadas", "age": 27}]
```

The equivalent JSONL output contains one object per line:

```jsonl
{"id": 1, "name": "marko", "age": 29}
{"id": 2, "name": "vadas", "age": 27}
```

The common `BATCH_SIZE` option described above also applies to JSON and JSONL
exports.

## Additional Export Formats

NeuG is expanding export capabilities through the [Extension](../extensions/index) framework. Planned export formats include:

- **Parquet Export**: High-performance columnar format for analytics and data science workflows
- **DataFrame Integration**: Direct export to pandas DataFrames and other data science tools

See the [Extensions](../extensions/index) page for the latest supported formats.

## Export Best Practices

- **Large Result Sets**: Use LIMIT clauses to avoid memory issues when exporting large datasets
- **Data Types**: Complex graph objects (nodes/edges) are exported as JSON strings for maximum compatibility
- **File Paths**: Ensure the target directory exists and is writable before running export commands
