# Export Data
The `COPY TO` command enables direct export of query results to a specified file format. This is useful for persisting the result of a query to be used in other systems or preserving query outputs for archival use.

## Copy to CSV
The COPY TO clause can export query results to a CSV file and is used as follows:
```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.csv' (header=true);
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
|`HEADER`|Whether to output a header row.|`false`|
|`DELIM` or `DELIMITER`|Character that separates fields in the CSV.|`\|`|

Another example is shown below.
```cypher
COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO 'person_knows_person.csv' (header=true);
```
This outputs the following results to `person_knows_person.csv`:
```csv
e
{'_SRC': '0:0', '_DST': '0:1', '_LABEL': 'knows', 'weight': 0.500000}
{'_SRC': '0:0', '_DST': '0:2', '_LABEL': 'knows', 'weight': 1.000000}
```