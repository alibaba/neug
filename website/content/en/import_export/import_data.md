# Import Data

NeuG supports inserting data into a database from a specified file. The prerequisite for inserting data into a database is that you first create a graph schema, i.e., the structure of your node and relationship tables.

Generally, you can use the `COPY FROM` command to import data from a file into the database.

## Copy from CSV

### Import into Node Table
Create a node table `person` as follows:
```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```
The CSV file `person.csv` contains the following columns:
```csv
id|name|age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
...
```
The following statement will import `person.csv` into person table.
```cypher
COPY User FROM "user.csv" (header=true);
```
**Note:**
- The number of columns in the CSV file must be equal to the number of properties defined for the node.
- The order of columns in the CSV file must align with the order of properties defined for the node. For example, the above node has properties ordered: id, age, name, which correspond to the columns in the CSV file.
### Import into Relationship Table
Create a relationship table `knows` using the following query:
```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```cypher
The CSV file `person_knows_person.csv` contains the following columns:
```csv
person.id|person.id|weight
1|2|0.5
1|4|1.0
...
```
The following statement imports the `person_knows_person.csv` file into the `knows` table.
```cypher
COPY knows from "person_knows_person.csv" (from="person", to="person", header=true);
```
**Note**
- When importing into a relationship table, NeuG assumes the first two columns in the file are the primary keys of the `FROM` and `TO` nodes; The rest of the columns correspond to relationship properties.
- The `FROM` and `TO` parameters must be included in the CSV configurations to specify the labels of the `FROM` and `TO` nodes.


### CSV configurations
The following configuration parameters are supported:
|Parameter|Description|Default|
|---|---|---|
|`HEADER`|Whether the first line of the CSV file is the header. Can be true or false.|`false`|
|`DELIM` or `DELIMITER`|Character that separates different columns in a lines.|`\|`|
|`QUOTE`|Character to start a string quote.|`"`|
|`ESCAPE`|Character within string quotes to escape QUOTE and other characters, e.g., a line break.|`\`|
|`SKIP`|Number of rows to skip from the input file.|`0`|
|`PARALLEL`|Read CSV files in parallel or not.|`true`|
|`IGNORE_ERRORS`|Skips malformed rows in CSV files if set to true.|`false`|
|`NULL_STRINGS`|The strings that should be treated as nulls in the CSV file.|`""`(empty string)|
|`FROM`|Name of source vertex label, only used in edge importing|`-`|
|`TO`|Name of destination vertex label, only used in edge importing|`-`|

**Note**
- Parameters should be specified as comma-separated key-value pairs, such as: `param1=value1, param2=value2, ...`
- Parameters are case-insensitive, meaning `Header`, `HEADER`, and `header` are all valid and will be recognized correctly in the parameter list.
- For boolean-typed parameters (e.g., `header`)：
  1. Use `True`, `true`, or `1` to indicate enabled.
  2. Use `False`, `false`, or `0` to indicate disabled.
  3. The value part can be omitted if it is a `true` value, e.g. `header=true` can be written as `header` for short.
- `FROM` and `TO` are a special pair of parameters. Only effective when importing edges from a CSV file. When multiple label combinations exist for the source and target vertices of an edge (e.g., Comment-replyOf->Post, Comment-replyOf->Comment), `FROM` and `TO` must be specified. Using these parameters in other scenarios will throw an exception.