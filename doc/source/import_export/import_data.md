# Import Data

NeuG supports importing data into a database from various file formats. Currently, CSV files are fully supported, with other formats such as Parquet and DataFrame =coming soon. This guide shows you how to efficiently load large datasets into your graph database.

## Quick Start: Complete Import Workflow

Here's a complete example of importing a social network dataset:

### Step 1: Prepare Your Data Files

**users.csv**:
```csv
id,name,age,email
1,Alice Johnson,30,alice@example.com
2,Bob Smith,25,bob@example.com
3,Carol Davis,28,carol@example.com
```

**friendships.csv**:
```csv
from_user_id,to_user_id,since_year
1,2,2020
2,3,2019
1,3,2021
```

### Step 2: Create Schema
```cypher
-- Create node table for users
CREATE NODE TABLE User(
    id INT64 PRIMARY KEY,
    name STRING,
    age INT64,
    email STRING
);

-- Create relationship table for friendships
CREATE REL TABLE FRIENDS(
    FROM User TO User,
    since_year INT64
);
```

### Step 3: Import Data
```cypher
-- Import users first (order matters!)
COPY User FROM "users.csv" (header=true, delimiter=",");

-- Then import relationships
COPY FRIENDS FROM "friendships.csv" (
    from="User", 
    to="User", 
    header=true, 
    delimiter=","
);
```

### Step 4: Verify Import
```cypher
-- Check if data was imported correctly
MATCH (u:User) RETURN count(u) as user_count;
MATCH ()-[f:FRIENDS]->() RETURN count(f) as friendship_count;

-- Sample the data
MATCH (u1:User)-[f:FRIENDS]->(u2:User) 
RETURN u1.name, u2.name, f.since_year 
LIMIT 5;
```

**🎉 Success!** You've imported a complete social network.

---

## Detailed Reference

The `COPY FROM` command is the primary way to import data from CSV files into NeuG.

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
COPY person FROM "person.csv" (header=true);
```
**Note:**
- The number of columns in the CSV file must be equal to the number of properties defined for the node.
- The order of columns in the CSV file must align with the order of properties defined for the node. For example, the above node has properties ordered: id, name, age, which correspond to the columns in the CSV file.
### Import into Relationship Table
Create a relationship table `knows` using the following query:
```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```
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

## 🛠️ Troubleshooting

### Common Import Issues

#### 1. "Table does not exist" Error
```cypher
-- ❌ Wrong: Importing before creating schema
COPY User FROM "users.csv" (header=true);

-- ✅ Correct: Create schema first
CREATE NODE TABLE User(id INT64 PRIMARY KEY, name STRING);
COPY User FROM "users.csv" (header=true);
```

#### 2. "Column count mismatch" Error
This happens when your CSV has different number of columns than your table schema.

**Solution**: Make sure CSV columns match table properties exactly:
```csv
-- ❌ Wrong: Missing 'age' column
id,name,email
1,Alice,alice@example.com

-- ✅ Correct: All columns present
id,name,age,email
1,Alice,30,alice@example.com
```

#### 3. "Primary key violation" Error
**Solution**: Check for duplicate IDs in your CSV file before importing:
```bash
# Check for duplicates in CSV (using command line tools)
cut -d',' -f1 users.csv | sort | uniq -d

# Or use a spreadsheet program to identify duplicate IDs
```

After importing, verify no duplicates exist:
```cypher
-- Count records per ID to find duplicates
MATCH (u:User) 
RETURN u.id, count(u) as record_count 
ORDER BY record_count DESC 
LIMIT 10;
```

### Performance Tips

#### 1. Import Order Matters
```cypher
-- ✅ Always import nodes before relationships
COPY User FROM "users.csv" (header=true);
COPY Company FROM "companies.csv" (header=true);
-- Only after all nodes are loaded:
COPY WORKS_FOR FROM "works_for.csv" (header=true);
```

#### 2. Use Parallel Import for Large Files
```cypher
-- Enable parallel processing for faster imports
COPY User FROM "large_users.csv" (header=true, parallel=true);
```

#### 3. Batch Processing for Very Large Datasets
For files larger than 1GB, consider splitting them:
```bash
# Split large CSV into smaller chunks
split -l 100000 large_users.csv users_chunk_

# Then COPY User FROM each chunked file. 
# Note that only use `header = true` for the first chunk
```

## Coming Soon: Additional Format Support

NeuG is expanding import capabilities to support more data formats:

- **Parquet Files**: High-performance columnar format, ideal for analytics workloads
- **DataFrame Integration**: Direct import from pandas DataFrames and other data science tools
- and more ...

These features will provide even more flexibility for data scientists and analysts working with diverse data sources.

## What's Next?

After successfully importing your data:
1. **[Query your data](../cypher_manual/index.md)** - Learn Cypher query language
2. **[Export data](export_data.md)** - Export data when needed