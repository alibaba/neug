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
If the data is spread across multiple files in a directory, you can use wildcard characters to specify all files at once. For example
```cypher
COPY person FROM "person*.csv" (header=true);
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

The following options control how CSV files are parsed:

| Option     | Type | Default | Description                                                                                  |
| ---------- | ---- | ------- | -------------------------------------------------------------------------------------------- |
| `delim`    | char | `\|`    | Field delimiter. Can be a single character (e.g. `','`) or an escape character (e.g. `'\t'`) |
| `header`   | bool | `true`  | Whether the first row contains column names                                                  |
| `quote`    | char | `"`     | Quote character used to enclose field values                                                 |
| `escape`   | char | `\`     | Escape character for special characters                                                      |
| `quoting`  | bool | `true`  | Whether to enable quote processing                                                           |
| `escaping` | bool | `true`  | Whether to enable escape character processing                                                |

### Loading with Column Remapping

By combining `COPY FROM` with `LOAD FROM`, NeuG allows users to **remap columns from external files to target properties** during data loading.
This mechanism enables flexible alignment between file schemas and graph schemas without requiring changes to the source files.

Assume that we already have the following two external files: `person_remap.csv` and `knows_remap.csv`.

**person_remap.csv:**

```
age,name
39,marko
27,vadas
32,josh
35,peter
```

**knows_remap.csv:**

```
dst_name,src_name,weight
josh,marko,1.0
vadas,marko,0.5
peter,josh,0.8
```

The following example remaps file columns to node properties:

```cypher
COPY person FROM (
    LOAD FROM "person_remap.csv"
    RETURN name, age
);
```

Similarly, column aliases can be used to remap relationship endpoints and properties:

```cypher
COPY knows FROM (
    LOAD FROM "knows_remap.csv"
    RETURN src_name AS src, dst_name AS dst, weight
);
```

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

| Option       | Type  | Default   | Description                                      |
| ------------ | ----- | --------- | ------------------------------------------------ |
| `batch_read` | bool  | `false`   | Read data incrementally in batches. If disabled, all data will be loaded into memory at once. |
| `batch_size` | int64 | `1048576`(1MB) | Batch size in bytes when `batch_read` is enabled |
| `parallel`   | bool  | `false`   | Enable parallel reading using multiple threads, maximum available CPU cores on the machine is utilized by default. |

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