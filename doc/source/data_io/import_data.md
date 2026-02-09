# COPY FROM

`COPY FROM` persists external data into NeuG's graph storage. It builds on top of [`LOAD FROM`](load_data) — internally, `COPY FROM` uses `LOAD FROM` to read and parse external files, then writes the result into node or relationship tables.

## Schema Requirement

Currently, `COPY FROM` requires a **predefined schema** — you must create node/relationship tables before importing data. The columns in the external file must match the table properties.

> **Tip:** Coming in v0.2: NeuG will support schema-flexible persistent import — allowing `COPY FROM` to leverage the capability of type inference of `LOAD FROM`, without requiring a predefined schema. This will make it much easier to quickly onboard new datasets.

## Quick Start

Here is a complete example of importing a social network dataset from CSV.

### Step 1: Prepare Data Files

**users.csv:**
```csv
id,name,age,email
1,Alice Johnson,30,alice@example.com
2,Bob Smith,25,bob@example.com
3,Carol Davis,28,carol@example.com
```

**friendships.csv:**
```csv
from_user_id,to_user_id,since_year
1,2,2020
2,3,2019
1,3,2021
```

### Step 2: Create Schema
```cypher
CREATE NODE TABLE User(
    id INT64 PRIMARY KEY,
    name STRING,
    age INT64,
    email STRING
);

CREATE REL TABLE FRIENDS(
    FROM User TO User,
    since_year INT64
);
```

### Step 3: Import Data
```cypher
-- Import nodes first (order matters — nodes must exist before relationships)
COPY User FROM "users.csv" (header=true, delimiter=",");

-- Then import relationships
COPY FRIENDS FROM "friendships.csv" (
    from="User",
    to="User",
    header=true,
    delimiter=","
);
```

### Step 4: Verify
```cypher
MATCH (u:User) RETURN count(u) AS user_count;

MATCH (u1:User)-[f:FRIENDS]->(u2:User)
RETURN u1.name, u2.name, f.since_year
LIMIT 5;
```

---

## Import into Node Table

Create a node table and import data:

```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```

```cypher
COPY person FROM "person.csv" (header=true);
```

If data is spread across multiple files, use wildcard characters:

```cypher
COPY person FROM "person*.csv" (header=true);
```

> **Note:** The number and order of columns in the CSV file must match the properties defined in the node table exactly.

## Import into Relationship Table

Create a relationship table and import data:

```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```

```cypher
COPY knows FROM "person_knows_person.csv" (from="person", to="person", header=true);
```

> **Note:** NeuG assumes the first two columns are the primary keys of the `FROM` and `TO` nodes. The remaining columns correspond to relationship properties. The `from` and `to` parameters must be specified to identify the endpoint node tables.

## CSV Options

The following options control how CSV files are parsed during `COPY FROM`. These are the same options supported by [`LOAD FROM`](load_data#csv):

| Option     | Type | Default | Description |
| ---------- | ---- | ------- | ----------- |
| `delim`    | char | `\|`    | Field delimiter |
| `header`   | bool | `true`  | Whether the first row contains column names |
| `quote`    | char | `"`     | Quote character |
| `escape`   | char | `\`     | Escape character |
| `quoting`  | bool | `true`  | Whether to enable quote processing |
| `escaping` | bool | `true`  | Whether to enable escape character processing |

## Column Remapping with LOAD FROM

Since `COPY FROM` builds on `LOAD FROM`, you can use a `LOAD FROM` subquery to **preprocess data before persisting** — including column reordering, renaming, type casting, and filtering.

### Reordering Columns

When the file columns are in a different order from the table schema:

**person_remap.csv:**
```
age,name,id
39,marko,1
27,vadas,2
32,josh,3
35,peter,4
```

```cypher
COPY person FROM (
    LOAD FROM "person_remap.csv"
    RETURN id, name, age
);
```

### Remapping Relationship Endpoints

**knows_remap.csv:**
```
dst_name,src_name,weight
josh,marko,1.0
vadas,marko,0.5
peter,josh,0.8
```

```cypher
COPY knows FROM (
    LOAD FROM "knows_remap.csv"
    RETURN src_name AS src, dst_name AS dst, weight
);
```

### Filtering During Import

You can filter rows before persisting, avoiding the need to clean the source file:

```cypher
COPY person FROM (
    LOAD FROM "person.csv" (delim=',')
    WHERE age >= 18
    RETURN *
);
```

For the full set of relational operations available in `LOAD FROM` subqueries, see the [LOAD FROM](load_data) reference.

---

## Performance Options

| Option       | Type  | Default        | Description |
| ------------ | ----- | -------------- | ----------- |
| `batch_read` | bool  | `false`        | Read data incrementally in batches. |
| `batch_size` | int64 | `1048576` (1MB) | Batch size in bytes when `batch_read` is enabled. |
| `parallel`   | bool  | `false`        | Enable parallel reading using multiple threads (max core number). |

```cypher
COPY User FROM "large_users.csv" (header=true, parallel=true);
```

### Import Order

Always import **nodes before relationships**, since relationship endpoints must reference existing nodes:

```cypher
COPY User FROM "users.csv" (header=true);
COPY Company FROM "companies.csv" (header=true);
-- Only after all nodes are loaded:
COPY WORKS_FOR FROM "works_for.csv" (header=true);
```

### Large Datasets

For files larger than 1GB, consider splitting them:

```bash
# Split large CSV into smaller chunks
split -l 100000 large_users.csv users_chunk_

# Then COPY User FROM each chunked file.
# Note: only use `header=true` for the first chunk
```

---

## Troubleshooting

### "Table does not exist" Error

Schema must be created before importing:

```cypher
-- Wrong: importing before creating schema
COPY User FROM "users.csv" (header=true);

-- Correct: create schema first
CREATE NODE TABLE User(id INT64 PRIMARY KEY, name STRING);
COPY User FROM "users.csv" (header=true);
```

### "Column count mismatch" Error

The CSV must have the same number of columns as the table schema:

```csv
-- Wrong: missing 'age' column
id,name,email
1,Alice,alice@example.com

-- Correct: all columns present
id,name,age,email
1,Alice,30,alice@example.com
```

### "Primary key violation" Error

Check for duplicate IDs in the source file:

```bash
cut -d',' -f1 users.csv | sort | uniq -d
```
