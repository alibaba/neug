# COPY FROM

`COPY FROM` persists external data into NeuG's graph storage. It builds on top of [`LOAD FROM`](load_data) — internally, `COPY FROM` uses `LOAD FROM` to read and parse external files, then writes the result into node or relationship tables.

For **import without creating tables first**, see [COPY FROM without a predefined schema](#copy-from-without-a-predefined-schema).

## Schema requirement

`COPY FROM` supports two modes:

1. **Existing table** — The target node or relationship label already exists in the catalog. Column layout must match the table (see the sections below).
2. **No predefined schema** — The target label does **not** exist yet. NeuG can **infer** column names and types from the data source (same pipeline as [`LOAD FROM`](load_data)), **create** the node or relationship type, and **import** rows in one statement. This is controlled with the **`auto_detect`** option (default: **`true`**).

If `auto_detect` is **`false`** and the table is missing, the binder reports that the table does not exist.

> **Note:** All option names are case-insensitive — for example, `HEADER`, `Header`, and `header` are all equivalent.

---

## COPY FROM without a predefined schema

When **`auto_detect`** is enabled (the default), a `COPY ... FROM` into a **new** label skips manual `CREATE NODE TABLE` / `CREATE REL TABLE` for that label. The compiler builds a plan that applies DDL for the inferred type, then runs the same bulk insert path as a normal `COPY`.

### Option: `auto_detect`

| Option         | Type | Default | Description |
| -------------- | ---- | ------- | ----------- |
| `auto_detect`  | bool | `true`  | If the target table does not exist, infer schema from the scan/sniff result and create it before insert. If `false`, a missing table is an error. |

You can set it explicitly when needed:

```cypher
COPY person FROM "person.csv" (header=true, auto_detect=true);
COPY person FROM "person.csv" (header=true, auto_detect=false);  -- require table to exist
```

### Nodes (new label)

- The **first column** of the source (after any `LOAD FROM ... RETURN` projection) is used as the **primary key** property.
- **All columns** become node properties; types are taken from type inference on the file or subquery output.
- The **COPY target** (`COPY <Label> FROM ...`) is the new **vertex label** name.

```cypher
-- File has header: id,name,age — id becomes PRIMARY KEY
COPY person FROM "person.csv" (header=true);
```

If the file column order is wrong for inference, reorder with a subquery (first returned column = primary key):

```cypher
COPY person FROM (
    LOAD FROM "person.csv" (header=true)
    RETURN user_id AS id, name, age
);
```

### Relationships (new edge type)

For a **new** relationship label, NeuG still needs to know the **endpoint node types**. You must pass **`from`** and **`to`** — each must name an **existing** node table (create or `COPY` those nodes first).

- The **first two columns** of the source are the **source** and **destination** endpoint keys (values must match the primary keys of the `from` / `to` node tables).
- **Remaining columns** map to relationship properties.

```cypher
-- After person nodes exist:
COPY knows FROM "knows.csv" (
    from="person",
    to="person",
    header=true
);
```

You can combine with a `LOAD FROM` subquery for column order, filtering, or projection, same as for node `COPY`.

### Import order

Rules are unchanged: **endpoints must exist** before loading edges. For no-schema edge `COPY`, create or import **`from` / `to`** node tables first, then run the edge `COPY`.

### Limitations (current behavior)

- **Edge inference** uses a **single** `(from, to)` pair per `COPY`; multi-pair relationship groups still need an existing schema or explicit DDL.
- **No-schema edge `COPY`** requires **`from`** and **`to`** to refer to **already registered** vertex types (no automatic creation of endpoint types in that step).

### JSON and Parquet Support

No-schema `COPY FROM` also works with JSON and Parquet files when the corresponding extension is loaded:

- [JSON Extension — COPY FROM JSON (No Schema)](../extensions/load_json#copy-from-json-no-schema)
- [Parquet Extension — COPY FROM Parquet (No Schema)](../extensions/load_parquet#copy-from-parquet-no-schema)

---

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

- **If `auto_detect` is `true` (default)** for a **new** label, NeuG should infer schema and create the table; if you still see this error, check that the data source binds correctly (e.g. file path, `header`, delimiter) so columns are discovered. For edges, **`from` and `to`** must name **existing** node types — create or `COPY` those nodes first.
- **If `auto_detect` is `false`**, the target table must already exist:

```cypher
-- With auto_detect=false, table must exist
COPY User FROM "users.csv" (header=true, auto_detect=false);

-- Either create DDL first:
CREATE NODE TABLE User(id INT64 PRIMARY KEY, name STRING);
COPY User FROM "users.csv" (header=true, auto_detect=false);

-- Or rely on default auto_detect=true for a new label (see section above)
COPY NewLabel FROM "users.csv" (header=true);
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
