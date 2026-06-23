# Data Pipeline Tutorial: Cloud Storage → Graph Query → Parquet Export

This tutorial walks you through a complete data pipeline using NeuG v0.1.2:

1. Read Parquet files directly from cloud storage (no download)
2. Auto-create graph tables without writing DDL
3. Run graph queries
4. Export results back to Parquet (local or cloud)

**Time**: ~10 minutes
**Prerequisites**: `pip install neug==0.1.2`, internet access

---

## Step 1: Install and Load Extensions

NeuG uses extensions for remote file access and Parquet support. The first time you run `install`, NeuG downloads the extension binary. After that, you only need `load` to activate it.

```python
from neug import Database
import tempfile

db = Database(db_path=tempfile.mkdtemp(), mode="w")
conn = db.connect()

conn.execute("install httpfs")
conn.execute("install parquet")
conn.execute("load httpfs")
conn.execute("load parquet")
```

> **Note**: `install` downloads the extension (one-time). `load` activates it for the current session.

---

## Step 2: Preview Remote Data

Before importing, let's peek at the data directly from a public OSS endpoint — no need to download anything:

```python
result = conn.execute('''
    LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
    RETURN ID, fName, age, isStudent
    LIMIT 5
''')
for row in result:
    print(row)
```

Expected output:

```
[0, 'Alice', 35, True]
[2, 'Bob', 30, True]
[3, 'Carol', 45, False]
[5, 'Dan', 20, False]
[7, 'Elizabeth', 20, True]
```

The same file can be accessed via OSS scheme (useful when you need credential-based access):

```python
result = conn.execute('''
    LOAD FROM "oss://graphscope/neug/vPerson.parquet" (
        CREDENTIALS_KIND='Anonymous',
        ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com'
    )
    RETURN ID, fName
    LIMIT 3
''')
```

---

## Step 3: Import Nodes — One Line, No DDL

> **Note:** `COPY FROM` is only supported in [embedded mode](../data_io/index.md#embedded-mode-only), not in service mode.

Traditionally, importing data into a graph database requires you to first define the schema (column names, types, primary key) with a `CREATE NODE TABLE` statement. In NeuG v0.1.2, you can skip all of that:

```python
conn.execute('''
    COPY Person FROM (
        LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
        RETURN *
    )
''')
```

NeuG automatically:

- Infers all column names and types from the Parquet file schema
- Uses the first column as the primary key
- Creates the node table and imports data in one step

Verify:

```python
result = conn.execute("MATCH (p:Person) RETURN count(p)")
print(list(result))  # [[8]]
```

The file has 16 columns (INT64, STRING, BOOL, DOUBLE, DATE) — all correctly inferred.

---

## Step 4: Import Edges

Edge tables work the same way, with one addition — you need to specify which node tables the edge connects:

```python
conn.execute('''
    COPY MEETS FROM (
        LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/eMeets.parquet"
        RETURN *
    ) (from="Person", to="Person")
''')
```

Verify:

```python
result = conn.execute("MATCH ()-[e:MEETS]->() RETURN count(e)")
print(list(result))  # [[7]]
```

---

## Step 5: Graph Queries

Now you have a graph. Query it:

```python
result = conn.execute('''
    MATCH (a:Person)-[m:MEETS]->(b:Person)
    WHERE a.age > 30
    RETURN a.fName, b.fName, m.location
''')
for row in result:
    print(row)
```

---

## Step 6: Export Query Results to Parquet

Export filtered graph query results:

```python
conn.execute('''
    COPY (
        MATCH (a:Person)-[m:MEETS]->(b:Person)
        WHERE a.age < 35
        RETURN a.fName AS name, a.age AS age, b.fName AS met_person, m.location
    ) TO '/tmp/young_social.parquet'
''')
```

This exports only the young (age < 35) people's social connections. The result is a standard Parquet file that any downstream tool (Spark, pandas, DuckDB) can consume directly.

### Export to Cloud Storage

In production, you can write directly to OSS or S3 — no local disk involved:

```python
conn.execute('''
    COPY (
        MATCH (a:Person)-[m:MEETS]->(b:Person)
        WHERE a.age < 35
        RETURN a.fName AS name, a.age AS age, b.fName AS met_person, m.location
    ) TO "oss://my-bucket/output/young_social.parquet" (
        CREDENTIALS_KIND='Explicit',
        OSS_ACCESS_KEY_ID='<your-access-key-id>',
        OSS_ACCESS_KEY_SECRET='<your-access-key-secret>',
        ENDPOINT_OVERRIDE='oss-cn-hangzhou.aliyuncs.com'
    )
''')
```

---

## Step 7: Convert to PyArrow Table

If you want to continue analysis in Python (pandas, polars, DuckDB), convert query results directly to a PyArrow Table:

```python
result = conn.execute('''
    MATCH (p:Person)
    RETURN p.ID, p.fName, p.age
    ORDER BY p.ID
''')
table = result.to_arrow()
print(table.schema)
print(table.to_pandas())
```

---

## Credential Configuration Reference

### OSS

| Parameter                 | Description                                                                                            |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| `CREDENTIALS_KIND`      | `'Explicit'` (provide AK/SK), `'Anonymous'` (public data), `'Default'` (environment credentials) |
| `OSS_ACCESS_KEY_ID`     | Your AccessKey ID                                                                                      |
| `OSS_ACCESS_KEY_SECRET` | Your AccessKey Secret                                                                                  |
| `ENDPOINT_OVERRIDE`     | OSS endpoint, e.g.`'oss-cn-hangzhou.aliyuncs.com'`                                                   |

### S3

| Parameter                | Description                                    |
| ------------------------ | ---------------------------------------------- |
| `CREDENTIALS_KIND`     | `'Explicit'`, `'Anonymous'`, `'Default'` |
| `S3_ACCESS_KEY_ID`     | Your AWS Access Key ID                         |
| `S3_SECRET_ACCESS_KEY` | Your AWS Secret Access Key                     |
| `S3_REGION`            | AWS region, e.g.`'us-east-1'`                |
| `ENDPOINT_OVERRIDE`    | Custom endpoint (for S3-compatible services)   |

### URL Schemes

| Scheme | Example                                       |
| ------ | --------------------------------------------- |
| HTTPS  | `https://bucket.endpoint/path/file.parquet` |
| OSS    | `oss://bucket/path/file.parquet`            |
| S3     | `s3://bucket/path/file.parquet`             |

---

## Common Pitfalls

### 1. Extension load order matters

Always `install` before `load`. If you get "Extension not found", run `install` first:

```cypher
install httpfs;    -- downloads (one-time)
install parquet;   -- downloads (one-time)
load httpfs;       -- activates
load parquet;      -- activates
```

### 2. LOAD FROM vs COPY FROM

- `LOAD FROM` = scan/preview remote data without importing into the graph
- `COPY ... FROM (LOAD FROM ...)` = import into the graph (creates table if not exists)

### 3. Node table: first column is the primary key

For node tables, the first column of the file automatically becomes the primary key. Make sure your data file has an ID column in the first position.

### 4. Edge table: first two columns are source/destination IDs

Edge tables need `(from="<node_table>", to="<node_table>")` to specify which node tables the edge connects. The first two columns of the edge file are treated as source and destination vertex IDs (referencing the primary keys of the connected node tables).

---

## Full Runnable Script

```python
from neug import Database
import tempfile
import os

db = Database(db_path=tempfile.mkdtemp(), mode="w")
conn = db.connect()

# Extensions
conn.execute("install httpfs")
conn.execute("install parquet")
conn.execute("load httpfs")
conn.execute("load parquet")

# Preview
result = conn.execute('''
    LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
    RETURN ID, fName, age, isStudent LIMIT 3
''')
print("Preview:", list(result))

# Import nodes (no DDL)
conn.execute('''
    COPY Person FROM (
        LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/vPerson.parquet"
        RETURN *
    )
''')

# Import edges (no DDL)
conn.execute('''
    COPY MEETS FROM (
        LOAD FROM "https://graphscope.oss-cn-beijing.aliyuncs.com/neug/eMeets.parquet"
        RETURN *
    ) (from="Person", to="Person")
''')

# Query
result = conn.execute('''
    MATCH (a:Person)-[m:MEETS]->(b:Person)
    WHERE a.age > 30
    RETURN a.fName, b.fName, m.location
''')
print("Query results:", list(result))

# Export to Parquet
out = os.path.join(tempfile.mkdtemp(), "young_social.parquet")
conn.execute(f'''
    COPY (
        MATCH (a:Person)-[m:MEETS]->(b:Person)
        WHERE a.age < 35
        RETURN a.fName AS name, a.age AS age, b.fName AS met_person, m.location
    ) TO '{out}'
''')
print(f"Exported to: {out} ({os.path.getsize(out)} bytes)")

# to_arrow()
result = conn.execute("MATCH (p:Person) RETURN p.ID, p.fName, p.age ORDER BY p.ID")
table = result.to_arrow()
print(f"Arrow table: {table.num_rows} rows, columns: {table.column_names}")

conn.close()
db.close()
```
