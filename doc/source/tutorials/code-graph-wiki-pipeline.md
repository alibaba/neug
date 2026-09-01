# Building a Code Wiki with Graph

This tutorial shows how to use graph algorithms to **structure and maintain a code wiki**
with NeuG (version >= v0.2.0) and its GDS (Graph Data Science) extension:

1. **Build** — import a file-level relationship graph, use Leiden to detect wiki section
   boundaries, and use PageRank to pick the core files of each section.
2. **Update** — add new files incrementally, re-cluster with freeze-assign Leiden
   (existing sections stay stable), and compute a section-level delta with Cypher.

The running example is a file-level relationship graph extracted from the NeuG codebase:
each node is a source file, and each edge means "file A is related to file B". These
relationships are derived from the underlying code graph — for example, if a function in
file A calls a function in file B, then A and B are connected by an edge.

**Time**: ~15 minutes
**Prerequisites**: `pip install neug>=0.2.0`

---

## The Data

The tutorial uses two CSV files:

| File                       | Columns                        | Description                                                                  |
| -------------------------- | ------------------------------ | ---------------------------------------------------------------------------- |
| `file_cluster_nodes.csv` | `id, label`                  | One row per source file.`id` is the file path, `label` is the file name. |
| `file_cluster_edges.csv` | `from_file, to_file, weight` | One row per relationship. `from_file` is related to `to_file` (e.g., a function in A calls a function in B). |

The files are hosted on Alibaba Cloud OSS and loaded straight into NeuG via the `httpfs`
extension — no manual download needed:

```python
OSS_NODES = "oss://neug/tutorial/neug-wiki-files/file_cluster_nodes.csv"
OSS_EDGES = "oss://neug/tutorial/neug-wiki-files/file_cluster_edges.csv"
```

> **Note**: The dataset has **1,332 files** and **2,982 edges**. Make sure you
> have loaded `httpfs` (`install httpfs` then `load httpfs`) before running the import
> queries below.

---

## Step 1: Set Up the Database and Load GDS

NeuG ships graph algorithms (Leiden, PageRank, BFS, SSSP, and more) in the `gds`
extension. Load it first:

```python
from neug import Database
import tempfile

db = Database(db_path=tempfile.mkdtemp(), mode="w")
conn = db.connect()

conn.execute("install httpfs")  # one-time
conn.execute("install gds")  # one-time
conn.execute("load httpfs")     # activate for this session
conn.execute("load gds")
```

> **Note**: `load gds` activates the extension for the current session. If it is not
> installed yet, run `install gds` once first to download it.

---

## Step 2: Import the File-Level Graph

`COPY TEMP` auto-infers the schema from the CSV — no `CREATE TABLE` DDL needed. The first
column becomes the node's primary key; for edges, the first two columns are source and
destination, with `from` / `to` specifying the endpoint labels.

```python
# Nodes: first column (id) is the primary key, schema auto-inferred
conn.execute(f"""
    COPY TEMP file FROM "{OSS_NODES}" (
        CREDENTIALS_KIND='Anonymous', ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
        header=true, delimiter=','
    )
""")

# Edges: first two columns are src/dst keys
conn.execute(f"""
    COPY TEMP depends FROM "{OSS_EDGES}" (
        CREDENTIALS_KIND='Anonymous', ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
        header=true, delimiter=',',
        from='file', to='file'
    )
""")
```

Verify the import:

```python
print(list(conn.execute("MATCH (f:file) RETURN count(f)")))        # [[1332]]
print(list(conn.execute("MATCH ()-[d:depends]->() RETURN count(d)")))  # [[2982]]
```

---

## Step 3: Project the Graph for GDS

GDS algorithms run on a **projected graph** — an in-memory view optimized for computation.
Project the `file` nodes and their `depends` edges:

```python
conn.execute("""
    CALL project_graph(
        'code_graph',
        ['file'],
        {'[file, depends, file]': ''}
    )
""")
```

The third argument maps an edge triplet `[from_label, rel_type, to_label]` to an optional
predicate (row filter). An empty string means "no filter" — include all edges of that type.

---

## Step 4: Detect Concept Boundaries with Leiden

Closely related files naturally cluster into the same community — independent of the
directory layout. Run Leiden community detection:

```python
result = conn.execute("""
    CALL leiden('code_graph', {concurrency: 1})
    YIELD node, community
    RETURN node.label, community
    ORDER BY community
""")
for row in result:
    print(row)
```

Expected output (truncated):

```
['benchmark.cc', 5]
['main.cc', 5]
...
['binder.h', 19]
['logical_plan.h', 19]
...
```

Count the communities:

```python
n_comm = list(conn.execute("""
    CALL leiden('code_graph', {concurrency: 1})
    YIELD node, community
    RETURN count(DISTINCT community)
"""))[0][0]
print(n_comm)  # 30
```

> **Note on reproducibility**: set `concurrency: 1` to get a **deterministic** partition.
> With multiple threads the community count can vary slightly between runs. Deterministic
> output is exactly what you want when the graph (not an LLM) is the source of truth for
> "which concept does this file belong to".

### Write communities back to the nodes

To filter by community later (and to seed the incremental update), store the community ID
on each node. First add a column, then write back:

```python
conn.execute("ALTER TABLE file ADD community INT64")

conn.execute("""
    CALL leiden('code_graph', {concurrency: 1})
    YIELD node, community
    MATCH (f:file) WHERE f.id = node.id
    SET f.community = community
""")
```

> **Note**: NeuG does not support the `ADD COLUMN` keyword — use `ALTER TABLE file ADD community INT64`.

---

## Step 5: Pick Core Files with PageRank

Each community is a candidate "concept" (a wiki section). But which files should you read
to write that section? PageRank ranks files by global reference importance, so the most
referenced files rise to the top.

Global top-5 most important files:

```python
result = conn.execute("""
    CALL page_rank('code_graph', {max_iterations: 20})
    YIELD node, rank
    RETURN node.label, rank
    ORDER BY rank DESC
    LIMIT 5
""")
for label, rank in result:
    print(f"{label:30s} {rank:.6f}")
```

Expected output:

```
binder.h                       0.015460
operator.h                     0.015152
database.py                    0.011829
string_format.h                0.010160
types.h                        0.009774
```

Top-5 within a single community (here, community `0`, the largest one with 191 files):

```python
result = conn.execute("""
    CALL page_rank('code_graph', {max_iterations: 20})
    YIELD node, rank
    WITH node, rank WHERE node.community = 0
    RETURN node.label, rank
    ORDER BY rank DESC
    LIMIT 5
""")
for label, rank in result:
    print(f"{label:30s} {rank:.6f}")
```

Expected output:

```
string_format.h                0.010160
types.h                        0.009774
value_vector.h                 0.006664
timestamp_t.h                  0.006314
types.h                        0.003926
```

> **Note**: you cannot put a `WHERE` directly after `YIELD`. Bridge with `WITH node, rank WHERE ...` before `RETURN`.

At this point the **Build** phase is done: you have stable wiki section boundaries (Leiden)
and a ranked reading list for each section (PageRank). Feed these two pieces to an LLM as a structured starting point — it decides how to organize wiki sections based on the communities, and reads the PageRank-ranked core files
instead of guessing which sources matter.

---

## Step 6: Update Incrementally with Freeze-Assign Leiden

Code changes every day, so a wiki cannot be written once and forgotten. The challenge is
to update wiki sections **without drifting** — a section called "compiler" before the
update should still be called "compiler" afterwards.

NeuG's **freeze-assign Leiden** does exactly this: existing vertices keep their community
assignments frozen, and only newly added vertices get clustered.

### 6.1 Append the new files

Suppose three new pattern matching algorithm files are added to the GDS extension, each related to two
existing files. Stage the new nodes and edges as small CSVs and append them to the existing
`file` / `depends` tables with `COPY`:

```python
new_files = [
    "extension/gds/include/impl/pattern_match_impl.h",
    "extension/gds/include/impl/subgraph_match_impl.h",
    "extension/gds/include/impl/motif_count_impl.h",
]
targets = [
    "extension/gds/include/utils/subgraph_utils.h",
    "extension/gds/include/gds_algo_function_collection.h",
]

# Append new file nodes. community = -1 is a sentinel meaning "no previous community",
# so freeze-assign treats them as brand-new vertices.
# Note: the CSV must carry all current columns of the `file` table (including `community`).
nodes_csv = os.path.join(tempfile.mkdtemp(), "new_nodes.csv")
with open(nodes_csv, "w") as fp:
    fp.write("id,label,community\n")
    for f in new_files:
        fp.write(f"{f},{os.path.basename(f)},-1\n")

conn.execute(f"""
    COPY file FROM "{nodes_csv}" (
        header=true, delimiter=','
    )
""")

# Append edges: each new file is related to existing files. Append them to the
# existing `depends` edge table with COPY.
edges_csv = os.path.join(tempfile.mkdtemp(), "new_edges.csv")
with open(edges_csv, "w") as fp:
    fp.write("from_file,to_file,weight\n")
    for f in new_files:
        for t in targets:
            fp.write(f"{f},{t},1\n")

conn.execute(f"""
    COPY depends FROM "{edges_csv}" (
        header=true, delimiter=',',
        from='file', to='file'
    )
""")

print(list(conn.execute("MATCH (f:file) RETURN count(f)")))  # [[1335]]
```

> **Note**: `community = -1` is a sentinel — any real community ID is `>= 0`, so `-1`
> unambiguously marks "not yet assigned".

### 6.2 Re-project and re-cluster

Drop the stale projection, re-project to include the new nodes and edges, then run
freeze-assign Leiden by passing `initial_community_property`:

```python
conn.execute("CALL drop_projected_graph('code_graph')")
conn.execute("CALL project_graph('code_graph', ['file'], {'[file, depends, file]': ''})")

result = conn.execute("""
    CALL leiden('code_graph', {concurrency: 1, initial_community_property: 'community'})
    YIELD node, community, previous_community
    RETURN node.id, community, previous_community
""")
for nid, community, prev in result:
    if prev is None:                      # newly assigned vertex
        print(f"NEW: {os.path.basename(nid)} -> community {community}")
```

Expected output:

```
NEW: pattern_match_impl.h -> community 3
NEW: subgraph_match_impl.h -> community 3
NEW: motif_count_impl.h -> community 3
```

`previous_community` is `NULL` for new vertices and equals `community` for frozen existing
vertices. The three new files land in community `3` — the same concept as the files they
are related to — while every existing vertex keeps its original community.

### 6.3 Compute the community-level delta

Only communities that actually changed need to be regenerated downstream. Group by
community and classify each one as `stable`, `growth`, or `new`:

```python
result = conn.execute("""
    CALL leiden('code_graph', {concurrency: 1, initial_community_property: 'community'})
    YIELD node, community, previous_community
    WITH community, count(*) AS total, count(previous_community) AS old_members
    RETURN community, total - old_members AS new_members,
      CASE WHEN old_members = 0      THEN 'new'
           WHEN old_members = total  THEN 'stable'
           ELSE 'growth' END AS change_type
""")
for community, new_members, change_type in result:
    if change_type != 'stable':
        print(f"community {community}: {change_type} (+{new_members})")
```

Expected output:

```
community 3: growth (+3)
```

Out of 30 communities, 29 are `stable` (reuse their existing wiki section as-is) and only
community `3` grew — so only that one section needs an incremental rewrite. This is the
structural answer to "which parts of the knowledge base are affected by this change?".

Pass this delta result to the LLM and it only needs to rewrite the affected sections —
the other 29 stay untouched.

---

## Full Runnable Script

```python
import os
import tempfile
from neug import Database

OSS_NODES = "oss://neug/toturial/neug-wiki-files/file_cluster_nodes.csv"
OSS_EDGES = "oss://neug/toturial/neug-wiki-files/file_cluster_edges.csv"

db = Database(db_path=tempfile.mkdtemp(), mode="w")
conn = db.connect()
conn.execute("install httpfs")
conn.execute("load httpfs")
conn.execute("install gds")  # one-time
conn.execute("load gds")

# ---- Build ----
conn.execute(f"""
    COPY TEMP file FROM "{OSS_NODES}" (
        CREDENTIALS_KIND='Anonymous', ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
        header=true, delimiter=','
    )
""")
conn.execute(f"""
    COPY TEMP depends FROM "{OSS_EDGES}" (
        CREDENTIALS_KIND='Anonymous', ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
        header=true, delimiter=',',
        from='file', to='file'
    )
""")
conn.execute("ALTER TABLE file ADD community INT64")

conn.execute("CALL project_graph('code_graph', ['file'], {'[file, depends, file]': ''})")
conn.execute("""
    CALL leiden('code_graph', {concurrency: 1})
    YIELD node, community
    MATCH (f:file) WHERE f.id = node.id
    SET f.community = community
""")
print("communities:", list(conn.execute(
    "MATCH (f:file) RETURN count(DISTINCT f.community)"))[0][0])  # 30

print("global top-5:", list(conn.execute("""
    CALL page_rank('code_graph', {max_iterations: 20})
    YIELD node, rank RETURN node.label, rank
    ORDER BY rank DESC LIMIT 5
""")))

# ---- Update ----
new_files = ["extension/gds/include/impl/pattern_match_impl.h",
           "extension/gds/include/impl/subgraph_match_impl.h",
           "extension/gds/include/impl/motif_count_impl.h"]
targets = ["extension/gds/include/utils/subgraph_utils.h",
           "extension/gds/include/gds_algo_function_collection.h"]

nodes_csv = os.path.join(tempfile.mkdtemp(), "new_nodes.csv")
with open(nodes_csv, "w") as fp:
    fp.write("id,label,community\n")
    for f in new_files:
        fp.write(f"{f},{os.path.basename(f)},-1\n")
conn.execute(f"""
    COPY file FROM "{nodes_csv}" (
        header=true, delimiter=','
    )
""")

edges_csv = os.path.join(tempfile.mkdtemp(), "new_edges.csv")
with open(edges_csv, "w") as fp:
    fp.write("from_file,to_file,weight\n")
    for f in new_files:
        for t in targets:
            fp.write(f"{f},{t},1\n")
conn.execute(f"""
    COPY depends FROM "{edges_csv}" (
        header=true, delimiter=',',
        from='file', to='file'
    )
""")

conn.execute("CALL drop_projected_graph('code_graph')")
conn.execute("CALL project_graph('code_graph', ['file'], {'[file, depends, file]': ''})")
print("delta:", list(conn.execute("""
    CALL leiden('code_graph', {concurrency: 1, initial_community_property: 'community'})
    YIELD node, community, previous_community
    WITH community, count(*) AS total, count(previous_community) AS old_members
    RETURN community, total - old_members AS new_members,
      CASE WHEN old_members = 0 THEN 'new'
           WHEN old_members = total THEN 'stable'
           ELSE 'growth' END AS change_type
""")))

conn.execute("CALL drop_projected_graph('code_graph')")
conn.close()
db.close()
```
