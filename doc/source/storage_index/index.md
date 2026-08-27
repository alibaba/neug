# Storage Indexes

Storage indexes accelerate queries while remaining integrated with NeuG's
graph storage and transaction model. Index types are provided by the core
engine or by extensions, so this section describes the lifecycle and behavior
shared by every storage index.

## Supported Index Types

NeuG currently supports:

| Index type | Use case | Documentation |
| ---------- | -------- | ------------- |
| `HNSW` | Approximate nearest-neighbor search over vector properties | [Vector Search](../extensions/vector_search.md) |
| `FTS` | BM25-ranked full-text search over string properties | [Full-Text Search](../extensions/fts_search.md) |

NeuG plans to support the following index type and capabilities in the future:

- `BTree` indexes for equality, range, and ordered queries over scalar properties.
- Composite indexes that index multiple properties together.
- Result reranking for reordering index candidates using additional scoring criteria.

The common index syntax and guarantees below also apply to index types added in
the future. See an index type's documentation for supported properties,
options, and query expressions.

## Create an Index

Use `CREATE INDEX` to build an index on a node property:

```cypher
CREATE INDEX <index_name> [IF NOT EXISTS]
ON <node_table>
USING <index_type> (<property>)
[WITH (
    <option> = <value>
    [, ...]
)];
```

- `index_name` uniquely identifies the index within the database.
- `IF NOT EXISTS` makes the statement succeed when that name already exists.
- `node_table` and `property` identify the indexed property.
- `index_type` selects the index implementation.
- `WITH` passes implementation-specific construction options.

Existing values are indexed when the index is created. Later inserts, updates,
and deletes maintain the index together with the underlying graph data.

## Drop an Index

Remove an index by its unique name:

```cypher
DROP INDEX <index_name> [IF EXISTS];
```

`IF EXISTS` makes the statement succeed when the named index does not exist.
An index is also removed when its indexed property or owning node table is
dropped.

## Inspect Indexes

Use `SHOW_INDEXES()` to inspect indexes in the current database:

```cypher
CALL SHOW_INDEXES() RETURN *;
```

The result includes the index name, type, node label, property, options, and
state. An index can have either of these states:

- `active`: its implementation is loaded and the index is available for query
  planning and online maintenance.
- `pending`: its metadata was restored, but the extension that implements the
  index is not loaded. The index becomes active after the provider extension is
  loaded.

A pending index is not a background index build. It is a persisted index
waiting for its implementation to become available.

## Index-Accelerated Queries

Index use is integrated into normal Cypher queries. Write the query using
`MATCH`, filtering or ordering expressions, and `LIMIT` as appropriate for the
index type. When an eligible active index exists, the optimizer rewrites the
matching scan or top-K operation into an index scan. The query does not need a
separate index-search procedure.

Eligibility is index-specific. HNSW recognizes supported vector distance
expressions in `ORDER BY`; see [Vector Similarity
Search](../extensions/vector_search.md#vector-similarity-search). FTS recognizes `bm25`
expressions; see [Full-Text Search](../extensions/fts_search.md#full-text-search).

## Transactions

Index lifecycle operations and index data changes participate in the same
transaction as graph changes:

- `CREATE INDEX` and `DROP INDEX` become visible atomically at commit.
- Inserts, updates, and deletes change graph properties and corresponding index
  entries atomically.
- Aborted transactions publish neither the graph changes nor their index
  changes.

Readers therefore cannot observe committed graph data with stale index data,
or an index lifecycle change without its associated transaction. For the
general ACID and isolation model, see [Transaction
Management](../transaction/transaction.mdx).

## Persistence and Recovery

Index metadata and data are stored with the database checkpoint. Committed
index lifecycle and maintenance operations are also recorded in the
write-ahead log (WAL). When a database is reopened, NeuG restores the checkpoint
and replays later WAL records, so committed indexes recover with the graph.

For an extension-backed index, recovery can initially expose the index as
`pending` because extensions and functions have the same lifetime as their
database. Load the provider extension in the reopened database to activate the
restored index; see [Extensions](../extensions/index.md).
