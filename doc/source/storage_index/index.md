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

Use the read-only `show_indexes` procedure to inspect index metadata, activation
state, and buffered data counts across all index types:

```cypher
// List all indexes.
CALL show_indexes() RETURN *;

// List all HNSW indexes.
CALL show_indexes('hnsw') RETURN *;

// Select an index by name.
CALL show_indexes(name := 'entity_embedding_hnsw') RETURN *;

// Select by both type and name.
CALL show_indexes(type := 'hnsw', name := 'entity_embedding_hnsw') RETURN *;
```

Both arguments are optional strings:

| Argument | Position | Meaning |
| --- | --- | --- |
| `type` | 1 | Index type, such as `'hnsw'` or `'fts'`; case-insensitive. |
| `name` | 2 | Exact index name. |

Named arguments use `:=`, not `=`. Arguments also support query parameters,
for example `CALL show_indexes($type, $name) RETURN *;`. Omitting both arguments
lists all indexes. Unmatched filters return no rows, and results are sorted by
index name.

Example output:

| name | type | label | property | options | state | buffered_count |
| --- | --- | --- | --- | --- | --- | ---: |
| entity_embedding_hnsw | hnsw | Entity | embedding | `{"metric":"cosine"}` | active | 6000 |
| entity_text_fts | fts | Entity | text | `{}` | active | 0 |

| Column | Meaning |
| --- | --- |
| `name` | Index name. |
| `type` | Index type. |
| `label` | Node table containing the indexed property. |
| `property` | Indexed property name; multiple properties are represented as a string such as `["title","body"]`. |
| `options` | Index options as a JSON string, with option values represented as strings. |
| `state` | `active` when the index implementation is loaded; `pending` when the index has not been activated because its provider extension is not loaded. |
| `buffered_count` | Number of data slots buffered in the index's LocalBuffer. The exact counting rules depend on the index implementation. Returns 0 for active indexes without a LocalBuffer and `NULL` for pending indexes. |

To inspect only the buffer size:

```cypher
CALL show_indexes() RETURN name, type, buffered_count;
```

## Optimize Indexes

Use `optimize_indexes` to explicitly perform maintenance on selected indexes.
Each index type implements its own maintenance logic, such as merging buffered
data into its main index structure. The procedure is available to all index
types.

For example, HNSW merges LocalBuffer data into its graph. That data is already
searchable before optimization, so applications do not need to change their
queries.

The procedure accepts the same optional string arguments, `type` and `name`,
as `show_indexes`, with the same positional order and support for named and
query parameters:

```cypher
// Optimize one HNSW index.
CALL optimize_indexes(type := 'hnsw', name := 'entity_embedding_hnsw');

// Equivalent positional arguments.
CALL optimize_indexes('hnsw', 'entity_embedding_hnsw');

// Select by name without specifying a type.
CALL optimize_indexes(name := 'entity_embedding_hnsw');

// Optimize all HNSW indexes.
CALL optimize_indexes('hnsw');
```

Omitting both arguments selects all indexes. Each index uses its own maintenance
implementation; an index with no maintenance work returns zero counts. A type with
no matching indexes returns no rows. An explicitly specified unknown name or
a name/type mismatch is an error. If any selected index is pending, the call
fails before optimizing any index. Load its provider extension first, or select
only active indexes.

The call waits for maintenance to finish and returns one row per selected index,
sorted by index name. Example output (counts and duration are illustrative):

| type | index_name | indexed_count | skipped_count | duration_ms |
| --- | --- | ---: | ---: | ---: |
| hnsw | entity_embedding_hnsw | 5800 | 200 | 1250.0 |

| Column | Meaning |
| --- | --- |
| `type` | Index type. |
| `index_name` | Index name. |
| `indexed_count` | Number of entries incorporated into the index by this call, as reported by its maintenance implementation. |
| `skipped_count` | Number of entries skipped during maintenance. Reasons depend on the index implementation; HNSW skips deleted or replaced entries, allocation gaps, and entries already added during an earlier attempt. |
| `duration_ms` | Time spent optimizing this index, in milliseconds. |

Optimization uses a write transaction. All selected indexes publish their
maintenance changes together; a failure aborts the operation rather than
returning partial success. Inside an explicit write transaction, changes become
committed when that transaction commits. Read-only transactions can inspect
indexes but cannot optimize them.

See [Local Buffer guidance](../extensions/vector_search.md#when-and-how-to-optimize)
for advice on when to optimize HNSW indexes.

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
