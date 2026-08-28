# Vector Search Extension

Since NeuG v0.2.0, NeuG provides vector search capabilities through the dedicated `vector_search` extension.

For syntax and guarantees shared by all index types, including inspection,
transactions, and recovery, see [Storage Indexes](../storage_index/index.md).

The vector search extension enables efficient similarity search over graph data by combining vector storage, distance computation, and HNSW-based approximate nearest neighbor (ANN) indexing.

.. warning::

   **COSINE HNSW INDEX CREATION OVERWRITES VECTOR DATA BY DEFAULT**

   Creating a cosine HNSW index uses ``cosine_normalize = true`` by default.
   NeuG may permanently replace every existing value in the indexed property
   with its L2-normalized value. The original vectors and their magnitudes are
   lost and cannot be recovered, including after ``DROP INDEX``.

   To preserve the existing property values, explicitly set
   ``cosine_normalize = false``. In that mode, you are responsible for ensuring
   that every existing and future vector is a valid L2-normalized vector.

   ``cosine_normalize`` applies only to cosine HNSW indexes. It has no effect
   on IP or L2 HNSW indexes, which preserve raw vectors. NeuG does not support
   or recommend using this option to normalize IP/L2 data because doing so
   would discard magnitude information and change distance semantics.

The major features include:

- **Optional Vector Search Extension**
  - Vector search is provided through the dedicated `vector_search` extension.
  - Users can install and enable this extension according to their requirements.

- **Native Vector Data Type Support**
  - Vectors are stored using fixed-length `ARRAY` columns.
  - Currently supports dense FP32 vectors.

- **Multiple Distance Metrics**
  - L2 distance returns the squared Euclidean distance used by zvec; a smaller
    value means the vectors are more similar.
  - Cosine distance measures the difference in direction between two vectors;
    a smaller value means their directions are more similar.
  - Inner product measures vector alignment and magnitude; a larger value
    means the vectors are more similar.


- **HNSW-based Approximate Nearest Neighbor Search**
  - Supports efficient top-K approximate nearest neighbor search through `HNSWIndex`.
  - Provides high-performance vector similarity retrieval.

- **Online Index Maintenance**
  - `HNSWIndex` supports both:
    - Bulk index construction
    - Incremental index updates
  - When data is imported or updated, the corresponding HNSW index is automatically updated.

---

## Vector Properties

NeuG uses fixed-length `ARRAY` columns to represent vector properties.

Currently, NeuG supports:

- Dense vectors only
- FP32 (`FLOAT`) element type only

Sparse vectors are not supported currently.

For example, the following vector:

`[0.1, 0, 0, 0.4]` is stored as a dense vector: `[0.1, 0, 0, 0.4]`; The sparse representation: `{0: 0.1, 3: 0.4}` is not supported.


### Create Vector Property

You can create a vector property by defining a fixed-length array property in
the node type schema.

The following example creates a node type with:

- A primary key column `id`
- A FP32 vector column `vec`
- Vector dimension = 4

```cypher
// Fixed-length FLOAT array with dimension=4 as vector column
// Primary key and vector dimension are declared together in DDL
CREATE NODE TABLE vector_node (
    id INT64,
    vec FLOAT[4],
    PRIMARY KEY (id)
);
```

If no default value is specified, a vector property is implicitly initialized
to `[0.0, 0.0, 0.0, ...]`, with one FP32 zero for each vector dimension. This
may cause nodes containing the default vector to appear in vector similarity
query results.

### Drop Vector Property

Dropping a node type will automatically remove:

- Vector properties defined on this node type
- Associated HNSW indexes

Example:

```cypher
// Cascade deletion:
// Remove node type properties and dependent HNSW indexes
DROP TABLE vector_node;
```

### Alter Vector Property

Similar to other properties, vector properties can be added to a node type
through `ALTER TABLE`.

Example:

```cypher
// Add vector column
ALTER TABLE vector_node
ADD IF NOT EXISTS vec2 FLOAT[4];
```

---

## Loading Vector Data

NeuG supports importing vector properties from: CSV, JSON, Parquet. All supported formats use the unified `COPY FROM` syntax.

### CSV Import

Example input file `vec.csv`:

```csv
id|vec
1|[0.1, 0.2, 0.3, 0.4]
2|[0.2, 0.1, 0.1, 0.1]
...
```

Create `vector_node` schema:

```cypher
CREATE NODE TABLE vector_node (
    id INT64,
    vec FLOAT[4],
    PRIMARY KEY (id)
);
```

Import CSV data:

```cypher
// Convert CSV string representation into FLOAT array explicitly
COPY vector_node FROM (
    LOAD FROM 'vec.csv'
    RETURN id,
           CAST(vec, 'FLOAT[4]') AS vec
);
```

### JSON Import

Example JSON file `vec.json`:

```json
[
  {
    "id": 1,
    "vec": [0.1, 0.2, 0.3, 0.4]
  },
  {
    "id": 2,
    "vec": [0.2, 0.1, 0.1, 0.1]
  }
]
```

Import:

```cypher
COPY vector_node FROM (
    LOAD FROM 'vec.json'
    RETURN id,
           CAST(vec, 'FLOAT[4]') AS vec
);
```

### Parquet Import

Vector data can also be imported from Parquet files.

The `vec` data in the Parquet file is stored as a `FLOAT32[4]` array.

Example:

```cypher
COPY vector_node FROM (
    LOAD FROM 'vec.parquet'
    RETURN id,
           CAST(vec, 'FLOAT[4]') AS vec
);
```

## Create HNSW Index

NeuG supports approximate nearest neighbor search through HNSW indexes. The
following example specializes the common [CREATE INDEX](../storage_index/index.md#create-an-index)
syntax for a vector property.

Example:

```cypher
// Create HNSW index on vector property
CREATE INDEX vec_hnsw_index IF NOT EXISTS
ON vector_node
USING HNSW (vec)
WITH (
    metric = 'cosine',
    cosine_normalize = true,
    m = 16,
    ef_construction = 200
);
```

In this statement:

- `vec_hnsw_index` is the index name. It must be unique within the database and
  is used to identify the index when dropping it.
- `vector_node` is the node type that contains the vector property.
- `HNSW` selects the Hierarchical Navigable Small World index type.
- `vec` is the vector property to index.

The following parameters control HNSW index construction and search behavior:

| Parameter         | Description                                                           | Default |
| ----------------- | --------------------------------------------------------------------- | ------- |
| `metric`          | Distance metric. Supported values are `l2` (or `l2sq`), `cosine`, and `ip` (or `inner_product`) | `l2` |
| `cosine_normalize` | Whether a **cosine** HNSW index may permanently L2-normalize the indexed property values | `true` |
| `m`               | Maximum number of connections created for each node in the HNSW graph | `50` |
| `ef_construction` | Size of the candidate list used while building the index. Larger values generally improve index quality at the cost of build time and memory | `500` |

When creating an index:

- Existing nodes are automatically indexed.
- Nodes inserted after index creation are automatically added to the HNSW
  index.

### Vector normalization

HNSW cosine search requires stored vectors to have unit L2 norm.
`cosine_normalize` defaults to `true` for cosine HNSW indexes, allowing NeuG to
normalize the property column when sampled values are not already normalized:

```cypher
CREATE INDEX vec_cosine_hnsw
ON vector_node
USING HNSW (vec)
WITH (metric = 'cosine', cosine_normalize = true);
```

To prevent property modification, explicitly set `cosine_normalize = false`.
NeuG then performs a deterministic sample check and rejects creation when a
sampled vector is not normalized. A successful sample check is not a
full-column guarantee: users choosing `cosine_normalize = false` are
responsible for ensuring that every existing and future vector has unit L2
norm.

When `cosine_normalize = true`, NeuG first samples the current column. If all sampled
vectors have unit norm, it skips the conversion. Otherwise it creates a new
buffer and normalizes the vectors in one pass. An all-zero vector remains
all-zero. Encountering NULL, a non-zero near-zero vector, NaN, infinity, or
another value that cannot be normalized
causes index creation to fail without publishing the partially converted
buffer.

.. warning::

   Sampling can miss a non-normalized vector. This applies even when
   ``cosine_normalize = true`` because NeuG skips conversion after a successful sample
   check. For strict correctness, normalize and validate the complete dataset
   before importing it.

.. warning::

   Normalization permanently overwrites the stored property values. The
   original vector magnitudes cannot be recovered. Property reads, exports,
   checkpoints, and values observed after ``DROP INDEX`` contain normalized
   vectors. Back up the original embeddings or store normalized vectors in a
   separate property before creating the index.

After a column adopts the normalized representation, subsequent INSERT and SET
operations are normalized automatically. Explicit NULL vector values and NULL
vector elements are unsupported. All-zero vectors are preserved as all-zero;
``vector_distance_cosine`` returns ``1`` when either argument is all-zero.
Non-zero near-zero vectors and non-finite values are rejected.
Omitting the property is accepted when its schema default materializes as a
valid non-NULL vector, including the implicit all-zero default.

The option has no effect on IP or L2 indexes. Those indexes do not trigger
normalization and preserve the current property representation. Normalizing IP
would remove magnitude information and change inner-product scores;
normalizing L2 would change distances and nearest-neighbor ordering. Therefore
NeuG neither supports nor recommends using `cosine_normalize` to normalize
IP/L2 data.

NeuG rejects attempts to normalize a property already used by an HNSW index
built from raw vectors. Avoid mixing indexes that require raw and normalized
representations on the same property. Dropping the last index does not restore
the original vectors or disable the normalized write constraint.

Therefore, users can choose either workflow:

- Workflow 1: Import Data First, Build Index Later
- Workflow 2: Create Index First, Continuously Write Data

### Duplicate Vectors

A large number of duplicate vectors can degrade HNSW index construction and
query performance. When `CREATE INDEX` bulk-builds an HNSW index, NeuG estimates
the duplicate rate from vector hashes and writes a warning containing approximate
duplicate statistics, for example:

```text
HNSW duplicate statistics for index 'vec_hnsw_index': 83 / 100 (83%) duplicate vectors
```

This check is intended as a lightweight diagnostic. Hash collisions are
possible, so the result is approximate. NeuG does not reject or automatically
remove duplicate vectors during index creation. The statistics are computed
only for existing vectors during the initial ``CREATE INDEX`` bulk build;
vectors added or changed later through ``CREATE`` or ``SET`` are not included,
and the statistics are not updated continuously.


To remove or inspect the index, use the common [DROP
INDEX](../storage_index/index.md#drop-an-index) and [SHOW_INDEXES](../storage_index/index.md#inspect-indexes)
operations. Dropping the vector property or its node table also removes its
HNSW index.

---

## Vector Query

NeuG provides vector query capabilities through:

- Vector distance functions
- HNSW-based similarity search
- Graph and vector hybrid retrieval
- Index-filtering vector search
- Graph filtering during vector search

### Vector Functions

NeuG provides three vector distance functions for similarity computation.

Supported functions include:

| Function | Description | Example |
|----------|-------------|---------|
| `vector_distance_l2(a, b)` | Squared L2 distance: the sum of squared differences. Smaller values indicate greater similarity. | `vector_distance_l2(n.vec, [0.1, 0.2, 0.3, 0.4])` |
| `vector_distance_cosine(a, b)` | Cosine distance: one minus cosine similarity. Smaller values indicate more similar directions. | `vector_distance_cosine(n.vec, [0.1, 0.2, 0.3, 0.4])` |
| `vector_distance_ip(a, b)` | Inner product: the sum of element-wise products. Larger values indicate greater similarity. | `vector_distance_ip(n.vec, [0.1, 0.2, 0.3, 0.4])` |


Vector distance functions can be directly used in Cypher expressions.

The following example calculates the L2 distance between every node vector and a query vector.

```cypher
// Full table scan distance calculation.
// Does not use HNSW.
// Suitable for validation or very small datasets.
MATCH (n:vector_node)
RETURN vector_distance_l2(
    n.vec,
    [0.1, 0.2, 0.3, 0.4]
) AS d;
```

>Note: When calculating the distance between a vector property and a constant vector, the constant is automatically cast to the property's numeric array type. For example, if the property is FLOAT[N], a DOUBLE[N] constant is converted to FLOAT[N], which may result in precision loss. To avoid unintended precision loss, use arguments with the same numeric array type whenever possible.


This query performs a brute-force distance calculation:

- Scan all vector nodes.
- Calculate vector distance row by row.
- Return calculated distances.

For large datasets, users should create an HNSW index and use vector similarity search.


### Vector Similarity Search

NeuG provides index-based vector similarity search using an embedded query syntax similar to PostgreSQL and DuckDB.

When an HNSW index exists, the NeuG optimizer automatically recognizes eligible vector queries and rewrites them into efficient HNSW index scans.

Instead of:

- Calculating distance for every node.
- Sorting all nodes by distance.
- Returning top-K results.

NeuG directly uses the HNSW index to retrieve approximate nearest neighbors.

Example:
```cypher
// If an HNSW index exists,
// the optimizer can rewrite this query into HNSWIndexScan.
MATCH (n:vector_node)
ORDER BY vector_distance_l2(
    n.vec,
    [0.1, 0.2, 0.3, 0.4]
)
LIMIT 3;
```

> Note: Vector properties that were not explicitly assigned use an implicit
> all-zero vector and may therefore appear in similarity search results. See
> [Create Vector Property](#create-vector-property) for details.

### Graph + Vector Hybrid Search

NeuG combines graph traversal capabilities with vector similarity search.

Users can first perform vector retrieval and then continue graph traversal operations.

Example:
```cypher
MATCH (n:vector_node)
WITH n
ORDER BY vector_distance_l2(
    n.vec,
    [0.1, 0.2, 0.3, 0.4]
)
LIMIT 3

MATCH (n)-[e:links]->(n2)

RETURN n, n2;
```

### Index-Filtering Vector Search

NeuG supports Index-Filtering during vector search.

Index-Filtering means:

- Filtering conditions are pushed into the vector index search process.
- The system continues searching until enough valid top-K results are found.

With post-filtering, the index first retrieves K nearest neighbors and the
predicate is applied only afterward. If some candidates are filtered out, the
query can return fewer than K results even when additional matching nodes
exist. Index-filtering evaluates the predicate during the HNSW search and
continues searching for eligible candidates, providing better recall and a
strict top-K result guarantee when at least K matching nodes exist.

Example:
```cypher
MATCH (n:vector_node)
WHERE n.id <> 1
RETURN n
ORDER BY vector_distance_l2(
    n.vec,
    [0.1, 0.2, 0.3, 0.4]
)
LIMIT 3;
```

The filtering condition:

`WHERE n.id <> 1`

is evaluated during HNSW search instead of after retrieving results.

### Graph Filtering During Vector Search

NeuG supports graph-based filtering during vector retrieval. The graph pattern
is evaluated first, and the matching vertices are used as a filter during the
HNSW search.

For example, the following query retrieves the most similar outgoing neighbors
of a specific node:

```cypher
MATCH (n1:vector_node {id: 1})
      -[:links]->
      (n2:vector_node)
RETURN n2,
       vector_distance_l2(
           n2.vec,
           [0.1, 0.2, 0.3, 0.4]
       ) AS score
ORDER BY score ASC
LIMIT 3;
```

The query combines:

- Graph pattern matching.
- Vector similarity computation.
- A top-K nearest-neighbor result restricted to the nodes that satisfy the
  graph pattern.

## Vector Modification

You can modify vector data after creating an HNSW index. Inserts, updates, and
deletes automatically update the corresponding index, so subsequent HNSW
queries operate on the latest data without requiring manual index
synchronization.

### Insert Vector Data

You can insert vector properties using normal Cypher write operations.

Example:

```cypher
// Insert node property data
// Update corresponding HNSW index automatically
CREATE (
    n:vector_node {
        id: 3,
        vec: [0.2, 0.2, 0.1, 0.1]
    }
);
```

### Delete Vector Data

Deleting nodes automatically removes corresponding vector index entries.

Example:

```cypher
// Delete node data
// Remove corresponding vector index data
MATCH (n:vector_node)
WHERE n.id = 1
DELETE n;
```

### Update Vector Data

Updating a vector property automatically updates the HNSW index.

Example:

```cypher
// Update vector property
// Update HNSW index automatically
MATCH (n:vector_node)
WHERE n.id = 1
SET n.vec = [0.2, 0.2, 0.1, 0.1];
```
