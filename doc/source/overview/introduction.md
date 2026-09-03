# Introduction

**NeuG** is a high-performance, graph-native transactional database that runs embedded in your application or behind a service. It provides durable storage, explicit transactions, Cypher-native querying, and in-place graph analytics.

Built on this data foundation, NeuG is **the one data index for your agentic applications**—indexing structure, semantics, and exact keywords over the same managed data. For questions and community support, visit the [NeuG repository](https://github.com/alibaba/neug).

## One Dataset, Indexed in Multiple Ways

NeuG provides complementary access paths over one graph:

| | What is indexed | What it enables |
|---|---|---|
| **Structure** | Entities, relationships, and topology | Cypher traversal, pattern matching, and structural analysis with algorithms such as PageRank, Leiden, shortest paths, and community detection |
| **Semantics** | Dense vector properties | HNSW-based similarity retrieval using cosine, L2, or inner-product distance |
| **Keywords** | Natural-language text | Full-text retrieval with BM25 ranking, phrase queries, prefix queries, and Boolean operators |

Structure is native to NeuG's graph storage; graph algorithms are another way to use and analyze that structure, not a separate index. Vector and full-text retrieval are provided by storage indexes integrated with NeuG's query and transaction model.

All three operate over the same underlying data. Inserts, updates, and deletes maintain graph properties and their vector or full-text indexes atomically. Committed index state is persisted and recovered with the graph through checkpoints and the write-ahead log.

> **Roadmap** — NeuG's unified indexing layer will continue to support more data types and access patterns, all over the same transactional data.

## One Engine, Two Ways to Run

NeuG keeps the core runtime lightweight and supports two deployment modes:

- **Embedded Mode** runs in the application process for low-overhead agent workflows, analytics, notebooks, and batch processing.
- **Service Mode** exposes the same NeuG runtime through a network endpoint for concurrent applications and transactional access.

Both modes use the same database and Cypher query engine. Start with `db.connect()` for embedded access. When the database needs to run behind a service, close the embedded connection and call `db.serve()`. See the [dual-mode benchmark](../../tutorials/benchmark-neug-dual-mode) for reproducible performance results and methodology.

## Quick Example

```python
import neug

# Step 1: Load and analyze data (Embedded Mode)
db = neug.Database("/path/to/database") 
# Load sample data
db.load_builtin_dataset("tinysnb")

conn = db.connect()

# Run analytics
result = conn.execute("""
    MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person),
        (a)-[:KNOWS]->(c)
    RETURN a.fName, b.fName, c.fName
""")

for record in result:
    print(f"{record} are mutual friends")

# Step 2: Serve users (Service Mode)  
# Should first close the embedded connection
conn.close()
db.serve(port=8080)
# Now your application can handle concurrent users
```

## Next Steps

- **[Installation](../../installation/installation)** — Set up NeuG for Python, Node.js, or C++
- **[Getting Started](../../getting_started/getting_started)** — Create a database and run your first queries
- **[Vector Search](../../extensions/vector_search)** — Store vectors and build HNSW indexes
- **[Full-Text Search](../../extensions/fts_search)** — Build full-text indexes and run BM25-ranked queries
- **[Graph Algorithms](../../extensions/load_gds)** — Project a graph and run graph algorithms
- **[Transaction Management](../../transaction/transaction)** — Understand NeuG's transaction and isolation model
