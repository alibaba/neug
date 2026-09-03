# Introduction

**NeuG** is a high-performance, graph-native transactional database that runs embedded in your application or behind a service. It provides durable storage, explicit transactions, Cypher-native querying, and in-place graph analytics.

Starting with **NeuG v0.2**, NeuG introduces a storage-index framework with HNSW vector search and BM25 full-text search. Together with NeuG's native graph structure, these capabilities make NeuG **the one data index for your agentic applications**—bringing structure, semantics, and exact keywords together over the same managed data. These indexing capabilities are not available in NeuG v0.1.x. For questions and community support, visit the [NeuG repository](https://github.com/alibaba/neug).

## Key Capabilities

- **Graph-native data management** — Store entities, relationships, and properties with durable storage, explicit transactions, checkpoints, and write-ahead logging.
- **Unified retrieval** — Query graph structure natively and use HNSW and BM25 indexes for semantic and keyword retrieval over the same data.
- **Cypher query and graph analytics** — Traverse and filter with Cypher, then run algorithms such as PageRank, Leiden, and shortest path without exporting the graph to another system.
- **Embedded or service deployment** — Run NeuG in-process for low-overhead local workflows, or expose the same database as a service for concurrent applications. See the [dual-mode benchmark](../../tutorials/benchmark-neug-dual-mode) for reproducible results.
- **Extensible and interoperable** — Add capabilities through extensions and exchange data through formats and systems such as Apache Arrow, Parquet, S3, and OSS.

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

## Quick Example

The following NeuG v0.2 example indexes the same `Runbook` data by semantics and keywords, while keeping its graph structure directly queryable:

```cypher
LOAD vector_search;
LOAD fts;

CREATE INDEX runbook_vec ON Runbook
USING HNSW (embedding) WITH (metric = 'l2');

CREATE INDEX runbook_text ON Runbook
USING FTS (content);

// Structure: follow relationships
MATCH (:Service {name: 'PaymentService'})-[:HAS_RUNBOOK]->(r:Runbook)
RETURN r.title;

// Semantics: find similar meaning
MATCH (r:Runbook)
RETURN r.title,
       vector_distance_l2(r.embedding, [0.1, 0.2, 0.3, 0.4]) AS distance
ORDER BY distance ASC LIMIT 5;

// Keywords: rank exact terms
MATCH (r:Runbook)
RETURN r.title, bm25(r.content, 'retry timeout') AS score
ORDER BY score ASC LIMIT 5;
```

See [Vector Search](../../extensions/vector_search) and [Full-Text Search](../../extensions/fts_search) for setup, index options, and complete examples.

## Start Exploring

- **[Installation](../../installation/installation)** — Set up NeuG for Python, Node.js, or C++
- **[Getting Started](../../getting_started/getting_started)** — Create a database and run your first queries
- **[Vector Search](../../extensions/vector_search)** — Store vectors and build HNSW indexes
- **[Full-Text Search](../../extensions/fts_search)** — Build full-text indexes and run BM25-ranked queries
- **[Graph Algorithms](../../extensions/load_gds)** — Project a graph and run graph algorithms
- **[Transaction Management](../../transaction/transaction)** — Understand NeuG's transaction and isolation model
