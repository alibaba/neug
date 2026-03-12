# Introduction


Welcome to NeuG (pronounced "new-gee"), a high-performance embedded graph database for analytics and real-time transactions. For questions and community support, visit our [GitHub repository](https://github.com/alibaba/neug).

NeuG follows the same design philosophy as [DuckDB](https://duckdb.org/) — but for graph databases: **lightweight, minimal dependencies, and easy to embed**. Just as DuckDB revolutionized how developers work with relational data, NeuG brings that same simplicity to graph data.

The core library stays simple by design. When you need to serve concurrent users, simply call `db.serve()` to expose the same database as a network service. For production concerns like high availability and security, we will manage them externally rather than coupling them into the core — keeping NeuG focused on what it does best.

**Two Modes, One Lightweight Library**:

- **Embedded Mode**: Import as a Python library with minimal external dependencies. Perfect for data science workflows, ML/AI pipelines, and batch analytics
- **Service Mode**: Call `db.serve()` to start a network service for concurrent access and real-time queries. The same lightweight core, now accessible over the network

This design makes NeuG easy to integrate into Python applications, Jupyter notebooks, and upcoming environments like Node.js — wherever you need graph capabilities without the overhead of running a JVM, or configuring complex infrastructure.

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
    MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person),
        (a)-[:knows]->(c)
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

## Key Features

**Lightweight & Embeddable**:
- Single binary, zero external dependencies
- Embed directly into your Python app for offline analytics, or run as a service for online transactions — no DevOps overhead

**Cypher-Native, GQL-Ready**:
- Write queries in industry-standard Cypher
- Powered by [GOpt](https://graphscope.io/blog/tech/2024/02/22/GOpt-A-Unified-Graph-Query-Optimization-Framework-in-GraphScope)'s unified IR design — ready for [ISO/GQL](https://www.gqlstandards.org/) with minimal migration cost

**Extensible by Design**:
- Postgres/DuckDB-inspired extension system
- Keep the core lean. Add graph algorithms, vector search, or custom procedures through an extensible framework

**High Performance**:
- Built on GraphScope Flex, which set the [record on LDBC SNB Interactive benchmark](https://ldbcouncil.org/benchmarks/snb/interactive/2025-04-21-graphscope-flex-sf300/) using Cypher queries (80,000+ QPS)
- Optimized for both analytical and transactional workloads
- Works on Linux, macOS, x86, and ARM architectures

**ACID Transactions**:
- Reliable data consistency for production applications
- Multi-session transaction support in Service Mode

NeuG is developed by the [GraphScope](https://graphscope.io) team at Alibaba, bringing enterprise-scale graph computing expertise to an easy-to-use embedded database.

## What's Next

NeuG is actively evolving. Here's what we're working on for v0.2:

- **Node.js Binding** — AI Agent integration ready
- **Graph Algorithms** — In particular Leiden community detection for AI applications
- **Data Lake Support** — S3/OSS + Parquet integration
- **Vector DB Extension** — RAG & GraphRAG support

Star us on [GitHub](https://github.com/alibaba/neug) to stay updated on new releases.

## Next Steps

- **[Installation](../../installation/installation)** - Setup guide for Python and C++
- **[Getting Started](../../getting_started/getting_started)** - Basic operations and examples  
- **[Data Import](../../data_io/import_data)** - Loading data into your database
- **[Cypher Manual](../../cypher_manual)** - Query language reference



