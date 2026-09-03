<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="img/neug-logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="img/neug-logo-light.png">
    <img src="img/neug-logo-light.png" width="320" alt="NeuG">
  </picture><br>
  <b>The one data index for your agentic applications</b><br><br>
  <a href="https://github.com/alibaba/neug/actions/workflows/Linux.yml"><img src="https://github.com/alibaba/neug/actions/workflows/Linux.yml/badge.svg" alt="NeuG Test (Linux)"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/release-wheel.yml"><img src="https://github.com/alibaba/neug/actions/workflows/release-wheel.yml/badge.svg" alt="NeuG Wheel Packaging"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/docs.yml"><img src="https://github.com/alibaba/neug/actions/workflows/docs.yml/badge.svg" alt="NeuG Documentation"></a>
  <a href="https://codecov.io/gh/alibaba/neug"><img src="https://codecov.io/gh/alibaba/neug/branch/main/graph/badge.svg" alt="Coverage"></a>
  <a href="https://x.com/graphscope2021"><img src="https://img.shields.io/badge/Follow-NeuG-111111?logo=x&logoColor=white" alt="Follow NeuG"></a>
</p>

---

**NeuG** (pronounced "new-gee") is a high-performance, graph-native transactional database that runs embedded in your application or behind a service. It provides durable storage, explicit transactions, Cypher-native querying, and in-place graph analytics.

Built on this data foundation, NeuG is **the one data index for your agentic applications**—indexing structure, semantics, and exact keywords over the same managed data. For more information, see the [NeuG documentation](https://graphscope.io/neug/en/overview/introduction/).

## News

- **2026-09** — NeuG v0.2: [HNSW vector search](https://graphscope.io/neug/en/extensions/vector_search/), [BM25 full-text search](https://graphscope.io/neug/en/extensions/fts_search/) and [explicit transactions](https://graphscope.io/neug/en/transaction/transaction/)
- **2026-07** — NeuG is listed in [Database of Databases](https://dbdb.io/db/neug)

<details>
<summary><b>Previous news</b></summary>
- **2026-06** — NeuG v0.1.3: [GDS extensions](https://graphscope.io/neug/en/extensions/load_gds/), [`COPY TEMP`](https://graphscope.io/neug/en/data_io/import_data/), [Node.js client](https://graphscope.io/neug/en/reference/nodejs_api/)
- **2026-05** — NeuG v0.1.2: [`LOAD FROM`](https://graphscope.io/neug/en/data_io/load_data/), [Parquet](https://graphscope.io/neug/en/extensions/load_parquet/) & [HTTPFS](https://graphscope.io/neug/en/extensions/load_httpfs/) extensions
- **2026-03** — NeuG v0.1 released
- **2025-06** — GraphScope Flex, the engine foundation behind NeuG, set an [LDBC SNB Interactive Benchmark record](https://graphscope.io/blog/tech/2025/06/12/graphscope-flex-achieved-record-breaking-on-ldbc-snb-interactive-workload-declarative) with 80,000+ QPS

</details>

## Installation

The packages support Linux and macOS on x86_64 and ARM64. Windows users can run NeuG through WSL2; native Windows support is on the roadmap. For more detailed instructions (including C++ from source), see the [installation guide](https://graphscope.io/neug/en/installation/installation).

<details open>
<summary><b>Python</b> &nbsp;·&nbsp; requires Python 3.8+</summary>

```bash
pip install neug
```
</details>

<details>
<summary><b>Node.js</b> &nbsp;·&nbsp; requires Node.js 20+ &nbsp;(since v0.1.3)</summary>

```bash
npm install @graphscope-neug/neug
```
</details>

## Quick Example

The same data can be queried by graph structure, vector similarity, or exact keywords. With the extensions installed and `Service` and `Runbook` data already loaded:

```python
import neug

db = neug.Database("agent.db")
conn = db.connect()
conn.execute("LOAD vector_search;")
conn.execute("LOAD fts;")
conn.execute("CREATE INDEX runbook_vec ON Runbook USING HNSW (embedding) WITH (metric = 'l2');")
conn.execute("CREATE INDEX runbook_text ON Runbook USING FTS (content);")
query_embedding = [0.1, 0.2, 0.3, 0.4]

# Structure
conn.execute("""
    MATCH (:Service {name: 'PaymentService'})-[:HAS_RUNBOOK]->(r:Runbook)
    RETURN r.title
""")

# Semantics — accelerated by an HNSW index on Runbook.embedding
conn.execute("""
    MATCH (r:Runbook)
    RETURN r.title, vector_distance_l2(r.embedding, $embedding) AS distance
    ORDER BY distance ASC LIMIT 5
""", parameters={"embedding": query_embedding})

# Keywords — ranked by an FTS index on Runbook.content
conn.execute("""
    MATCH (r:Runbook)
    RETURN r.title, bm25(r.content, 'retry timeout') AS score
    ORDER BY score ASC LIMIT 5
""")
```

[Create an HNSW index](./doc/source/extensions/vector_search.md#create-hnsw-index) · [Create a full-text index](./doc/source/extensions/fts_search.md#create-an-fts-index)

## One Data, Indexed Three Ways

NeuG provides complementary ways to retrieve and analyze the same entities and properties:

| | What NeuG indexes | What it enables |
|---|---|---|
| **Structure** | Entities, relationships, and graph topology | Cypher traversal, pattern matching, PageRank, Leiden, shortest paths, and more |
| **Semantics** | Dense vector properties with HNSW | Similarity search using cosine, L2, or inner-product distance |
| **Keywords** | Text properties with full-text indexes | BM25-ranked word, phrase, prefix, Boolean, and exclusion search |

Structure is native to NeuG's graph storage. Vector and full-text indexes are maintained with the same underlying graph properties: graph changes and index changes commit atomically, and committed indexes recover with the graph through checkpoints and the write-ahead log.

## Embedded or Service

Run NeuG in-process for local agent workflows and low-overhead analytics. When concurrent applications need network access, expose the same runtime as a service with `db.serve()`.

See the [reproducible dual-mode benchmark](./doc/source/tutorials/benchmark-neug-dual-mode.md) for complete results and methodology.

## Development & Contributing

For building NeuG from source, see the [Development Guide](./doc/source/development/dev_guide.md). We welcome contributions — please read the [Contributing Guide](./CONTRIBUTING.md) before submitting issues or pull requests.

<details>
<summary>AI-Assisted Workflow</summary>

We apply an AI-assisted **Spec-Driven** workflow inspired by [GitHub Spec-Kit](https://github.com/github/spec-kit):

- 🐛 **Bug Reports**: Use `/create-issue` command in your IDE, or [submit an issue](https://github.com/alibaba/neug/issues) manually
- 💻 **Pull Requests**: Use `/create-pr` command in your IDE, or [submit a PR](https://github.com/alibaba/neug/pulls) manually

For more details, see the [AI-Assisted Development Guide](./doc/source/development/ai_coding.md).
</details>

## Acknowledgements

NeuG builds upon the excellent work of the open-source community. We would like to acknowledge:

- **[Kùzu](https://github.com/kuzudb/kuzu/)**: Our C++ Cypher compiler is adapted from Kùzu's implementation
- **[DuckDB](https://duckdb.org/)**: Our runtime value system and extension framework are inspired by DuckDB's architecture
- **[zvec](https://github.com/alibaba/zvec)**: Its in-process vector indexing engine provides the HNSW foundation for NeuG's vector search extension

## License

NeuG is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
