<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="img/neug-logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="img/neug-logo-light.png">
    <img src="img/neug-logo-light.png" width="320" alt="NeuG">
  </picture><br>
  <b>A Graph Database for HTAP Workloads</b><br><br>
  <a href="https://github.com/alibaba/neug/actions/workflows/Linux.yml"><img src="https://github.com/alibaba/neug/actions/workflows/Linux.yml/badge.svg" alt="NeuG Test (Linux)"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/build-wheel.yml"><img src="https://github.com/alibaba/neug/actions/workflows/build-wheel.yml/badge.svg" alt="NeuG Wheel Packaging"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/docs.yml"><img src="https://github.com/alibaba/neug/actions/workflows/docs.yml/badge.svg" alt="NeuG Documentation"></a>
  <a href="https://codecov.io/gh/alibaba/neug"><img src="https://codecov.io/gh/alibaba/neug/branch/main/graph/badge.svg" alt="Coverage"></a>
  <a href="https://discord.gg/2S8344ew"><img src="https://img.shields.io/badge/Discord-NeuG-7289da?logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/graphscope2021"><img src="https://img.shields.io/badge/Twitter-@graphscope2021-1da1f2?logo=x&logoColor=white" alt="Twitter"></a>
</p>

---

**NeuG** (pronounced "new-gee") is a graph database for HTAP (Hybrid Transactional/Analytical Processing) workloads. It provides **two modes** that you can switch between based on your needs:

- **Embedded Mode**: Optimized for analytical workloads including bulk data loading, complex pattern matching, and graph analytics
- **Service Mode**: Optimized for transactional workloads for real-time applications and concurrent user access

For more information, please refer to the [NeuG documentation](https://graphscope.io/neug/en/overview/introduction/).

## News

- **2026-06** — NeuG v0.1.3: [GDS extensions](https://graphscope.io/neug/en/extensions/load_gds/), [`COPY TEMP`](https://graphscope.io/neug/en/data_io/import_data/), [Node.js client](https://graphscope.io/neug/en/reference/nodejs_api/)
- **2026-05** — NeuG v0.1.2: [`LOAD FROM`](https://graphscope.io/neug/en/data_io/load_data/), [Parquet](https://graphscope.io/neug/en/extensions/load_parquet/) & [HTTPFS](https://graphscope.io/neug/en/extensions/load_httpfs/) extensions
- **2026-03** — NeuG v0.1 released
- **2025-06** — Shattered [LDBC SNB Interactive Benchmark world record](https://graphscope.io/blog/tech/2025/06/12/graphscope-flex-achieved-record-breaking-on-ldbc-snb-interactive-workload-declarative) with 80,000+ QPS

## Installation

The packages work on Linux, macOS, and Windows (via WSL2). For more detailed instructions (including C++ from source), see the [installation guide](https://graphscope.io/neug/en/installation/installation).

<details open>
<summary><b>Python</b> &nbsp;·&nbsp; requires Python 3.8+</summary>

```bash
pip install neug
```
</details>

<details>
<summary><b>Node.js</b> &nbsp;·&nbsp; requires Node.js 18+ &nbsp;(since v0.1.3)</summary>

```bash
npm install @graphscope-neug/neug
```
</details>

## Quick Example

<details open>
<summary><b>Python</b></summary>

```python
import neug

db = neug.Database("/path/to/database")
db.load_builtin_dataset("tinysnb")

conn = db.connect()

# Find triangles in the graph
result = conn.execute("""
    MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person),
          (a)-[:knows]->(c)
    RETURN a.fName, b.fName, c.fName
""")

for record in result:
    print(f"{record[0]}, {record[1]}, {record[2]} are mutual friends")

# Switch to service mode
conn.close()
db.serve(port=8080)
```
</details>

<details>
<summary><b>Node.js</b></summary>

```javascript
const { Database } = require('@graphscope-neug/neug');

const db = new Database({ databasePath: '', mode: 'w' });
const conn = db.connect();

conn.execute("CREATE NODE TABLE person(id INT64, fName STRING, PRIMARY KEY(id));");
conn.execute("CREATE REL TABLE knows(FROM person TO person);");
conn.execute("CREATE (:person {id: 1, fName: 'Alice'}), (:person {id: 2, fName: 'Bob'}), (:person {id: 3, fName: 'Carol'});");
conn.execute("MATCH (a:person {id: 1}), (b:person {id: 2}), (c:person {id: 3}) CREATE (a)-[:knows]->(b), (b)-[:knows]->(c), (a)-[:knows]->(c);");

// Find triangles in the graph
const result = conn.execute(`
    MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person),
          (a)-[:knows]->(c)
    RETURN a.fName, b.fName, c.fName
`);

for (const record of result) {
    console.log(`${record[0]}, ${record[1]}, ${record[2]} are mutual friends`);
}

conn.close();
db.close();
```
</details>

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

## License

NeuG is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
