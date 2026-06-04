<p align="center">
  <img src="img/neug-logo.png" width="280" alt="NeuG"><br>
  <b>A Graph Database for HTAP Workloads</b><br><br>
  <a href="https://github.com/alibaba/neug/actions/workflows/neug-test.yml"><img src="https://github.com/alibaba/neug/actions/workflows/neug-test.yml/badge.svg" alt="NeuG Test"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/build-wheel.yml"><img src="https://github.com/alibaba/neug/actions/workflows/build-wheel.yml/badge.svg" alt="NeuG Wheel Packaging"></a>
  <a href="https://github.com/alibaba/neug/actions/workflows/docs.yml"><img src="https://github.com/alibaba/neug/actions/workflows/docs.yml/badge.svg" alt="NeuG Documentation"></a>
  <a href="https://codecov.io/gh/alibaba/neug"><img src="https://codecov.io/gh/alibaba/neug/branch/main/graph/badge.svg" alt="Coverage"></a>
  <a href="https://discord.gg/2S8344ew"><img src="https://img.shields.io/badge/Discord-NeuG-7289da?logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/graphscope2021"><img src="https://img.shields.io/badge/Twitter-@graphscope2021-1da1f2?logo=x&logoColor=white" alt="Twitter"></a>
</p>

---

**NeuG** (pronounced "new-gee") is a graph database for HTAP (Hybrid Transactional/Analytical Processing) workloads. NeuG provides **two modes** that you can switch between based on your needs:

- **Embedded Mode**: Optimized for analytical workloads including bulk data loading, complex pattern matching, and graph analytics
- **Service Mode**: Optimized for transactional workloads for real-time applications and concurrent user access

For more information on using NeuG, please refer to the [NeuG documentation](https://graphscope.io/neug/en/overview/introduction/).

## News
- **2026-03**: We officially release NeuG v0.1
- **2025-06**: We shatter [LDBC SNB Interactive Benchmark world record](https://graphscope.io/blog/tech/2025/06/12/graphscope-flex-achieved-record-breaking-on-ldbc-snb-interactive-workload-declarative) with 80,000+ QPS for declarative queries

## Installation

```bash
pip install neug
```

Please note that `neug` requires `Python` version 3.8 or above. The package works on Linux, macOS, and Windows (via WSL2).

For more detailed installation instructions, please refer to the [installation guide](https://graphscope.io/neug/en/installation/installation).

## Quick Example

```python
import neug

# Step 1: Load and analyze data (Embedded Mode)
db = neug.Database("/path/to/database") 

# Load sample data (must load data before creating connection)
db.load_builtin_dataset("tinysnb")

# Create connection to execute queries
conn = db.connect()

# Run analytics - find triangles in the graph
result = conn.execute("""
    MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person),
          (a)-[:knows]->(c)
    RETURN a.fName, b.fName, c.fName
""")

# Access results by index (QueryResult returns a list for each row)
for record in result:
    print(f"{record[0]}, {record[1]}, {record[2]} are mutual friends")

# Step 2: Serve applications (Service Mode)  
conn.close()
db.serve(port=8080)
# Now your application can handle concurrent users
```


## Development & Contributing

For building NeuG from source and development instructions, see the [Development Guide](./doc/source/development/dev_guide.md).

We welcome contributions! Please read our [Contributing Guide](./CONTRIBUTING.md) before submitting issues or pull requests.

### AI-Assisted Workflow

We apply an AI-assisted **Spec-Driven** workflow inspired by [GitHub Spec-Kit](https://github.com/github/spec-kit). We provide convenient commands for contributions:

- 🐛 **Bug Reports**: Use `/create-issue` command in your IDE, or [submit an issue](https://github.com/alibaba/neug/issues) manually
- 💻 **Pull Requests**: Use `/create-pr` command in your IDE, or [submit a PR](https://github.com/alibaba/neug/pulls) manually

For more details, see the [AI-Assisted Development Guide](./doc/source/development/ai_coding.md).

## Acknowledgements

NeuG builds upon the excellent work of the open-source community. We would like to acknowledge:

- **[Kùzu](https://github.com/kuzudb/kuzu/)**: Our C++ Cypher compiler is adapted from Kùzu's implementation
- **[DuckDB](https://duckdb.org/)**: Our runtime value system and extension framework are inspired by DuckDB's architecture

## License

NeuG is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
