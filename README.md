# NeuG

**NeuG** (pronounced "new-gee") is a graph database for HTAP (Hybrid Transactional/Analytical Processing) workloads. NeuG provides **two modes** that you can switch between based on your needs:

- **Embedded Mode**: Optimized for analytical workloads including bulk data loading, complex pattern matching, and graph analytics
- **Service Mode**: Optimized for transactional workloads for real-time applications and concurrent user access

[![NeuG Test](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/neug-test.yml)
[![NeuG Wheel Packaging](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/build-wheel.yml)
[![NeuG Documentation](https://github.com/GraphScope/neug/actions/workflows/docs.yml/badge.svg)](https://github.com/GraphScope/neug/actions/workflows/docs.yml)

For more information on using NeuG, please refer to the [NeuG documentation](https://graphscope.io/en/overview/introduction/).

## News
- **2025-09**: We officially release NeuG v0.1.0 🎉
- **2025-06**: We shatter [LDBC SNB Interactive Benchmark world record](https://graphscope.io/blog/tech/2025/06/12/graphscope-flex-achieved-record-breaking-on-ldbc-snb-interactive-workload-declarative) with 80,000+ QPS for declarative queries 🎉

## Installation

```bash
pip install neug
```

Please note that `neug` requires `Python` version 3.8 or above. The package works on Linux, macOS, and Windows (via WSL2).

For more detailed installation instructions, please refer to the [installation guide](./doc/source/installation/installation.md).

## Quick Example

```python
import neug

# Step 1: Load and analyze data (Embedded Mode)
db = neug.Database("/path/to/database") 
conn = db.connect()

# Load sample data
db.load_builtin_dataset("tinysnb")

# Run analytics - find triangles in the graph
result = conn.execute("""
    MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person),
          (a)-[:knows]->(c)
    RETURN a.fName, b.fName, c.fName
""")

for record in result:
    print(f"{record['a.fName']}, {record['b.fName']}, {record['c.fName']} are mutual friends")

# Step 2: Serve applications (Service Mode)  
conn.close()
db.serve(port=8080)
# Now your application can handle concurrent users
```

## Documentation

📚 **[Official Documentation](https://graphscope.io/en/overview/introduction/)** - Comprehensive guides, tutorials, and API reference

📝 **[GraphScope Blog](https://graphscope.io/blog/)** - Latest updates and technical insights

You can also build the documentation locally:

```bash
cd doc && make dependencies && make html
python3 -m http.server --directory build/html 8080
```

## Development & Contributing

For building NeuG from source and development instructions, see the [Development Guide](./doc/source/development/dev_guide.md).

We welcome contributions! Please read our [Contributing Guide](./CONTRIBUTING.md) before submitting issues or pull requests.

### AI-Assisted Workflow

We apply an AI-assisted **Spec-Driven** workflow inspired by [GitHub Spec-Kit](https://github.com/github/spec-kit). We provide convenient commands for contributions:

- 🐛 **Bug Reports**: Use `/create-issue` command in your IDE, or [submit an issue](https://github.com/GraphScope/neug/issues) manually
- 💻 **Pull Requests**: Use `/create-pr` command in your IDE, or [submit a PR](https://github.com/GraphScope/neug/pulls) manually

For more details, see the [AI-Assisted Development Guide](./doc/source/development/ai_coding.md).

## License

NeuG is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
