# Introduction


Welcome to NeuG (pronounced "new-gee"), a graph database for HTAP (Hybrid Transactional/Analytical Processing) workloads. For questions and community support, visit our [GitHub repository](https://github.com/GraphScope/neug).

Most database systems excel at either analytics or transactions, but not both. Users are forced to choose between analytical performance and transactional consistency, or manage multiple systems.

**NeuG's Approach**: Instead of compromising, NeuG provides **two modes** that you can switch between based on your needs:

- **Embedded Mode**: Optimized for analytical workloads including complex pattern matching, graph analytics and bulk data loading
- **Service Mode**: Optimized for transactional workloads for real-time applications and concurrent user access

This flexibility allows you can use the same database for both data science work and production applications.

## What is NeuG?

NeuG is a graph database that gives you the best of both worlds: **powerful analytics** and **fast transactions**.

NeuG provides **two modes** that you can switch between:
- **Embedded Mode**: For data processing and analytics  
- **Service Mode**: For applications and real-time access

## Quick Example

```python
import neug

# Step 1: Load and analyze data (Embedded Mode)
db = neug.Database("/path/to/database") 
conn = db.connect()

# Load sample data
db.load_builtin_dataset("tinysnb")

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

**Dual-Mode Architecture**:
- **Embedded Mode**: Data analytics, ML/DL pipeline, bulk loading, research prototyping - no external dependencies
- **Service Mode**: Web/mobile apps, real-time APIs, multi-user systems

**Technical Excellence**:
- **Property Graph model** with standard Cypher query language
- **High Performance**: Optimized for both analytical and transactional workloads
- **Cross-Platform**: Works on Linux, macOS, x86, and ARM architectures
- **ACID Transactions**: Reliable data consistency for production applications

**Easy Integration**:
- **Zero Dependencies**: Embedded mode requires no external services or setup
- **Flexible Deployment**: Switch between embedded library and network service
- **Seamless Workflow**: Use both modes together - process offline, serve online

NeuG is developed by the [GraphScope](https://graphscope.io) team at Alibaba, bringing enterprise-scale graph computing expertise to an easy-to-use embedded database.

## Next Steps

- **[Installation](../installation/installation.md)** - Setup guide for Python and C++
- **[Getting Started](../getting_started/getting_started.md)** - Basic operations and examples  
- **[Data Import](../import_export/import_data.md)** - Loading data into your database
- **[Cypher Manual](../cypher_manual/index.md)** - Query language reference



