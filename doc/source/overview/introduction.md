# Introducing NeuG

Welcome to NeuG (pronounced "new-gee"), an HTAP (Hybrid Transactional/Analytical Processing) graph database designed for high-performance embedded graph analytics and real-time transaction processing.

NeuG is built to address the growing need for systems that can handle both operational graph workloads and complex analytical queries on the same data, providing a unified platform for modern graph applications.

## What is NeuG?

NeuG is a graph database that combines the best of both worlds through its **unique dual-mode architecture**:
- **Transactional Processing (TP)**: Real-time graph operations with ACID guarantees
- **Analytical Processing (AP)**: High-performance graph analytics and pattern matching
- **Dual Connection Modes**: Flexible deployment as embedded or service-based system
- **Optimized Performance**: Each mode is specifically tuned for its target workload

Built on a modern C++ core engine, NeuG provides:
- Property Graph data model with Cypher query language
- Columnar disk-based storage with compressed adjacency lists
- Vectorized and parallelized query processing
- MVCC-based transaction management

## Why Choose NeuG?

### Unique Dual-Mode Architecture

NeuG's standout feature is its **dual-mode design** that optimizes performance for different graph workload patterns:

#### Embedded Mode - Optimized for Analytics (AP)
**Best for**: Complex pattern matching, full-graph computations, and analytical workloads
- **Batch Data Loading**: Efficiently handle large-scale data imports
- **Maximum Query Performance**: Utilize all available CPU cores for complex queries
- **Full Resource Utilization**: Every query can leverage the complete system resources

**Typical Use Cases**:
- Graph analytics and data science workloads
- Complex pattern detection and graph algorithms
- Batch processing and ETL operations
- Research and exploratory data analysis

#### Service Mode - Optimized for Transactions (TP)
**Best for**: Real-time applications with frequent small updates and concurrent access
- **Concurrent Read-Write Operations**: Optimized for high-frequency, small-scale data modifications
- **Point Queries**: Efficient access to small subsets of graph data
- **Multi-Session Support**: Handle multiple concurrent connections
- **Low-Latency Operations**: Fast response times for transactional workloads

**Typical Use Cases**:
- Real-time recommendation systems
- Fraud detection and risk management
- Online transaction processing

### Flexible Mode Combination

The true power of NeuG lies in the **flexible combination** of both modes:

- **Data Pipeline Integration**: Use embedded mode for initial bulk data loading, then switch to service mode for operational queries
- **Hybrid Workflows**: Export a checkpointed data from a running TP service to a separate copy for intensive AP analysis
- **Development to Production**: Develop and test with embedded mode, deploy as service for production
- **Workload Separation**: Run AP workloads on dedicated embedded instances while maintaining TP services

> **Note**: While embedded mode can handle small-scale modifications, but frequent writes will block all read queries (and vice versa) due to its exclusive locking design. Similarly, service mode can execute large pattern queries, but it is not recommended to use all available threads for single query acceleration since this would starve other concurrent queries.

### Performance and Scalability
NeuG is engineered for high-performance graph workloads with:
- Advanced query optimization and execution planning
- Vectorized operations for analytical workloads
- Efficient storage format optimized for graph traversals
- Multi-core parallelism for complex queries

### Flexibility and Usability
- **Cross-Platform**: Runs on Linux and macOS across x86 and ARM architectures
- **Easy Integration**: Embedded architecture eliminates server management overhead
- **Mode Switching**: Seamlessly transition between embedded and service modes


### Enterprise-Ready Features
- **ACID Transactions**: Serializable ACID guarantees for mission-critical applications
- **Concurrent Access**: Optimized for both read-heavy analytics and write-intensive operations
- **Error Handling**: Comprehensive error management and recovery mechanisms
- **Monitoring**: Built-in performance monitoring and query profiling

NeuG is developed by the [GraphScope](https://graphscope.io) team, a leading research group from Alibaba renowned for its expertise in graph computing and large-scale data processing. The framework integrates advanced methodologies derived from Alibaba’s extensive experience in deploying graph analytics to solve enterprise-scale challenges across domains such as e-commerce, supply chain optimization, and fraud detection.

