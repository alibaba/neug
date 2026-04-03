# Benchmark NeuG: The Dual-Mode Graph Database

This tutorial demonstrates how to reproduce NeuG's benchmark performance results. NeuG supports two execution modes:

- **Embedded Mode**: Run queries directly in your Python process with zero serialization overhead
- **Service Mode**: Connect to a NeuG server for concurrent transactional workloads

We benchmark both modes against industry-standard databases to showcase NeuG's performance advantages.

> **Note**: The LDBC SNB Interactive benchmark section covers only the complex read queries (IC1–IC14). It does not include write operations and transactional updates. A full LDBC SNB Interactive benchmark tutorial will be provided in a future release.

## Dataset: LDBC SNB SF1

Both benchmarks use the same LDBC SNB SF1 dataset:

- **Nodes**: ~3 million (Person, Post, Comment, Tag, etc.)
- **Edges**: ~17 million (KNOWS, LIKES, HASTAG, etc.)
- **Size**: ~282MB compressed, ~1.1GB extracted

### Download the Dataset

```bash
wget https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
tar -xzf ldbc-snb-sf1-lsqb.tar.gz
```

---

## LSQB Benchmark (Embedded Mode)

[LSQB](https://github.com/ldbc/lsqb) contains 9 complex subgraph matching queries that lean toward analytical workloads. This benchmark compares NeuG with LadybugDB in embedded mode.

### Query Descriptions

| Query | Description |
|-------|-------------|
| Q1 | Long path traversal (9-hop chain) |
| Q2 | 2-hop with comment-post pattern |
| Q3 | Triangle pattern in same country |
| Q4 | Multi-label with likes/replies |
| Q5 | Tag co-occurrence via comments |
| Q6 | 2-hop with interest tags |
| Q7 | Optional matches for likes/replies |
| Q8 | Tag pattern with NOT EXISTS |
| Q9 | 2-hop with NOT EXISTS |

### Running the Benchmark

```bash
# Create virtual environment
python -m venv neug-env
source neug-env/bin/activate

# Install dependencies
pip install neug real_ladybug

# Run the benchmark
cd neug/examples/lsqb_benchmark
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb
```

Use `--force` to overwrite an existing database.

### Expected Results

On Apple Silicon Mac (M1/M2/M3), NeuG wins all 9 queries with just a single thread:

| Query | NeuG (1 thread) | LadybugDB (best) | Speedup |
|-------|-----------------|------------------|---------|
| Q1 | ~2,600 ms | - | NeuG wins |
| Q2 | ~140 ms | ~12,800 ms | **91x** |
| Q3 | ~370 ms | ~106,000 ms | **287x** |
| Q4 | ~140 ms | - | NeuG wins |
| Q5 | ~830 ms | - | NeuG wins |
| Q6 | ~480 ms | ~1,540 ms | **3.2x** |
| Q7 | ~580 ms | - | NeuG wins |
| Q8 | ~710 ms | - | NeuG wins |
| Q9 | ~600 ms | ~1,020 ms | **1.7x** |

Note: Some LadybugDB queries timed out or failed to complete.

---

## LDBC SNB Interactive Benchmark (Service Mode)

[LDBC SNB Interactive](https://ldbcouncil.org/benchmarks/snb-interactive/) contains 14 complex read queries (IC1–IC14) covering multi-hop friend finding, shortest paths, and aggregation. This benchmark compares NeuG with Neo4j in service mode.

### Query Descriptions

| Query | Description |
|-------|-------------|
| IC1 | Friends with specific first name (up to 3 hops) |
| IC2 | Recent messages from friends |
| IC3 | Friends with messages in two countries |
| IC4 | Messages with specific tags in time range |
| IC5 | Forums with most friend members |
| IC6 | Friends with tagged posts |
| IC7 | Recent likes on messages |
| IC8 | Recent replies on messages |
| IC9 | Recent messages from friends of friends |
| IC10 | Friends with same birthday month |
| IC11 | Friends working in specific country |
| IC12 | Friends with specific tag class interests |
| IC13 | Shortest path between two persons |
| IC14 | Weighted shortest path between two persons |

### Running the Benchmark

```bash
# Start Neo4j server (optional, for comparison)
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/neo4j123 \
  neo4j:latest

# Create virtual environment
python -m venv neug-env
source neug-env/bin/activate

# Install dependencies
pip install neug neo4j

# Run the benchmark
cd neug/examples/ldbc_interactive_benchmark
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb
```

### Expected Results

On Apple Silicon Mac (M1/M2/M3), with 4 concurrent clients:

**Throughput Comparison**

| Engine | QPS | P50 Latency | P95 Latency |
|--------|-----|-------------|-------------|
| NeuG | **617** | 3.1 ms | 20.6 ms |
| Neo4j | 12.2 | 16.0 ms | 1,728 ms |

NeuG achieves **50.6x** the throughput of Neo4j.

**Per-Query Latency (P50 ms)**

| Query | NeuG | Neo4j | NeuG Wins |
|-------|------|-------|-----------|
| IC1 | 6.2 | 5.2 | - |
| IC2 | 3.2 | 8.4 | **2.6x** |
| IC3 | 11.1 | 984.4 | **89x** |
| IC4 | 10.9 | 8.0 | - |
| IC5 | 22.3 | 1892.4 | **85x** |
| IC6 | 2.7 | 423.0 | **157x** |
| IC7 | 4.5 | 4.0 | - |
| IC8 | 2.4 | 1.0 | - |
| IC9 | 3.9 | 829.8 | **212x** |
| IC10 | 5.3 | 84.2 | **16x** |
| IC11 | 2.4 | 6.6 | **2.7x** |
| IC12 | 3.9 | 18.8 | **4.8x** |
| IC13 | 0.4 | 0.8 | **2x** |
| IC14 | 4.5 | 185.2 | **41x** |

NeuG wins on 10 out of 14 queries, with dramatic speedups on complex multi-hop and aggregation queries.

---

## Why NeuG is Faster

NeuG's performance advantage comes from:

### Embedded Mode Advantages

1. **Graph-native query optimizer**: [GOpt](https://graphscope.io/blog/tech/2024/02/22/GOpt-A-Unified-Graph-Query-Optimization-Framework-in-GraphScope) performs cost-based optimization with graph-specific cardinality estimation.

2. **Columnar vectorized execution**: Efficient memory management and cache-friendly traversals.

3. **Zero serialization overhead**: Executes in-process with direct memory access.

### Service Mode Advantages

1. **MVCC concurrency control**: Efficient handling of concurrent transactions without blocking reads.

2. **Battle-tested engine**: NeuG is built on [GraphScope Flex](https://graphscope.io), the engine that [set the LDBC SNB Interactive world record](https://ldbcouncil.org/benchmarks/snb/interactive/2025-04-21-graphscope-flex-sf300/) at 80,000+ QPS.

---

## Reproducibility

All performance results are independently reproducible:

- **Dataset**: https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
- **LSQB Scripts**: `neug/examples/lsqb_benchmark/`
- **Interactive Scripts**: `neug/examples/ldbc_interactive_benchmark/`
- **Tutorial**: `neug/doc/source/tutorials/benchmark-neug-dual-mode.md`

If you encounter any issues reproducing these results, please report them on [GitHub Issues](https://github.com/alibaba/neug/issues).