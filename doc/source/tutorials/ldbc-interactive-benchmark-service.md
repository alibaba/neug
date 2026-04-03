# LDBC SNB Interactive Benchmark: NeuG vs Neo4j (Service Mode)

This tutorial demonstrates how to reproduce the LDBC SNB Interactive Benchmark performance results comparing NeuG with Neo4j in service mode.

> **Note**: This tutorial covers only the complex read queries (IC1–IC14) from the LDBC SNB Interactive Benchmark. It does not include the write operations and transactional updates that are part of the complete benchmark. A full LDBC SNB Interactive Benchmark tutorial will be provided in a future release.

## Overview

[LDBC SNB Interactive](https://ldbcouncil.org/benchmarks/snb-interactive/) contains 14 complex read queries (IC1–IC14) covering multi-hop friend finding, shortest paths, and aggregation — typical transactional graph workloads.

### Dataset: LDBC SNB SF1

- **Nodes**: ~3 million (Person, Post, Comment, Tag, etc.)
- **Edges**: ~17 million (KNOWS, LIKES, HASTAG, etc.)
- **Size**: ~282MB compressed, ~1.1GB extracted

## Prerequisites

- Python 3.10+ with `pip`
- NeuG Python package: `pip install neug`
- Neo4j Python driver: `pip install neo4j`
- Neo4j server running at `bolt://localhost:7687` (with `neo4j/neo4j123` credentials)
- About 1.5GB disk space for the dataset

## Download the Dataset

The LDBC SNB SF1 dataset is available on OSS:

```bash
# Download and extract the dataset
wget https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
tar -xzf ldbc-snb-sf1-lsqb.tar.gz
```

## Interactive Complex Query Descriptions

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

## Running the Benchmark

The complete benchmark script is available in the NeuG repository at `examples/ldbc_interactive_benchmark/`.

### Step 1: Start Neo4j Server

```bash
# Using Docker
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/neo4j123 \
  neo4j:latest
```

### Step 2: Create Virtual Environment

```bash
python -m venv neug-env
source neug-env/bin/activate  # Linux/macOS
```

### Step 3: Install Dependencies

```bash
pip install neug neo4j
```

### Step 4: Run the Benchmark

```bash
cd neug/examples/ldbc_interactive_benchmark
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb
```

## Expected Results

On an Apple Silicon Mac (M1/M2/M3), with 4 concurrent clients for 300 seconds:

### Throughput Comparison

| Engine | QPS | P50 Latency | P95 Latency | Total Queries |
|--------|-----|-------------|-------------|---------------|
| NeuG | **617** | 3.1 ms | 20.6 ms | 185,156 |
| Neo4j | 12.2 | 16.0 ms | 1,728 ms | 3,659 |

NeuG achieves **50.6x** the throughput of Neo4j. On latency, NeuG's P95 is just 20.6ms while Neo4j's P95 reaches 1,728ms.

### Per-Query Latency (P50 ms)

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

## Why NeuG is Faster in Service Mode

NeuG's performance advantage in service mode comes from:

1. **MVCC concurrency control**: Efficient handling of concurrent transactions without blocking reads.

2. **Columnar execution engine**: Vectorized processing for complex queries.

3. **Graph-native optimizer**: GOpt performs cost-based optimization with graph-specific cardinality estimation.

4. **Battle-tested engine**: NeuG is built on [GraphScope Flex](https://graphscope.io), the engine that [set the LDBC SNB Interactive world record](https://ldbcouncil.org/benchmarks/snb/interactive/2025-04-21-graphscope-flex-sf300/) at 80,000+ QPS.

## Reproducibility

All performance results are independently reproducible:

- **Dataset**: https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
- **Benchmark Scripts**: `neug/examples/ldbc_interactive_benchmark/`
- **Tutorial**: `neug/doc/source/tutorials/ldbc-interactive-benchmark-service.md`

If you encounter any issues reproducing these results, please report them on [GitHub Issues](https://github.com/alibaba/neug/issues).