# LDBC SNB Interactive Benchmark for NeuG

This directory contains scripts to reproduce the LDBC SNB Interactive Benchmark
performance results comparing NeuG (service mode) with Neo4j.

## Dataset

Download the LDBC SNB SF1 dataset:

```bash
wget https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
tar -xzf ldbc-snb-sf1-lsqb.tar.gz
```

The dataset contains:
- ~3 million nodes (Person, Post, Comment, Tag, etc.)
- ~17 million edges (KNOWS, LIKES, HASTAG, etc.)
- Size: ~282MB compressed, ~1.1GB extracted

## Prerequisites

- Python 3.10+ with pip
- NeuG: `pip install neug`
- Neo4j Python driver: `pip install neo4j`
- Neo4j server running at `bolt://localhost:7687` (optional, for comparison)

## Quick Start

```bash
# Create virtual environment
python -m venv neug-env
source neug-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the benchmark (NeuG only)
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb

# Run with Neo4j comparison (requires Neo4j server running)
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb

# To overwrite an existing database, use --force
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb --force
```

## Command Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--data-dir` | Yes | - | Path to LDBC SNB SF1 dataset directory |
| `--db-path` | No | `./ldbc_sf1.db` | Path to NeuG database |
| `--derived-dir` | No | `./derived_csvs` | Path to derived CSV directory |
| `--output-dir` | No | `./results` | Path to output directory |
| `--skip-load` | No | false | Skip data loading (use existing database) |
| `--force` | No | false | Force overwrite existing database |
| `--duration` | No | 60 | Throughput test duration in seconds |
| `--clients` | No | 4 | Number of concurrent clients |

## Expected Results

On Apple Silicon Mac (M1/M2/M3), with 4 concurrent clients for 300 seconds:

### Throughput Comparison

| Engine | QPS | P50 Latency | P95 Latency |
|--------|-----|-------------|-------------|
| NeuG | ~617 | 3.1 ms | 20.6 ms |
| Neo4j | ~12 | 16.0 ms | 1,728 ms |

NeuG achieves **50x** the throughput of Neo4j.

## Query Descriptions

| Query | Description |
|-------|-------------|
| IC1 | Friends with specific first name (shortest path) |
| IC2 | Recent messages from friends |
| IC7 | Recent likes on messages |
| IC8 | Recent replies on messages |
| IC13 | Shortest path between two persons |

## Starting Neo4j

To compare with Neo4j, you need a running Neo4j server:

```bash
# Using Docker
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/neo4j123 \
  neo4j:latest
```

## References

- [LDBC SNB Interactive](https://ldbcouncil.org/benchmarks/snb-interactive/)
- [NeuG Documentation](https://github.com/alibaba/neug)
- [Neo4j](https://neo4j.com/)