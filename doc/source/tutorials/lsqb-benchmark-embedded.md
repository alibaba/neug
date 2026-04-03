# LSQB Benchmark: NeuG vs LadybugDB (Embedded Mode)

This tutorial demonstrates how to reproduce the LSQB (Labelled Subgraph Query Benchmark) performance results comparing NeuG with LadybugDB in embedded mode.

## Overview

[LSQB](https://github.com/ldbc/lsqb) contains 9 complex subgraph matching queries that lean toward analytical workloads, making it an excellent test for embedded graph database query optimization and execution.

### Dataset: LDBC SNB SF1

- **Nodes**: ~3 million (Person, Post, Comment, Tag, etc.)
- **Edges**: ~17 million (KNOWS, LIKES, HASTAG, etc.)
- **Size**: ~282MB compressed, ~1.1GB extracted

## Prerequisites

- Python 3.10+ with `pip`
- NeuG Python package: `pip install neug`
- LadybugDB Python package: `pip install real_ladybug`
- About 1.5GB disk space for the dataset

## Download the Dataset

The LDBC SNB SF1 dataset is available on OSS:

```bash
# Download and extract the dataset
wget https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
tar -xzf ldbc-snb-sf1-lsqb.tar.gz
```

The extracted directory structure:

```
social_network-sf1-CsvComposite-StringDateFormatter/
├── dynamic/
│   ├── person_0_0.csv
│   ├── person_knows_person_0_0.csv
│   ├── comment_0_0.csv
│   ├── post_0_0.csv
│   └── ...
└── static/
    ├── place_0_0.csv
    ├── tag_0_0.csv
    └── ...
```

## LSQB Query Descriptions

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

## Running the Benchmark

The complete benchmark script is available in the NeuG repository at `examples/lsqb_benchmark/run_benchmark.py`.

### Step 1: Create Virtual Environment

```bash
python -m venv neug-env
source neug-env/bin/activate  # Linux/macOS
# neug-env\Scripts\activate  # Windows
```

### Step 2: Install Dependencies

```bash
pip install neug real_ladybug
```

### Step 3: Run the Benchmark

```bash
cd neug/examples/lsqb_benchmark
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb
```

Use `--force` to overwrite an existing database. See [Command Line Options](#command-line-options) for all available flags.

### Command Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--data-dir` | Yes | - | Path to LDBC SNB SF1 dataset directory |
| `--db-path` | No | `./lsqb_sf1.db` | Path to NeuG database |
| `--derived-dir` | No | `./derived_csvs` | Path to derived CSV directory |
| `--output-dir` | No | `./results` | Path to output directory |
| `--skip-load` | No | false | Skip data loading (use existing database) |
| `--force` | No | false | Force overwrite existing database |

## Expected Results

On an Apple Silicon Mac (M1/M2/M3), you should see results similar to:

### NeuG Results (Single-threaded)

| Query | P50 (ms) | Result |
|-------|----------|--------|
| Q1 | ~2,600 | 179,510,748 |
| Q2 | ~140 | 498,997 |
| Q3 | ~370 | 0 (triangles are rare) |
| Q4 | ~140 | 16,312,503 |
| Q5 | ~830 | 12,501,170 |
| Q6 | ~480 | 200,468,189 |
| Q7 | ~580 | 26,097,816 |
| Q8 | ~710 | 6,241,640 |
| Q9 | ~600 | 191,485,250 |

### Comparison with LadybugDB

NeuG wins all 9 queries with just a single thread. On Q3 (triangle pattern within the same country) it achieves a **287x** speedup; on Q2 (two-hop with filtering) a **91x** speedup. Even with LadybugDB using 8 threads, NeuG single-threaded significantly outperforms on all queries.

| Query | NeuG (1 thread) | LadybugDB (best thread) | Speedup |
|-------|-----------------|-------------------------|---------|
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

## Why NeuG is Faster

NeuG's performance advantage comes from:

1. **Graph-native query optimizer**: NeuG uses [GOpt](https://graphscope.io/blog/tech/2024/02/22/GOpt-A-Unified-Graph-Query-Optimization-Framework-in-GraphScope) — a graph-native query optimizer with cost-based optimization and graph-specific cardinality estimation.

2. **Columnar vectorized execution**: Efficient memory management and cache-friendly traversals.

3. **Zero serialization overhead**: In embedded mode, NeuG executes in-process with direct memory access.

## Reproducibility

All performance results are independently reproducible:

- **Dataset**: https://neug.oss-cn-hangzhou.aliyuncs.com/datasets/ldbc-snb-sf1-lsqb.tar.gz
- **Benchmark Scripts**: `neug/examples/lsqb_benchmark/`
- **Tutorial**: `neug/doc/source/tutorials/lsqb-benchmark-embedded.md`

If you encounter any issues reproducing these results, please report them on [GitHub Issues](https://github.com/alibaba/neug/issues).