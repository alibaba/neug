# LSQB Benchmark for NeuG

This directory contains scripts to reproduce the LSQB (Labelled Subgraph Query Benchmark)
performance results from the NeuG blog article.

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

## Quick Start

```bash
# Create virtual environment
python -m venv neug-env
source neug-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the benchmark
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb

# To overwrite an existing database, use --force
python run_benchmark.py --data-dir /path/to/ldbc-snb-sf1-lsqb --force
```

## Command Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--data-dir` | Yes | - | Path to LDBC SNB SF1 dataset directory |
| `--db-path` | No | `./lsqb_sf1.db` | Path to NeuG database |
| `--derived-dir` | No | `./derived_csvs` | Path to derived CSV directory |
| `--output-dir` | No | `./results` | Path to output directory |
| `--skip-load` | No | false | Skip data loading (use existing database) |
| `--force` | No | false | Force overwrite existing database |

## Expected Results

On Apple Silicon Mac (M1/M2/M3), you should see results similar to:

| Query | P50 (ms) | Result |
|-------|----------|--------|
| Q1    | ~2,600   | 179,510,748 |
| Q2    | ~140     | 498,997 |
| Q3    | ~370     | 0 |
| Q4    | ~140     | 16,312,503 |
| Q5    | ~830     | 12,501,170 |
| Q6    | ~480     | 200,468,189 |
| Q7    | ~580     | 26,097,816 |
| Q8    | ~710     | 6,241,640 |
| Q9    | ~600     | 191,485,250 |

## Files

- `run_benchmark.py` - Main benchmark script for NeuG
- `requirements.txt` - Python dependencies
- `README.md` - This file

## Query Descriptions

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

## References

- [LSQB Benchmark](https://github.com/ldbc/lsqb)
- [LDBC SNB](https://ldbcouncil.org/benchmarks/snb/)
- [NeuG Documentation](https://github.com/alibaba/neug)