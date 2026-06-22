# LDBC Graphalytics benchmark driver

`graphalytics_bench.py` times NeuG GDS kernels on [LDBC Graphalytics](http://graphalytics.org/) datasets.

## Timing methodology

- **Kernel only for algorithms**: each GDS `CALL` uses `LIMIT 1 RETURN count(*)` so the full algorithm runs but per-vertex results are not exported (same idea as other platforms that avoid serializing billion-row outputs).
- **Repeated runs**: `--runs N` (default 5) reports the **median** wall time per algorithm.
- **Reuse loaded graph**: `--skip-load` opens an existing `.db` checkpoint and skips `COPY FROM` (load times are reported separately on first import).

Correctness is covered by `tools/python_bind/tests/test_graphalytics.py` on the official small validation graphs.

## Example

```bash
# Build NeuG and install the Python bindings first.
export PYTHONPATH=/path/to/neug/build/tools/python_bind
export NEUG_BUILD_DIR=/path/to/neug/build
export GRAPHALYTICS_DATA_ROOT=/path/to/ldbc-graphalytics/datasets

python3 tools/benchmark/graphalytics_bench.py \
  --dataset graph500-26 \
  --concurrency 64 \
  --runs 5 \
  --skip-load
```

Environment variables:

| Variable | Purpose |
|----------|---------|
| `GRAPHALYTICS_DATA_ROOT` | Default for `--data-root` |
| `NEUG_BENCH_DB_ROOT` | Default for `--db-root` (default `./neug_bench_db`) |
