# LDBC Graphalytics benchmark driver

`graphalytics_bench.py` times NeuG GDS kernels on [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) datasets. See [Performance.md](../Performance.md) for cross-system results.

## Timing methodology

- **Kernel only for algorithms**: each GDS `CALL` uses `LIMIT 1 RETURN count(*)` so the full algorithm runs but per-vertex results are not exported (same idea as other platforms that avoid serializing billion-row outputs).
- **Repeated runs**: `--runs N` (default 5) reports the **median** wall time per algorithm.
- **Reuse loaded graph**: `--skip-load` opens an existing `.db` checkpoint and skips `COPY FROM` (load times are reported separately on first import).

Correctness is covered by `tools/python_bind/tests/test_graphalytics.py` on the official small validation graphs.

## Example

From the NeuG repository root (with GDS extension already built):

```bash
export GRAPHALYTICS_DATA_ROOT=/path/to/ldbc-graphalytics/datasets
export NEUG_BENCH_DB_ROOT=/path/to/neug_bench_db

# First run: COPY FROM + project_graph + time all GDS kernels (median of 5 runs)
python3 extension/gds/benchmark/graphalytics_bench.py \
  --data-root "$GRAPHALYTICS_DATA_ROOT" \
  --db-root "$NEUG_BENCH_DB_ROOT" \
  --dataset graph500-26 \
  --concurrency 64 \
  --runs 5

# Later runs: reuse the .db checkpoint (algorithm kernel time only)
python3 extension/gds/benchmark/graphalytics_bench.py \
  --data-root "$GRAPHALYTICS_DATA_ROOT" \
  --db-root "$NEUG_BENCH_DB_ROOT" \
  --dataset graph500-26 \
  --concurrency 64 \
  --runs 5 \
  --skip-load
```

Run the Graphalytics conformance tests:

```bash
cd tools/python_bind
pytest tests/test_graphalytics.py
```

Environment variables:

| Variable | Purpose |
|----------|---------|
| `GRAPHALYTICS_DATA_ROOT` | Default for `--data-root` |
| `NEUG_BENCH_DB_ROOT` | Default for `--db-root` (default `./neug_bench_db`) |
