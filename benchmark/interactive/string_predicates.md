# String predicate benchmark

This manual benchmark covers the performance part of [issue #1259](https://github.com/alibaba/neug/issues/1259).
It measures complete embedded queries, including result consumption:

```cypher
MATCH (n:Text80) WHERE n.text CONTAINS 'rhprobe' RETURN count(*);
```

Run from the repository root with a built Python binding and its matching Python
environment. Use the same build configuration and benchmark arguments for both
implementations:

```sh
PYTHONPATH=tools/python_bind python benchmark/interactive/string_predicates.py \
  --label baseline --output /tmp/string-predicates-baseline.json
# Run in a separate process against the fixed build:
PYTHONPATH=tools/python_bind python benchmark/interactive/string_predicates.py \
  --label fixed --output /tmp/string-predicates-fixed.json
```

Defaults: 170,000 rows for 7- and 80-byte strings; 10,000 rows for 1,100- and
4,600-byte strings. Each length has matching and nonmatching `CONTAINS`,
`STARTS WITH`, and `ENDS WITH` queries. Additional cases cover non-primary-key
INT64/STRING equality, a pattern property, and a parameter pattern. Explicit
`VARCHAR(N)` widths and a round-trip assertion prevent silent truncation of
long strings. All matching queries return every row; nonmatching queries return
zero rows. Every execution's count is checked.

`CONTAINS` also measures a late hit and two kinds of misses. The ordinary miss
uses `missing`, whose first byte never occurs in the text; `absent` shares the
first byte with the repeated `a` body and forces many candidate comparisons.
The matching `rhprobe` appears at both ends, so the ordinary `CONTAINS` hit is
an early hit. The late-hit pattern is ` rhprobe` for longer strings and `probe`
for the 7-byte string. All 36 cases are reported, including less favorable ones.

Each query is profiled once to record operator names, then receives one ordinary
warmup and five timed executions. Every sweep uses a deterministic shuffled
query order. CSV generation, bulk load, profiling and warmup are excluded from
timings. JSON output includes raw samples, medians, ranges, queries, counts,
operator names, platform, revision and the predicate source's SHA-256.

For the before/after comparison, all code and build settings were identical
except `src/compiler/function/vector_string_functions.cpp`: the baseline used
that file from `25f68ded`, and the fixed run used it from `459ce54c`. Both were
built sequentially in the same checkout with the fixed local Release profile.
The datasets and query order were identical across the separate processes.

The data deliberately contains no line terminators, so the baseline and fixed
implementations must produce identical answers. Multiline correctness is covered
separately by `tools/python_bind/tests/test_string_predicates.py`. These are
embedded-mode wall-clock measurements on one machine, not service-mode results
or a portable performance guarantee. The equality cases help identify general
machine-speed drift between runs.

## Measurement environment

Measured on 2026-11-26: Apple M6, 12 physical CPU cores, 16 GiB memory,
macOS 27.0 arm64, Python 3.12.14. Both builds used Apple Clang, Release/Ninja,
with Python enabled and native CPU tuning, LTO and HTTP server disabled.
The expanded 36-case comparison ran the fixed implementation first, then the
baseline. The earlier 28-case exploratory comparison used the opposite order
and showed similar improvements. Results below use only the expanded run.

## Results

Median query latency in milliseconds; speedup is baseline / fixed. Raw five-sample
measurements are in [string_predicates_results.csv](string_predicates_results.csv).
All queries used `ScanWithGPredOpr → GroupByOpr → ProjectOpr → SinkOpr` in both builds.

| Case | Rows | Baseline ms | Fixed ms | Speedup |
| --- | ---: | ---: | ---: | ---: |
| 7/CONTAINS/hit | 170,000 | 94.114 | 6.804 | 13.83x |
| 7/CONTAINS/miss | 170,000 | 80.606 | 5.503 | 14.65x |
| 7/CONTAINS/late hit | 170,000 | 88.242 | 6.686 | 13.20x |
| 7/CONTAINS/fast miss | 170,000 | 82.650 | 5.499 | 15.03x |
| 7/STARTS WITH/hit | 170,000 | 46.260 | 6.681 | 6.92x |
| 7/STARTS WITH/miss | 170,000 | 37.122 | 5.578 | 6.66x |
| 7/ENDS WITH/hit | 170,000 | 85.782 | 6.383 | 13.44x |
| 7/ENDS WITH/miss | 170,000 | 76.210 | 5.476 | 13.92x |
| 80/CONTAINS/hit | 170,000 | 375.463 | 9.416 | 39.88x |
| 80/CONTAINS/miss | 170,000 | 369.357 | 58.732 | 6.29x |
| 80/CONTAINS/late hit | 170,000 | 372.949 | 9.620 | 38.77x |
| 80/CONTAINS/fast miss | 170,000 | 363.132 | 7.865 | 46.17x |
| 80/STARTS WITH/hit | 170,000 | 297.622 | 9.215 | 32.30x |
| 80/STARTS WITH/miss | 170,000 | 41.248 | 7.831 | 5.27x |
| 80/ENDS WITH/hit | 170,000 | 328.120 | 8.767 | 37.43x |
| 80/ENDS WITH/miss | 170,000 | 359.186 | 7.869 | 45.65x |
| 1100/CONTAINS/hit | 10,000 | 238.398 | 0.809 | 294.67x |
| 1100/CONTAINS/miss | 10,000 | 272.123 | 49.393 | 5.51x |
| 1100/CONTAINS/late hit | 10,000 | 239.457 | 0.982 | 243.84x |
| 1100/CONTAINS/fast miss | 10,000 | 260.803 | 0.872 | 299.19x |
| 1100/STARTS WITH/hit | 10,000 | 228.281 | 0.768 | 297.06x |
| 1100/STARTS WITH/miss | 10,000 | 2.834 | 0.737 | 3.84x |
| 1100/ENDS WITH/hit | 10,000 | 230.000 | 0.812 | 283.11x |
| 1100/ENDS WITH/miss | 10,000 | 264.837 | 0.712 | 372.12x |
| 4600/CONTAINS/hit | 10,000 | 977.760 | 1.463 | 668.29x |
| 4600/CONTAINS/miss | 10,000 | 1107.549 | 205.898 | 5.38x |
| 4600/CONTAINS/late hit | 10,000 | 990.486 | 2.315 | 427.81x |
| 4600/CONTAINS/fast miss | 10,000 | 1073.895 | 2.169 | 495.20x |
| 4600/STARTS WITH/hit | 10,000 | 941.332 | 1.453 | 647.89x |
| 4600/STARTS WITH/miss | 10,000 | 3.824 | 1.369 | 2.79x |
| 4600/ENDS WITH/hit | 10,000 | 948.976 | 1.444 | 657.38x |
| 4600/ENDS WITH/miss | 10,000 | 1081.501 | 1.371 | 788.98x |
| INT64 equality | 170,000 | 2.492 | 2.551 | 0.98x |
| STRING equality | 170,000 | 4.778 | 4.883 | 0.98x |
| column CONTAINS | 170,000 | 376.313 | 11.225 | 33.52x |
| parameter CONTAINS | 170,000 | 371.573 | 9.147 | 40.62x |

At 170,000 rows, matching 80-byte predicates improved 32.3–39.9x. The
7-byte matching predicates improved 6.9–13.8x. The equality controls differed
by about 2%, while column and parameter patterns improved 33.5x and 40.6x.

Search content still matters: the repeated-first-byte `CONTAINS` miss improved
5.4–6.3x for 80–4,600-byte inputs, rather than the much larger matching-case
speedups. Late hits also improved substantially; the result is not limited to
finding a pattern at the beginning. No case retains per-row regex compilation.

Predicate source SHA-256:

- Baseline: `7018796842112be440010e0941af9742c38bd450b8a269db1a378d4ad86001d0`
- Fixed: `4942b1a9042c2030ccb6bbdd441c09bf8b06b7538bf63035bc701d5168028bfe`
