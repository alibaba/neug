# Performance

We evaluated performance with the [LDBC Graphalytics Benchmark](https://ldbcouncil.org/benchmarks/graphalytics/) on large and extra-large datasets (graph500-22/26, datagen-9\_*, com-friendster). Datasets follow the [LDBC Graphalytics specification](https://ldbcouncil.org/benchmarks/graphalytics/).

We compared **neug** GDS with [SuiteSparse:GraphBLAS](https://github.com/DrTimothyAldenDavis/GraphBLAS) **v8.0.0** + [LAGraph](https://github.com/GraphBLAS/LAGraph) **LDBC\_2023** via [ldbc\_graphalytics\_platforms\_graphblas](https://github.com/ldbc/ldbc_graphalytics_platforms_graphblas) (4671056), [GeminiGraph](https://github.com/thu-pacman/GeminiGraph) (170e7d3), and [ladybug](https://github.com/LadybugDB/ladybug) **0.18.0** (d5a975c3b). GeminiGraph built with GCC 11.4 and Open MPI 4.0.3; ladybug with GCC 13.1. All systems compiled with **`-O3`** (CMake `Release` or Makefile equivalent).

Minor changes for fair LDBC comparisons:

- GeminiGraph: LDBC CSV converted to 0-based binary edge lists; SSSP weight type `double` (was `float`).
- neug: `extension/gds/benchmark/graphalytics_bench.py` times each GDS `CALL` with `LIMIT 1 RETURN count(*)` to avoid full result export.
- GraphBLAS: PageRank=`LAGr_PageRankGX`, WCC=`LG_CC_FastSV6`; kernel timer excludes load and result I/O.
- ladybug: PR + WCC only; `RETURN count(*)` to avoid full result export.

Times measure **algorithm execution only** (seconds). **`—`** = not implemented. **Bold** = fastest in row.

## Environment

All figures below were measured on one server:

| Item | Configuration |
|------|---------------|
| CPU | AMD EPYC 9T24, **32 cores / 64 threads** (cloud VM) |
| Memory | 495 GB |
| OS | Ubuntu 20.04 |
| Threads | **64** (`nproc`) |
| Toolchain | GCC 11.4 (neug, GeminiGraph, GraphBLAS); GCC 13.1 (ladybug); Open MPI 4.0.3 |
| Optimization | **`-O3`** (`Release`) for all systems |

## Setup

| System | Engine | Algorithms | How timed |
|--------|--------|------------|-----------|
| **neug** | In-memory GDS kernels (CSR views) | BFS, WCC, PR, CDLP, LCC, SSSP | `extension/gds/benchmark/graphalytics_bench.py` per `CALL` |
| **GraphBLAS/LAGraph** | SuiteSparse **v8.0.0** + LAGraph **1.0.1** | same as neug | Kernel timer; PR=`LAGr_PageRankGX`, WCC=`LG_CC_FastSV6` |
| **GeminiGraph** | MPI + OpenMP (`toolkits/bfs`, `pagerank`, `sssp`, `cc`) | BFS, WCC, PR, SSSP | First `exec_time=` in tool output |
| **ladybug** 0.18.0 | Kuzu `algo` extension | PR + WCC only | `lbug` `Time: executing` |

Correctness of neug GDS on the official small Graphalytics validation graphs is covered by `tools/python_bind/tests/test_graphalytics.py`.

## Datasets

| Dataset | Scale | Vertices | Edges | Weighted | Notes |
|---------|-------|----------:|------:|:--------:|-------|
| graph500-26 | large | 32,804,978 | 1,051,922,853 | no | synthetic R-MAT |
| datagen-9_0-fb | large | 12,857,671 | 1,049,527,225 | yes | ~2.5× fewer vertices than graph500-26 at ~1.05B edges |
| datagen-9_1-fb | large | 16,087,483 | 1,342,158,397 | yes | denser than 9_0 (~1.34B edges) |
| datagen-9_2-zf | xlarge | 434,943,376 | 1,042,340,732 | yes | sparse (~2.4 avg degree) |
| com-friendster | xlarge | 65,608,366 | 1,806,067,135 | no | real-world social graph |

All graphs are **undirected**.

---

## graph500-26 (large)

| Algorithm | neug | GraphBLAS | GeminiGraph | ladybug |
|-----------|-----:|----------:|------------:|--------:|
| BFS | **0.542** | 0.608 | 0.595 | — |
| WCC | **0.467** | 0.480 | 2.217 | 555.522 |
| PageRank | 6.047 | 6.015 | **5.140** | 1468.170 |
| CDLP | **10.619** | 38.616 | — | — |
| LCC | **360.452** | 622.956 | — | — |

**neug** leads BFS, WCC, CDLP, and LCC (0.54 s / 0.47 s / 11 s / 360 s vs GraphBLAS 0.61 s / 0.48 s / 39 s / 623 s). PageRank ~6.0 s; GeminiGraph fastest on PR (5.14 s).

---

## datagen-9_0-fb (large, weighted)

| Algorithm | neug | GraphBLAS | GeminiGraph | ladybug |
|-----------|-----:|----------:|------------:|--------:|
| BFS | **0.196** | 0.307 | 0.315 | — |
| WCC | **0.198** | 0.200 | 1.741 | 516.700 |
| PageRank | 4.391 | **3.194** | 4.406 | 1363.222 |
| CDLP | **11.993** | 20.513 | — | — |
| LCC | **21.753** | 65.085 | — | — |
| SSSP | **1.137** | 6.474 | 1.242 | — |

Similar edge count to graph500-26 (~1.05B) but **~2.5× fewer vertices**.

**neug** fastest on BFS, WCC, and SSSP (0.20 s / 0.20 s / 1.14 s); well ahead on CDLP/LCC. PageRank roughly tracks GraphBLAS.

---

## datagen-9_1-fb (large, weighted)

| Algorithm | neug | GraphBLAS | GeminiGraph | ladybug |
|-----------|-----:|----------:|------------:|--------:|
| BFS | **0.268** | 0.365 | 0.362 | — |
| WCC | **0.254** | 0.275 | 2.295 | 675.243 |
| PageRank | 5.761 | **4.281** | 5.659 | 1877.338 |
| CDLP | **16.008** | 25.973 | — | — |
| LCC | **28.581** | 83.572 | — | — |
| SSSP | **1.460** | 8.594 | 1.554 | — |

**neug** wins BFS, WCC, CDLP, LCC, and SSSP (0.25 s / 0.25 s / 16 s / 29 s / 1.46 s). PageRank ~5.8 s; GraphBLAS slightly faster on PR.

---

## datagen-9_2-zf (xlarge, weighted, sparse)

| Algorithm | neug | GraphBLAS | GeminiGraph | ladybug |
|-----------|-----:|----------:|------------:|--------:|
| BFS | 4.855 | **2.262** | 4.818 | — |
| WCC | 4.898 | **4.435** | 15.432 | 965.511 |
| PageRank | 8.861 | **7.227** | 19.313 | 2234.142 |
| CDLP | **19.110** | 315.556 | — | — |
| LCC | **11.047** | 16.369 | — | — |
| SSSP | **7.356** | 18.531 | 12.220 | — |

Very high vertex count but low average degree (~2.4).

**neug** dominates CDLP (19 s vs GraphBLAS 316 s) and leads LCC/SSSP (11 s / 7.4 s). WCC ~4.9 s, within ~10% of GraphBLAS (4.4 s). BFS and PageRank trail GraphBLAS.

---

## com-friendster (xlarge, unweighted)

| Algorithm | neug | GraphBLAS | GeminiGraph | ladybug |
|-----------|-----:|----------:|------------:|--------:|
| BFS | **0.840** | 1.580 | 0.993 | — |
| WCC | **0.945** | 1.141 | 8.471 | 991.129 |
| PageRank | 14.830 | 14.760 | **14.521** | 2667.213 |
| CDLP | **23.629** | 79.490 | — | — |
| LCC | **55.568** | 122.007 | — | — |

**neug** leads BFS, WCC, CDLP, and LCC at 1.8B edges (0.84 s / 0.95 s / 24 s / 56 s). PageRank ~14.8 s; GeminiGraph slightly fastest (14.5 s).

---

## Cross-scale observations

- **neug** fastest on **CDLP** and **LCC** at every scale shown; on graph500-26 LCC, 360 s vs GraphBLAS 623 s. **SSSP** fastest on all three weighted datagen datasets.
- **GraphBLAS** leads **BFS** and **PageRank** on datagen-9_2-zf and several other rows; **WCC** within ~10% of neug on 9_2-zf (4.4 s vs 4.9 s).
- **GeminiGraph** **PageRank** within a few percent of neug on graph500-26 and com-friendster; **WCC** lags (**2.2–15.4 s** vs **0.2–4.9 s**).
- **ladybug** implements PR + WCC only; com-friendster **2667 s** / **991 s** respectively.
