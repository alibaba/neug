# Unit Tests

This directory includes all unit tests for neug storages and engines.


## VertexTableBenchmark

A microbenchmark for `VertexTable`.

### Benchmark results

On macos m4 pro

```
[==========] Running 5 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 5 tests from VertexTableBenchmark
[ RUN      ] VertexTableBenchmark.AddVertexPerformance
I20250917 09:04:09.096124 9140340 vertex_table_benchmark.cc:176] Added 1000000 vertices in 50705 microseconds
I20250917 09:04:09.096153 9140340 vertex_table_benchmark.cc:178] Average time per vertex: 0.050705 microseconds
[       OK ] VertexTableBenchmark.AddVertexPerformance (52 ms)
[ RUN      ] VertexTableBenchmark.GetOidPerformance
I20250917 09:04:28.808403 9140340 vertex_table_benchmark.cc:219] Deleted 25 thousand vertices
I20250917 09:04:29.099862 9140340 vertex_table_benchmark.cc:230] Performed 25000000 GetOid operations in 295449 microseconds
I20250917 09:04:29.099895 9140340 vertex_table_benchmark.cc:232] Average time per GetOid: 0.011818 microseconds
I20250917 09:04:29.099902 9140340 vertex_table_benchmark.cc:234] After deletion, performed 25000000 GetOid operations in 291411 microseconds
I20250917 09:04:29.099906 9140340 vertex_table_benchmark.cc:237] Average time per GetOid after deletion: 0.0116564 microseconds
[       OK ] VertexTableBenchmark.GetOidPerformance (20208 ms)
[ RUN      ] VertexTableBenchmark.GetIndexPerformance
I20250917 09:04:53.680011 9140340 vertex_table_benchmark.cc:280] Deleted 25 thousand vertices
I20250917 09:04:56.050974 9140340 vertex_table_benchmark.cc:295] Performed 25000000 get_index operations in 2346263 microseconds
I20250917 09:04:56.051012 9140340 vertex_table_benchmark.cc:297] Average time per get_index: 0.0938505 microseconds
I20250917 09:04:56.051033 9140340 vertex_table_benchmark.cc:299] After deletion, performed 25000000 get_index operations in 2370891 microseconds
I20250917 09:04:56.051038 9140340 vertex_table_benchmark.cc:302] Average time per get_index after deletion: 0.0948356 microseconds
[       OK ] VertexTableBenchmark.GetIndexPerformance (26987 ms)
[ RUN      ] VertexTableBenchmark.VertexSetPerformance
I20250917 09:05:14.690007 9140340 vertex_table_benchmark.cc:330] Iterated over 100000000 vertices in 3864 microseconds
I20250917 09:05:14.690032 9140340 vertex_table_benchmark.cc:332] Average time per vertex iteration: 3.864e-05 microseconds
I20250917 09:05:14.690038 9140340 vertex_table_benchmark.cc:334] Dummy result to prevent optimization: 134217727
I20250917 09:05:14.816138 9140340 vertex_table_benchmark.cc:349] After deletion, iterated over 75000000 vertices in 48663 microseconds
I20250917 09:05:14.816160 9140340 vertex_table_benchmark.cc:351] Average time per vertex iteration after deletion: 0.00064884 microseconds
I20250917 09:05:14.816167 9140340 vertex_table_benchmark.cc:354] Dummy result to prevent optimization: 134217727
[       OK ] VertexTableBenchmark.VertexSetPerformance (18662 ms)
[ RUN      ] VertexTableBenchmark.MixedOperationsPerformance
I20250917 09:05:36.661360 9140340 vertex_table_benchmark.cc:404] Performed 25000000 mixed operations in 2116170 microseconds
I20250917 09:05:36.661412 9140340 vertex_table_benchmark.cc:406] Average time per mixed operation: 0.0846468 microseconds
[       OK ] VertexTableBenchmark.MixedOperationsPerformance (21942 ms)
[----------] 5 tests from VertexTableBenchmark (87854 ms total)

[----------] Global test environment tear-down
[==========] 5 tests from 1 test suite ran. (87854 ms total)
[  PASSED  ] 5 tests.
```