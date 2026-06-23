# pattern_matching Extension

This extension provides two pattern matching interfaces:

- `CALL PATTERN_MATCH(pattern_text_or_file, limit)` for exact matching. It runs
  the DAF integration and validates matches against NeuG's graph storage.
- `CALL SAMPLED_PATTERN_MATCH(pattern_text_or_file, sample_size)` for sampled
  matching based on FaSTest.

Both functions accept inline mini-Cypher, a mini-Cypher file, inline JSON, or a
JSON pattern file. Mini-Cypher input is translated by the extension parser into
the existing JSON pattern format before execution.

Both match functions return NeuG native `QueryResult` columns. Pattern vertex
variables are emitted as `Vertex` columns and relationship variables are emitted
as `Edge` columns. Column names come from pattern aliases:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r:person_knows_person]->(b:Person)',
  10
) RETURN *;
```

returns columns:

```text
a | r | b
```

Anonymous JSON or mini-Cypher patterns use deterministic fallback names such as
`v0`, `e0`, `v1`. Results are not written to JSON files; Python and other tools
should consume the returned `QueryResult` directly.

The sampled implementation is based on the FaSTest algorithm described in:

**Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach** (VLDB 2024)

See the original project note in `include/fastest_lib/README.md`:
- `Fastest [VLDB 2024]`
- `Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach`

## Build Test

From your build directory, compile tests with:

```sh
mkdir build && cd build
cmake .. -DBUILD_EXTENSIONS="pattern_matching" -DBUILD_TEST=ON
cmake --build . --target pattern_matching_extension_test -j$(sysctl -n hw.ncpu)
ctest -R pattern_matching_extension_test --output-on-failure
```

Each `TEST_F` runs under a unique `mkdtemp` scratch directory, so the suite
is safe to run in parallel (e.g. `ctest -j`).
