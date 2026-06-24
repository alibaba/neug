# pattern_matching Extension

This extension provides two pattern matching interfaces:

- `CALL PATTERN_MATCH(pattern_text_or_file, limit)` for exact matching. It runs
  the DAF integration and validates matches against NeuG's graph storage.
- `CALL SAMPLED_PATTERN_MATCH(pattern_text_or_file, sample_size)` for sampled
  matching based on FaSTest.

Both functions accept inline Cypher pattern text, a Cypher pattern file, inline
JSON, or a JSON pattern file. Cypher input is parsed with NeuG's official Cypher
parser, validated against the subset supported by the matchers, and translated
into the existing JSON pattern format before execution.

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

Anonymous JSON or Cypher patterns use deterministic fallback names such as
`v0`, `e0`, `v1`. Results are not written to JSON files; Python and other tools
should consume the returned `QueryResult` directly.

Supported Cypher input is intentionally narrow:

- one `MATCH` clause with node labels and relationship types;
- directed relationships using `->` or `<-`;
- inline property maps with literal values;
- `WHERE` filters made of `AND`-combined `var.property OP literal`
  comparisons, where `OP` is `=`, `>`, `>=`, `<`, or `<=`;
- optional `RETURN *`, pattern variables, or `var.property`;
- `ORDER BY var.property [ASC|DESC]`;
- `SKIP` and `LIMIT` with non-negative integer literals.

Unsupported expressions such as `OR`, variable-length relationships,
multi-label nodes, multi-type relationships, `WITH`, `UNION`, computed
projections, computed `ORDER BY` expressions, non-literal `SKIP`/`LIMIT`, and
undirected relationships fail during bind with the existing pattern input parse
error path.

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
