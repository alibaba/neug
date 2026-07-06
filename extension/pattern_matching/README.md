# pattern_matching Extension

This extension provides a single, unified subgraph matching interface:

```text
CALL PATTERN_MATCH(pattern_text_or_file)
CALL PATTERN_MATCH(pattern_text_or_file, size, is_sampled)
```

- `CALL PATTERN_MATCH(pattern_text_or_file)` — **exact** matching over **all**
  matches. Enumerates every embedding with a deterministic adjacency-intersection
  matcher run directly on NeuG's in-memory graph.
- `CALL PATTERN_MATCH(pattern_text_or_file, size, is_sampled)` — a bounded call
  whose algorithm is chosen by the boolean `is_sampled` flag:
  - `is_sampled = false` → **exact** matching that **early-terminates**
    after the first `size` matches are found. Useful when you only need a few
    matches and want to avoid full enumeration.
  - `is_sampled = true` → **sampled** matching (FaSTest) with sample size
    `size`.

  `size` must be a positive integer (`>= 1`) in both modes; `0` or a negative
  value is rejected at bind time. `is_sampled` is a boolean literal — use
  `true` / `false` (it does **not** accept integer `0` / `1`).

The function accepts inline Cypher pattern text, a Cypher pattern file, inline
JSON, or a JSON pattern file. Cypher input is parsed with NeuG's official Cypher
parser, validated against the subset supported by the matchers, and translated
into the existing JSON pattern format before execution.

`PATTERN_MATCH` returns NeuG native `QueryResult` columns. Pattern vertex
variables are emitted as `Vertex` columns and relationship variables are emitted
as `Edge` columns. Column names come from pattern aliases:

The leading `MATCH` keyword is optional — a bare pattern is accepted and a
`MATCH` is prepended automatically (the explicit `MATCH ...` form still works):

```cypher
-- exact matching over all matches (bare pattern, no MATCH keyword)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN *;

-- exact matching, stop after the first 10 matches (early termination)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)',
                   10, false)
RETURN *;

-- sampled matching with a sample size of 10
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)',
                   10, true)
RETURN *;

-- the explicit MATCH form is still accepted
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
RETURN *;
```

returns columns:

```text
a | r | b
```

Because the output vertex/edge variables carry the same catalog and property
metadata as a `MATCH`-bound node, NeuG's own pipeline clauses can be applied to
the result. Property access, `ORDER BY <var>.<prop>`, `LIMIT`, and aggregates
such as `count(<var>)` all work on the trailing `RETURN`:

```cypher
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
RETURN a.name, r.weight ORDER BY a.age DESC LIMIT 10;
```

Note that `ORDER BY a` over a whole `Vertex`/`Edge` object is not supported by
NeuG (order it by a scalar property such as `ORDER BY a.age` instead).

An optional `YIELD` clause between the call and `RETURN` selects/renames the
pattern variables to expose, e.g. `CALL PATTERN_MATCH('...') YIELD a AS src, b
RETURN src.name, b.name`. `YIELD` items are bare variable names with an optional
`AS <alias>`; a property access such as `YIELD a.name` is a syntax error
(access properties in the trailing `RETURN`), and variables not listed in
`YIELD` are hidden from `RETURN`.

Anonymous JSON or Cypher patterns use deterministic fallback names such as
`v0`, `e0`, `v1`. Results are not written to JSON files; Python and other tools
should consume the returned `QueryResult` directly.

## Cypher input is a pattern expression only

The Cypher string passed to `PATTERN_MATCH("...")` is **not** a full Cypher
query — it only describes a single, explicit graph pattern. Every node and
relationship must be spelled out concretely.

In particular, **variable-length / recursive relationships are not supported**.
Patterns such as the following are rejected:

```cypher
-- NOT supported: variable-length path with a hop count or range
MATCH (a:A)-[p:Path*3]-()
MATCH (a:A)-[p:Path*1..3]->(b:B)
MATCH (a:A)-[*]->(b:B)
```

You must instead write the path out explicitly, one relationship at a time:

```cypher
-- supported: the 3-hop path spelled out explicitly
MATCH (a:A)-[p1:Path]->(x1)-[p2:Path]->(x2)-[p3:Path]->(b)
```

Supported Cypher input is intentionally limited:

- one `MATCH` clause with node labels and relationship types;
- fixed-length, explicitly written relationships only (no `*`, no `*n`, no
  `*m..n` variable-length ranges);
- directed relationships using `->` or `<-`;
- inline property maps with literal values;
- `WHERE` filters made of `AND`-combined `var.property OP literal`
  comparisons, where `OP` is `=`, `>`, `>=`, `<`, or `<=`;
- optional `RETURN *`, pattern variables, or `var.property`;
- `ORDER BY var.property [ASC|DESC]`;
- `SKIP` and `LIMIT` with non-negative integer literals.

Unsupported expressions such as `OR`, variable-length / recursive relationships
(`*`, `*n`, `*m..n`), multi-label nodes, multi-type relationships, `WITH`,
`UNION`, computed projections, computed `ORDER BY` expressions, non-literal
`SKIP`/`LIMIT`, and undirected relationships fail during bind with the existing
pattern input parse error path.

## References

The matching algorithms in this extension draw on the methods and ideas of the
following works from the SNU CSE Theory & Algorithms group. We gratefully
acknowledge them:

- **FaSTest** — *Cardinality Estimation of Subgraph Matching: A
  Filtering-Sampling Approach* (VLDB 2024). The **sampled** matching mode is
  based on FaSTest (its core is vendored under `include/fastest_lib/`).
  <https://github.com/SNUCSE-CTA/FaSTest>
- **DAF** — *Efficient Subgraph Matching: Harmonizing Dynamic Programming,
  Adaptive Matching Order, and Failing Set Together* (SIGMOD 2019). The
  **exact** matching design was informed by DAF's approach.
  <https://github.com/SNUCSE-CTA/DAF>

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
