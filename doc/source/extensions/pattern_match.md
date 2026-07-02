# Pattern Match Extension

Since NeuG **v0.1.4**, we have introduced the Pattern Match extension, which provides subgraph pattern matching over the current NeuG graph.


```cypher
CALL PATTERN_MATCH(Pattern, size, is_sampled)
[YIELD ...]
RETURN *;
```

- **`Pattern`** — the graph pattern to match, e.g. `'(a:Person)-[r:person_knows_person]->(b:Person)'`. It uses Cypher node/relationship syntax. It is a pattern only, not a full query: every node and relationship must be written out explicitly, though an inline `WHERE` is allowed (for node/relationship property filters).
- **`size`** *(optional)* — a positive integer (`>= 1`). In exact mode it is the early-termination bound (stop after the first `size` matches); in sampled mode it is the sample size.
- **`is_sampled`** *(optional)* — a boolean choosing the algorithm: `false` → exact matching, `true` → sampled matching (FaSTest). Must be written as `true` / `false` (not `0` / `1`).

`size` and `is_sampled` go together. Omit both for plain exact matching over all matches:

```cypher
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;
```

## Supported Patterns

**Supported:**

- Directed relationships: `->` and `<-`
- One label per node, one type per relationship
- Multi-hop paths: `(a)-[r1]->(b)-[r2]->(c)` (write each hop explicitly)
- Cycles: `(a)-[r1]->(b)-[r2]->(c)-[r3]->(a)`
- Inline property maps with literal values: `(a:Person {age: 20})`
- Inline `WHERE` for property-based filtering: `(a:Person)-[r:knows]->(b:Person) WHERE a.age > 25`

**Examples:**

```cypher
-- Reverse direction
CALL PATTERN_MATCH('(a:Person)<-[r:person_knows_person]-(b:Person)') RETURN *;

-- Inline property map (only match Person nodes with age = 20)
CALL PATTERN_MATCH('(a:Person {age: 20})-[r:person_knows_person]->(b:Person)') RETURN *;

-- Inline WHERE (only match edges where source age > 25)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person) WHERE a.age > 25') RETURN *;

-- Multi-hop path (2 hops)
CALL PATTERN_MATCH('(a:Person)-[r1:person_knows_person]->(b:Person)-[r2:person_knows_person]->(c:Person)') RETURN *;

-- Triangle cycle (3 hops back to start)
CALL PATTERN_MATCH('(a:Person)-[r1:person_knows_person]->(b:Person)-[r2:person_knows_person]->(c:Person)-[r3:person_knows_person]->(a:Person)') RETURN *;
```

**Not supported (rejected at bind time):**

- Variable-length / recursive relationships: `-[:R*3]->`, `-[:R*1..3]->`, `-[*]->`
- Undirected relationships: `(a)-[r]-(b)`
- Multi-label nodes: `(a:A:B)`
- Multi-type relationships: `[:A|:B]`
- `OPTIONAL MATCH`, `WITH`, `UNION`, mutations
- `OR`, `NOT`, `XOR` in `WHERE`
- Cross-variable comparisons: `a.age = b.age`
- Computed projections, `ORDER BY`, `SKIP`, `LIMIT` inside the pattern

Write a fixed-length path out one relationship at a time instead of using `*`.

## Install Extension

```cypher
INSTALL pattern_matching;
```

## Load Extension

```cypher
LOAD pattern_matching;
```

When building from source, enable the extension with:

```bash
cmake -S . -B build -DBUILD_EXTENSIONS="pattern_matching" -DBUILD_TEST=ON
cmake --build build --target neug_pattern_matching_extension -j$(sysctl -n hw.ncpu)
```

On Linux, use `-j$(nproc)` instead of `-j$(sysctl -n hw.ncpu)`.

## Exact Matching

Call `PATTERN_MATCH` with only the pattern to enumerate **all** exact embeddings:

```cypher
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN *;
```

Output shape — one row per match, vertices as `Vertex` columns and relationships as `Edge` columns, named by the pattern aliases:

| a | r | b |
| --- | --- | --- |
| `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` |

For multi-hop patterns, each hop produces its own column:

| a | r1 | b | r2 | c |
| --- | --- | --- | --- | --- |
| `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` |

To stop after the first `size` matches (early termination), pass `size` with `is_sampled = false`:

```cypher
-- exact matching, stop after the first 10 matches
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)', 10, false)
RETURN *;
```

## Sampled Matching

Pass `size` with `is_sampled = true` when exact enumeration may be too expensive and sampled embeddings are sufficient:

```cypher
CALL PATTERN_MATCH(
  '(a:Person)-[r:person_knows_person]->(b:Person)',
  1000, true
)
RETURN *;
```

Here `size` is the target sample size. The function returns sampled embeddings using the same vertex/edge column layout as the exact mode.

The sampled implementation is based on the FaSTest filtering-sampling algorithm described in "Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach" (VLDB 2024). The implementation computes FaSTest estimates internally, but the public table function returns sampled matches as native `QueryResult` rows rather than `estimated_count`, `result_file`, or `props_file` columns.

## Applying NeuG Operators to the Result

Because the output vertex/edge variables carry the same catalog and property metadata as a `MATCH`-bound node, NeuG's own pipeline clauses can be applied on the trailing `RETURN`: property access, `ORDER BY <var>.<prop>`, `LIMIT`, and aggregates such as `count(<var>)` and `count(DISTINCT <var>.<prop>)`.

```cypher
-- project scalar properties, order and limit
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN a.name AS src, r.weight AS weight
ORDER BY r.weight DESC
LIMIT 10;

-- aggregate over the matches
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN count(a) AS matches, count(DISTINCT a.name) AS distinct_sources;
```

## YIELD

`PATTERN_MATCH` supports an optional `YIELD` clause between the call and the trailing `RETURN`. `YIELD` lists the pattern variables to expose to the rest of the query and may rename them with `AS`:

```cypher
-- expose all matched variables (equivalent to omitting YIELD)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a, r, b
RETURN a.name, r.weight, b.name;

-- rename matched variables
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a AS src, b AS dst
RETURN src.name, dst.name;

-- expose only a subset; unlisted variables are hidden from RETURN
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD b
RETURN b.name;
```

Rules and limitations, consistent with the rest of NeuG's `YIELD`:

- A `YIELD` item is a bare variable name, optionally followed by `AS <alias>`. It refers to a whole pattern variable (a `Vertex` or `Edge`).
- **`YIELD` cannot contain a property access.** `YIELD a.name` is a syntax error; access properties in the trailing `RETURN` (e.g. `YIELD a RETURN a.name`).
- A variable that is not listed in `YIELD` is hidden — referencing it in `RETURN` raises a "not in scope" error.
- Yielding a name that is not a pattern variable raises a bind error.
- `YIELD` must be followed by a `RETURN`; it does not terminate the query on its own.

Ordering by a whole `Vertex`/`Edge` object (for example `ORDER BY a`) is not supported by NeuG; order by a scalar property such as `ORDER BY a.age` instead.

## End-to-end Example

A complete, runnable sequence — create the schema, insert a small graph, load the extension, then match. Every statement can be run as-is in the CLI or via `conn.execute(...)`:

```cypher
-- 1. Schema
CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age INT32);
CREATE REL TABLE person_knows_person(FROM Person TO Person, weight DOUBLE);

-- 2. Data (3 people, a directed triangle of "knows" edges)
CREATE (n:Person {id: 0, name: 'Alice', age: 20});
CREATE (n:Person {id: 1, name: 'Bob',   age: 30});
CREATE (n:Person {id: 2, name: 'Carol', age: 40});
MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1
CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2
CREATE (a)-[:person_knows_person {weight: 1.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0
CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);

-- 3. Load the extension
LOAD pattern_matching;

-- 4. Exact match: all (a)-[r]->(b) embeddings
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;

-- 5. Project / order / limit on the matched output
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN a.name AS src, b.name AS dst, r.weight AS weight
ORDER BY weight DESC LIMIT 5;

-- 6. Aggregate over the matches
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN count(a) AS matches, count(DISTINCT a.name) AS distinct_sources;

-- 7. YIELD: rename and expose only the columns you need
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a AS src, b AS dst
RETURN src.name AS source, dst.name AS destination
ORDER BY source;

-- 8. Sampled matching (sample size 2)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)', 2, true) RETURN *;
```

The same flow as a runnable Python script (using the in-repo bindings) is available at `tools/python_bind/example/pattern_match_demo.py`:

```bash
python3 tools/python_bind/example/pattern_match_demo.py
```

## Using the Locally Built Extension

The build above produces `build/extension/pattern_matching/libpattern_matching.neug_extension`. `LOAD pattern_matching;` resolves to `<home>/extension/pattern_matching/libpattern_matching.neug_extension`, where `<home>` comes from the `NEUG_EXTENSION_HOME_PYENV` environment variable.

When you use the in-repo Python bindings this is automatic: importing `neug` discovers `neug_py_bind` under the build tree and sets `NEUG_EXTENSION_HOME_PYENV` to `<repo>/build` for you, so `LOAD pattern_matching;` picks up the freshly built library with no extra setup:

```python
import neug
from neug.database import Database

db = Database("/tmp/demo_db", "w")
conn = db.connect()
conn.execute("LOAD pattern_matching;")  # loads build/extension/pattern_matching/...
```

If your build tree lives elsewhere, point the loader at it before importing `neug`:

```bash
# directory containing tools/python_bind/neug_py_bind* and extension/
export NEUG_BUILD_DIR=/path/to/neug/build
# or set the extension home directly (the dir that contains extension/):
export NEUG_EXTENSION_HOME_PYENV=/path/to/neug/build
```

## Experiments

We benchmarked three ways of running the **same** subgraph-matching patterns on
the **LDBC SNB scale-factor 1 (SF1)** dataset:

- **NeuG-Native** — NeuG's built-in Cypher engine (`MATCH ... RETURN ...`);
- **Exact-Match** — `CALL PATTERN_MATCH(pattern)` (exact enumeration of all embeddings);
- **Sample-Match** — `CALL PATTERN_MATCH(pattern, 1000000, true)` (sampled matching, sample size **1,000,000**).

The six test patterns (Q1–Q6) are adapted from the LDBC
[LSQB](https://github.com/ldbc/lsqb/tree/main/cypher) query set. Each query runs
with a **10-minute timeout**; a run that exceeds it is marked **OOT** (out of
time).

![Benchmark latency, log scale](../images/benchmark_latency_log.png)

The figure reports end-to-end latency in seconds (log scale). Key observations:

- **NeuG-Native** handles the small patterns (Q1, Q2) but **times out (OOT)** on
  the larger ones (Q3–Q6), whose full result sets are enormous (tens of
  millions to billions of embeddings).
- **Exact-Match** completes every query within the timeout and is one to several
  orders of magnitude faster than the native engine on the large patterns.
- **Sample-Match** keeps latency low and stable across all six patterns and is
  the fastest option on the larger, high-cardinality patterns (Q2–Q6).

## References

The matching algorithms in this extension draw on the methods and ideas of the
following works from the SNU CSE Theory & Algorithms group, which we gratefully
acknowledge:

- **FaSTest** — *Cardinality Estimation of Subgraph Matching: A
  Filtering-Sampling Approach* (VLDB 2024). The **sampled** matching mode is
  based on FaSTest. <https://github.com/SNUCSE-CTA/FaSTest>
- **DAF** — *Efficient Subgraph Matching: Harmonizing Dynamic Programming,
  Adaptive Matching Order, and Failing Set Together* (SIGMOD 2019). The
  **exact** matching design was informed by DAF's approach.
  <https://github.com/SNUCSE-CTA/DAF>
