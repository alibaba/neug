# Pattern Match Extension

The Pattern Match extension provides subgraph pattern matching over the current NeuG graph. The extension is loaded as `pattern_matching` and exposes a single, unified entry point, `PATTERN_MATCH`, that runs either exact or sampled matching depending on its arguments:

| Call | Algorithm | Use case |
| --- | --- | --- |
| `PATTERN_MATCH(pattern_text_or_file)` | Exact matching backed by DAF, validated against NeuG storage | Enumerate **all** exact embeddings |
| `PATTERN_MATCH(pattern_text_or_file, size, false)` | Exact matching (DAF) that **early-terminates** after `size` matches | Only a few matches are needed and full enumeration should be avoided |
| `PATTERN_MATCH(pattern_text_or_file, size, true)` | Sampled matching backed by FaSTest | Return representative sampled embeddings when exact enumeration is too expensive |

The second and third arguments go together:

- `size` is a positive integer (`>= 1`). In exact mode it is the early-termination bound; in sampled mode it is the sample size. A `size` of `0` or a negative value is rejected at bind time.
- `is_sampled` is a boolean literal that chooses the algorithm: `false` → exact (with early termination), `true` → sampled. It must be written as `true` / `false`; integer `0` / `1` is **not** accepted.

> The old separate `SAMPLED_PATTERN_MATCH` function and the two-argument `PATTERN_MATCH(pattern, limit)` form have been removed; both are now expressed through the unified call above.

All forms accept the same pattern input and return the same native `QueryResult` shape. Pattern vertex variables are returned as `Vertex` columns, relationship variables are returned as `Edge` columns, and column names come from the aliases in the pattern. Anonymous pattern elements use deterministic fallback names such as `v0`, `e0`, and `v1`.

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

## Pattern Input

`PATTERN_MATCH` takes a pattern as the first argument. The argument can be:

| Input form | Description |
| --- | --- |
| Inline Cypher pattern text | The common form for interactive queries |
| Path to a Cypher pattern file | Useful for generated or long patterns |
| Inline JSON pattern text | Legacy/internal pattern format |
| Path to a JSON pattern file | Legacy/internal pattern format stored on disk |

Cypher input is parsed with NeuG's Cypher parser, then translated into the JSON pattern format used by the matching backends.

### The leading `MATCH` keyword is optional

You can pass a bare pattern without the `MATCH` keyword — `PATTERN_MATCH` prepends one automatically. This avoids the awkward `pattern_match('MATCH ...')` repetition:

```cypher
-- these two are equivalent
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;
```

A bare pattern may still carry an inline `WHERE` / `RETURN`:

```cypher
CALL PATTERN_MATCH(
  '(a:Person {age: 20})-[r:person_knows_person]->(b:Person) WHERE r.weight >= 0.5'
) RETURN *;
```

The explicit `MATCH ...` form continues to work unchanged.

### The Cypher argument is a pattern expression only

The Cypher string passed to `PATTERN_MATCH` is **not** a full Cypher query — it only describes a single, explicit graph pattern. Every node and relationship must be spelled out concretely. In particular, **variable-length / recursive relationships are not supported**:

```cypher
-- NOT supported: variable-length path with a hop count or range
MATCH (a:A)-[p:Path*3]-()
MATCH (a:A)-[p:Path*1..3]->(b:B)
MATCH (a:A)-[*]->(b:B)
```

Write the path out explicitly, one relationship at a time, instead:

```cypher
-- supported: the 3-hop path spelled out explicitly
MATCH (a:A)-[p1:Path]->(x1)-[p2:Path]->(x2)-[p3:Path]->(b)
```

### Supported Cypher Subset

Pattern Match intentionally supports a limited, pattern-only Cypher subset:

- One `MATCH` clause.
- Fixed-length, explicitly written relationships only (no `*`, `*n`, or `*m..n` variable-length ranges).
- Directed relationships using `->` or `<-`.
- One node label per vertex and one relationship type per edge.
- Inline property maps with literal values.
- `WHERE` filters combined with `AND`.
- Predicate shape `var.property OP literal`, where `OP` is `=`, `>`, `>=`, `<`, or `<=`.
- Optional `RETURN *`, pattern variables, or `var.property`.
- `ORDER BY var.property [ASC|DESC]`.
- `SKIP` and `LIMIT` with non-negative integer literals.

The `RETURN` clause inside the pattern input is accepted for pattern validation and property requirements. The outer table function still returns native vertex and edge columns for the matched pattern variables.

### Unsupported Constructs

The bind step rejects unsupported Cypher instead of silently changing semantics:

| Construct | Status |
| --- | --- |
| `OPTIONAL MATCH`, `WITH`, `UNION`, `CREATE`, mutation clauses | Not supported |
| Undirected relationships such as `(a)-[r]-(b)` | Not supported |
| Variable-length / recursive relationships such as `-[:R*1..3]->` or `-[*]->` | Not supported |
| Multi-label nodes such as `(a:A:B)` | Not supported |
| Multi-type relationships such as `[:A|:B]` | Not supported |
| `OR`, `NOT`, `XOR` in `WHERE` | Not supported |
| Cross-variable comparisons such as `a.age = b.age` | Not supported |
| Computed projections such as `RETURN a.age + b.age` | Not supported |
| Computed `ORDER BY`, `SKIP`, or `LIMIT` expressions | Not supported |

## Exact Matching

Call `PATTERN_MATCH` with only the pattern to enumerate **all** exact embeddings (the leading `MATCH` keyword is optional):

```cypher
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN *;
```

Example output shape:

| a | r | b |
| --- | --- | --- |
| `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` |

To stop after the first `size` matches (early termination), pass `size` with `is_sampled = false`:

```cypher
-- exact matching, stop after the first 10 matches
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r:person_knows_person]->(b:Person)',
  10, false
)
RETURN *;
```

Inline properties and `WHERE` predicates can restrict candidates:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person {age: 20})-[r:person_knows_person]->(b:Person)
   WHERE r.weight >= 0.5
   RETURN a, r, b'
)
RETURN *;
```

## Sampled Matching

Pass `size` with `is_sampled = true` when exact enumeration may be too expensive and sampled embeddings are sufficient:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r:person_knows_person]->(b:Person)',
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
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
RETURN a.name AS src, r.weight AS weight
ORDER BY r.weight DESC
LIMIT 10;

-- aggregate over the matches
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
RETURN count(a) AS matches, count(DISTINCT a.name) AS distinct_sources;
```

## YIELD

`PATTERN_MATCH` supports an optional `YIELD` clause between the call and the trailing `RETURN`. `YIELD` lists the pattern variables to expose to the rest of the query and may rename them with `AS`:

```cypher
-- expose all matched variables (equivalent to omitting YIELD)
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a, r, b
RETURN a.name, r.weight, b.name;

-- rename matched variables
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a AS src, b AS dst
RETURN src.name, dst.name;

-- expose only a subset; unlisted variables are hidden from RETURN
CALL PATTERN_MATCH('MATCH (a:Person)-[r:person_knows_person]->(b:Person)')
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

## File Input

For longer patterns, store the Cypher pattern in a file and pass its path:

```bash
mkdir -p /tmp/pattern_match
cat > /tmp/pattern_match/triangle.cypher <<'EOF'
MATCH (a:Person)-[r1:person_knows_person]->
      (b:Person)-[r2:person_knows_person]->
      (c:Person)-[r3:person_knows_person]->(a:Person)
WHERE a.age >= 18
RETURN a, r1, b, r2, c, r3
EOF
```

```cypher
-- exact over all matches
CALL PATTERN_MATCH('/tmp/pattern_match/triangle.cypher')
RETURN *;

-- sampled, sample size 1000
CALL PATTERN_MATCH('/tmp/pattern_match/triangle.cypher', 1000, true)
RETURN *;
```

## Cache and Support Functions

`INITIALIZE` builds the graph metadata cache used by Pattern Match. Matching calls initialize the cache lazily, but explicit initialization is useful before running a batch of patterns.

```cypher
CALL INITIALIZE()
RETURN status, num_vertices, num_edges, max_degree, degeneracy;
```

You can persist and reload the cache for faster restarts:

```cypher
CALL SAVE_SAMPLEDMATCH_CHECKPOINT('/tmp/pattern_match_checkpoint')
RETURN status, checkpoint_dir;

CALL INITIALIZE('/tmp/pattern_match_checkpoint')
RETURN status, num_vertices, num_edges, max_degree, degeneracy;
```

The checkpoint function keeps its current `SAVE_SAMPLEDMATCH_CHECKPOINT` name for compatibility with the existing implementation.

The extension also includes property helper functions for workflows that need CSV files:

| Function | Output |
| --- | --- |
| `GET_VERTEX_PROPERTY(vertex_ids_json, vertex_label, prop_names_json)` | One `result_file` column pointing to a generated CSV |
| `GET_EDGE_PROPERTY(edge_keys_json, edge_label, prop_names_json)` | One `result_file` column pointing to a generated CSV |

## End-to-end Example

Run the snippets against a database with `Person` vertices and `person_knows_person` edges:

```cypher
LOAD pattern_matching;

-- exact: enumerate all matches
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r1:person_knows_person]->
        (b:Person)-[r2:person_knows_person]->
        (c:Person)-[r3:person_knows_person]->(a:Person)
   WHERE a.age >= 18
   RETURN a, r1, b, r2, c, r3'
)
RETURN *;
```

To switch to the sampled algorithm, keep the same pattern and add a sample size with `is_sampled = true`:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r1:person_knows_person]->
        (b:Person)-[r2:person_knows_person]->
        (c:Person)-[r3:person_knows_person]->(a:Person)
   WHERE a.age >= 18
   RETURN a, r1, b, r2, c, r3',
  1000, true
)
RETURN *;
```

For exact matching that stops early once a handful of embeddings are found, use `is_sampled = false`:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r1:person_knows_person]->
        (b:Person)-[r2:person_knows_person]->
        (c:Person)-[r3:person_knows_person]->(a:Person)
   WHERE a.age >= 18
   RETURN a, r1, b, r2, c, r3',
  5, false
)
RETURN *;
```
