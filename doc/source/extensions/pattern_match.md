# Pattern Match Extension

The Pattern Match extension provides subgraph pattern matching over the current NeuG graph. The extension is loaded as `pattern_matching` and currently exposes two matching algorithms:

| Function | Algorithm | Use case |
| --- | --- | --- |
| `PATTERN_MATCH(pattern_text_or_file, limit)` | Exact matching backed by DAF, with matches validated against NeuG storage | Enumerate exact embeddings up to `limit` |
| `SAMPLED_PATTERN_MATCH(pattern_text_or_file, sample_size)` | Sampled matching backed by FaSTest | Return representative sampled embeddings when exact enumeration is too expensive |

`SAMPLED_PATTERN_MATCH` is one algorithm inside Pattern Match. It is no longer the name of the whole extension.

Both matching functions accept the same pattern input forms and return the same native `QueryResult` shape. Pattern vertex variables are returned as `Vertex` columns, relationship variables are returned as `Edge` columns, and column names come from the aliases in the pattern. Anonymous pattern elements use deterministic fallback names such as `v0`, `e0`, and `v1`.

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

Both match functions take a pattern as the first argument. The argument can be:

| Input form | Description |
| --- | --- |
| Inline Cypher pattern text | The common form for interactive queries |
| Path to a Cypher pattern file | Useful for generated or long patterns |
| Inline JSON pattern text | Legacy/internal pattern format |
| Path to a JSON pattern file | Legacy/internal pattern format stored on disk |

Cypher input is parsed with NeuG's Cypher parser, then translated into the JSON pattern format used by the matching backends.

### Supported Cypher Subset

Pattern Match intentionally supports a narrow, pattern-only Cypher subset:

- One `MATCH` clause.
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
| Variable-length relationships such as `-[:R*1..3]->` | Not supported |
| Multi-label nodes such as `(a:A:B)` | Not supported |
| Multi-type relationships such as `[:A|:B]` | Not supported |
| `OR`, `NOT`, `XOR` in `WHERE` | Not supported |
| Cross-variable comparisons such as `a.age = b.age` | Not supported |
| Computed projections such as `RETURN a.age + b.age` | Not supported |
| Computed `ORDER BY`, `SKIP`, or `LIMIT` expressions | Not supported |

## Exact Matching

Use `PATTERN_MATCH` when you need exact embeddings.

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r:person_knows_person]->(b:Person)',
  10
)
RETURN *;
```

Example output shape:

| a | r | b |
| --- | --- | --- |
| `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` |

The second argument is an upper bound on rows returned by the exact matcher.

Inline properties and `WHERE` predicates can restrict candidates:

```cypher
CALL PATTERN_MATCH(
  'MATCH (a:Person {age: 20})-[r:person_knows_person]->(b:Person)
   WHERE r.weight >= 0.5
   RETURN a, r, b',
  10
)
RETURN *;
```

## Sampled Matching

Use `SAMPLED_PATTERN_MATCH` when exact enumeration may be too expensive and sampled embeddings are sufficient.

```cypher
CALL SAMPLED_PATTERN_MATCH(
  'MATCH (a:Person)-[r:person_knows_person]->(b:Person)',
  1000
)
RETURN *;
```

The second argument is the target sample size. The function returns sampled embeddings using the same vertex/edge column layout as `PATTERN_MATCH`.

The sampled implementation is based on the FaSTest filtering-sampling algorithm described in "Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach" (VLDB 2024). The implementation computes FaSTest estimates internally, but the public table function currently returns sampled matches as native `QueryResult` rows rather than `estimated_count`, `result_file`, or `props_file` columns.

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
CALL PATTERN_MATCH('/tmp/pattern_match/triangle.cypher', 1000)
RETURN *;
```

The same file can be passed to `SAMPLED_PATTERN_MATCH`.

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

CALL PATTERN_MATCH(
  'MATCH (a:Person)-[r1:person_knows_person]->
        (b:Person)-[r2:person_knows_person]->
        (c:Person)-[r3:person_knows_person]->(a:Person)
   WHERE a.age >= 18
   RETURN a, r1, b, r2, c, r3',
  1000
)
RETURN *;
```

To switch to the sampled algorithm, keep the same pattern and call `SAMPLED_PATTERN_MATCH`:

```cypher
CALL SAMPLED_PATTERN_MATCH(
  'MATCH (a:Person)-[r1:person_knows_person]->
        (b:Person)-[r2:person_knows_person]->
        (c:Person)-[r3:person_knows_person]->(a:Person)
   WHERE a.age >= 18
   RETURN a, r1, b, r2, c, r3',
  1000
)
RETURN *;
```
