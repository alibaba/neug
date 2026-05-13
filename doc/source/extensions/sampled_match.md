# Sampled Match Extension

Subgraph matching cardinality estimation is a common building block for query optimization and graph analytics: given a small *pattern graph* `P` and a large *data graph* `G`, estimate how many embeddings of `P` exist in `G` and produce a representative sample of those embeddings. NeuG provides this capability through the `sampled_match` extension, which implements the FaSTest filtering-sampling algorithm described in *Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach* (VLDB 2024).

Patterns are described with a small Cypher-like DSL passed to `SAMPLED_MATCH_PATTERN`. The procedure returns one row with the estimated total count, the sampled embedding count, and the paths of two artifact files (the embeddings CSV and a per-sample properties JSON).

## Install Extension

```cypher
INSTALL SAMPLED_MATCH;
```

## Load Extension

```cypher
LOAD SAMPLED_MATCH;
```

## Pattern DSL

`SAMPLED_MATCH_PATTERN` accepts a pattern-only subset of Cypher. The DSL covers everything the matcher actually supports — nothing more, nothing less — so anything outside it is rejected with a pointed parse error pointing at the offending `line:col`.

### Triangle example

A directed 3-cycle over three `Person` vertices connected by `person_knows_person` edges, with an `age >= 18` filter on the first vertex and the matched names projected out:

```cypher
MATCH (a:Person)-[:person_knows_person]->
      (b:Person)-[:person_knows_person]->
      (c:Person)-[:person_knows_person]->(a)
WHERE a.age >= 18
RETURN a.name, b.name, c.name
```

That's the entire pattern description — no separate vertices/edges arrays, no numeric IDs.

### Supported grammar

```
program     := MATCH pattern ( ',' pattern )*
               ( WHERE predicate ( AND predicate )* )?
               ( RETURN proj ( ',' proj )* )?

pattern     := node ( rel node )*
node        := '(' var? ( ':' label )? prop_map? ')'
rel         := '-' rel_detail? '->'   |   '<-' rel_detail? '-'
rel_detail  := '[' var? ( ':' label )? prop_map? ']'
prop_map    := '{' prop_kv ( ',' prop_kv )* '}'
prop_kv     := IDENT ':' literal

predicate   := var '.' IDENT op literal
op          := '=' | '>' | '>=' | '<' | '<='

proj        := var ( '.' IDENT )?
literal     := INT | FLOAT | STRING | TRUE | FALSE
```

- Keywords (`MATCH`, `WHERE`, `RETURN`, `AND`, `TRUE`, `FALSE`) are case-insensitive.
- Strings can use single or double quotes; standard `\\ \' \" \n \t \r` escapes work.
- Line comments start with `--` and run to end-of-line.

### Inline property maps vs. WHERE

These two forms are equivalent — pick whichever reads better at the call site:

```cypher
-- inline equality on the property map
MATCH (a:Person {age: 30})-[:person_knows_person]->(b:Person)

-- as a WHERE predicate
MATCH (a:Person)-[:person_knows_person]->(b:Person)
WHERE a.age = 30
```

Inline maps only support equality (Cypher convention). Use `WHERE` for `>`, `>=`, `<`, `<=`.

### What the DSL deliberately rejects

The grammar above is intentionally smaller than full Cypher. Each rejection is paired with a clear error message so it's obvious what failed:

| Construct | Why rejected |
| --- | --- |
| Undirected edges `(a)-[:r]-(b)` | The matcher needs a direction. |
| Variable-length paths `-[:r*1..N]->` | Not supported by the sampler. |
| Multi-label nodes `(a:A:B)` | The matcher resolves to one label per vertex. |
| `<>` / `!=` | The matcher has no NOT-EQUALS constraint. |
| `IN` operator | The matcher treats `in` as a runtime no-op — surfacing it from the DSL would be misleading. |
| `OR`, `NOT`, `XOR` in WHERE | The matcher AND-combines constraints; richer combinators would be silently flattened. |
| Cross-variable comparisons `a.age > b.age` | The matcher only filters per-vertex/per-edge. |
| Function calls (`length(a.name)`, etc.) | No expression evaluator on the matcher side. |
| `OPTIONAL MATCH`, `WITH`, `CREATE`, ... | Out of scope; this is a pattern description, not a query. |

### Calling `SAMPLED_MATCH_PATTERN`

`SAMPLED_MATCH_PATTERN` takes two arguments:

| Argument | Type | Description |
| --- | --- | --- |
| `dsl` | string | DSL text, or an absolute path to a `.dsl` file containing it. The procedure auto-detects which form was passed. |
| `sample_size` | int64 | Target embedding count. Larger ⇒ tighter estimate, bigger result file. |

The procedure returns one row with four columns:

| Column | Type | Description |
| --- | --- | --- |
| `estimated_count` | double | FaSTest's unbiased estimate of total embeddings. |
| `sample_count` | int64 | Number of embeddings actually written to `result_file`. |
| `result_file` | string | CSV path holding the sampled embeddings. |
| `props_file` | string | JSON path holding the requested properties, or empty if no `RETURN` clause is present. |

#### Inline form

The DSL goes straight in the call:

```cypher
CALL SAMPLED_MATCH_PATTERN('
  MATCH (a:Person)-[:person_knows_person]->(b:Person)
        -[:person_knows_person]->(c:Person)-[:person_knows_person]->(a)
  WHERE a.age >= 18
  RETURN a.name, b.name, c.name
', 1000)
RETURN estimated_count, sample_count, result_file, props_file;
```

This is the recommended form for everyday use.

#### File form

For long patterns, generated workflows, or DSL stored alongside other artifacts, save the DSL to a `.dsl` file and pass its path. `SAMPLED_MATCH_PATTERN` detects an existing file path and reads it transparently — same result, same output schema:

```bash
mkdir -p /tmp/sm
cat > /tmp/sm/q.dsl <<'EOF'
MATCH (a:Person)-[:person_knows_person]->
      (b:Person)-[:person_knows_person]->
      (c:Person)-[:person_knows_person]->(a)
WHERE a.age >= 18
RETURN a.name, b.name, c.name
EOF
```

```cypher
CALL SAMPLED_MATCH_PATTERN('/tmp/sm/q.dsl', 1000)
RETURN estimated_count, sample_count, result_file, props_file;
```

The DSL → in-memory JSON conversion is performed transparently; no temporary files are written on the matcher's side.

### Reading the result files

`result_file` is a CSV with one column per pattern vertex (`v0`, `v1`, …) and one column per pattern edge (`v0-v1`, `v1-v2`, …). Vertex columns hold internal vertex ids; edge columns hold `src:dst:offset` triples that identify each matched edge instance.

```csv
v0,v1,v2,v0-v1,v1-v2,v2-v0
101,102,103,101:102:0,102:103:0,103:101:0
...
```

If the DSL had a `RETURN` clause, `props_file` is a JSON document keyed by sample index that holds the requested properties:

```json
{
  "0": {
    "v0": {"name": "Alice"},
    "v1": {"name": "Bob"},
    "v2": {"name": "Carol"}
  }
}
```

You can rejoin these ids with a regular Cypher query if you need the full property record:

```cypher
MATCH (p:Person) WHERE p.id IN [101, 102, 103]
RETURN p.id, p.name, p.age;
```

## End-to-end walkthrough

Run the snippets in order against an empty database.

### Step 1. Install and load

```cypher
INSTALL SAMPLED_MATCH;
LOAD SAMPLED_MATCH;
```

### Step 2. Schema

Labels in your pattern must exist in the data graph schema.

```cypher
CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age INT32);
CREATE REL TABLE person_knows_person(FROM Person TO Person, weight DOUBLE);
```

### Step 3. Data

```cypher
CREATE (n:Person {id: 0, name: 'Alice', age: 20});
CREATE (n:Person {id: 1, name: 'Bob',   age: 30});
CREATE (n:Person {id: 2, name: 'Carol', age: 20});
CREATE (n:Person {id: 3, name: 'Dave',  age: 40});

MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2
  CREATE (a)-[:person_knows_person {weight: 1.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 3
  CREATE (a)-[:person_knows_person {weight: 2.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 1
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
```

### Step 4. (Optional) Warm the FaSTest cache

`CALL INITIALIZE()` builds the auxiliary data structures FaSTest uses. It runs automatically on first use; you can invoke it explicitly to amortise setup across a batch of pattern queries.

```cypher
CALL INITIALIZE()
RETURN status, num_vertices, num_edges, max_degree, degeneracy;
```

### Step 5. Run the estimate

```cypher
CALL SAMPLED_MATCH_PATTERN('
  MATCH (a:Person)-[:person_knows_person]->
        (b:Person)-[:person_knows_person]->
        (c:Person)-[:person_knows_person]->(a)
  WHERE a.age >= 18
  RETURN a.name, b.name, c.name
', 1000)
RETURN estimated_count, sample_count, result_file, props_file;
```

Example output:

| estimated_count | sample_count | result_file | props_file |
| --- | --- | --- | --- |
| 3.16 | 1000 | `/tmp/neug_sample/sampled_match_…csv` | `/tmp/neug_sample/sampled_match_…json` |
