# Sampled Match Extension

Subgraph matching cardinality estimation is a common building block for query optimization and graph analytics: given a small *pattern graph* `P` and a large *data graph* `G`, estimate how many embeddings of `P` exist in `G` and produce a representative sample of those embeddings. NeuG provides this capability through the `sampled_match` extension, which implements the FaSTest filtering-sampling algorithm described in *Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach* (VLDB 2024).

After loading the extension, users can describe a pattern in JSON, run the estimator with a single `CALL`, and then look up properties on the matched vertices and edges from the result.

## Install Extension

```cypher
INSTALL SAMPLED_MATCH;
```

## Load Extension

```cypher
LOAD SAMPLED_MATCH;
```

## Pattern File JSON Format

`SAMPLED_MATCH` reads the pattern graph from a JSON file on disk. The file describes the labeled vertices and edges of `P`, plus optional property constraints and a list of properties that should be returned for each match.

### Top-level Schema

| Field      | Type            | Required | Description                                                          |
| ---------- | --------------- | -------- | -------------------------------------------------------------------- |
| `vertices` | array of object | yes      | The pattern's vertices. Vertex `id` values must be `0..N-1`.         |
| `edges`    | array of object | yes      | The pattern's directed edges. Each entry references vertex ids.      |

### Vertex Object

| Field            | Type            | Required | Description                                                                                                          |
| ---------------- | --------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| `id`             | int or string   | yes      | Vertex index in the pattern, starting at `0`. String values are parsed as integers.                                  |
| `label`          | string          | yes      | Vertex label name. Must exist in the data graph schema.                                                              |
| `constraints`    | array of object | no       | Property predicates applied during matching. See [Property Constraints](#property-constraints) below.                |
| `required_props` | array of string | no       | Property names to fetch for this matched vertex. The values are written to the `props_file` JSON output.             |

### Edge Object

| Field            | Type            | Required | Description                                                                                                          |
| ---------------- | --------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| `source`         | int or string   | yes      | The `id` of the source vertex in `vertices`.                                                                         |
| `target`         | int or string   | yes      | The `id` of the target vertex in `vertices`.                                                                         |
| `label`          | string          | no       | Edge label name. Must exist in the data graph schema if provided.                                                    |
| `constraints`    | array of object | no       | Property predicates applied to this edge during matching.                                                            |
| `required_props` | array of string | no       | Property names to fetch for this matched edge.                                                                       |

### Property Constraints

Each entry in a `constraints` array filters embeddings by a vertex/edge property:

| Field      | Type             | Required | Description                                                                       |
| ---------- | ---------------- | -------- | --------------------------------------------------------------------------------- |
| `property` | string           | yes      | Property name on the vertex or edge.                                              |
| `operator` | string           | no       | Comparison operator. One of `=` / `==`, `>`, `<`, `>=`, `<=`, `in`, `not_in`. Defaults to `=`. |
| `value`    | int / double / string / bool | yes | Literal value compared against the property.                              |

### Example Pattern: a directed triangle

The pattern below is a directed 3-cycle over three `Person` vertices connected by `person_knows_person` edges. Vertex `0` carries an `age >= 18` constraint and every vertex asks the engine to return the matched person's `name`.

```text
            (v0:Person, age >= 18)
                  /  \
                 /    \
                v      \
        (v1:Person)     \
                 \      /
                  \    /
                   v  /
            (v2:Person)

Directed edges (all labelled person_knows_person):
    v0 ──▶ v1 ──▶ v2 ──▶ v0
```

The same pattern as a JSON file:

```json
{
  "vertices": [
    {
      "id": 0,
      "label": "Person",
      "constraints": [
        {"property": "age", "operator": ">=", "value": 18}
      ],
      "required_props": ["name"]
    },
    {"id": 1, "label": "Person", "required_props": ["name"]},
    {"id": 2, "label": "Person", "required_props": ["name"]}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
}
```

## Step-by-Step Walkthrough

This walkthrough starts from an empty database and runs `SAMPLED_MATCH` against the triangle pattern above. Run the Cypher snippets in order — each step assumes the previous step succeeded.

### Step 1. Install and load the extension

`INSTALL` only needs to run once per database; `LOAD` makes the extension's procedures (such as `SAMPLED_MATCH` and `INITIALIZE`) available in the current session.

```cypher
INSTALL SAMPLED_MATCH;
LOAD SAMPLED_MATCH;
```

### Step 2. Create the data graph schema

Define the node and relationship tables that the pattern's labels will reference. The labels and property names used here (`Person`, `person_knows_person`, `age`, `name`) must match the ones used in the pattern JSON.

```cypher
CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age INT32);
CREATE REL TABLE person_knows_person(FROM Person TO Person, weight DOUBLE);
```

### Step 3. Insert the data graph

Insert a handful of `Person` nodes and the `person_knows_person` edges between them. The example below seeds four people and five edges; the directed cycle `0 → 1 → 2 → 0` produces exactly one triangle embedding.

```cypher
-- Nodes
CREATE (n:Person {id: 0, name: 'Alice', age: 20});
CREATE (n:Person {id: 1, name: 'Bob',   age: 30});
CREATE (n:Person {id: 2, name: 'Carol', age: 20});
CREATE (n:Person {id: 3, name: 'Dave',  age: 40});

-- Edges (the first three form the triangle 0 -> 1 -> 2 -> 0)
MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2
  CREATE (a)-[:person_knows_person {weight: 1.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);

-- Two extra edges so the graph isn't just the triangle
MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 3
  CREATE (a)-[:person_knows_person {weight: 2.5}]->(b);
MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 1
  CREATE (a)-[:person_knows_person {weight: 0.5}]->(b);
```

### Step 4. Save the pattern JSON to disk

Write the triangle pattern from the [Example Pattern](#example-pattern-a-directed-triangle) section to a short path on disk, for example `/tmp/sm/p.json`. The path is passed as a string literal inside `CALL`, and the current parser limits string literals to 48 characters — so keep the directory short.

```bash
mkdir -p /tmp/sm
cat > /tmp/sm/p.json <<'EOF'
{
  "vertices": [
    {
      "id": 0, "label": "Person",
      "constraints": [{"property": "age", "operator": ">=", "value": 18}],
      "required_props": ["name"]
    },
    {"id": 1, "label": "Person", "required_props": ["name"]},
    {"id": 2, "label": "Person", "required_props": ["name"]}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
}
EOF
```

### Step 5. (Optional) Initialize the FaSTest cache

`CALL INITIALIZE()` builds the auxiliary data structures FaSTest uses (degree information, ordering hints, etc.). It is invoked automatically the first time you call `SAMPLED_MATCH`, but you can run it explicitly to inspect the loaded graph or to amortise the setup cost before a batch of pattern queries.

```cypher
CALL INITIALIZE()
RETURN status, num_vertices, num_edges, max_degree, degeneracy;
```

### Step 6. Run `SAMPLED_MATCH`

`SAMPLED_MATCH` runs the FaSTest cardinality estimator against the loaded data graph. It takes two positional arguments:

| Argument       | Type   | Required | Description                                                                                                                                              |
| -------------- | ------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pattern_file` | string | yes      | Absolute path to the pattern JSON file described in [Pattern File JSON Format](#pattern-file-json-format). Keep the path short — string literals in `CALL` are bounded at 48 characters in the current parser, so prefer paths like `/tmp/sm/p.json`. |
| `sample_size`  | int64  | yes      | Target number of embeddings to sample. Larger values produce tighter estimates and bigger result files; the estimator returns a representative subset (the actual number written may be smaller and is reported as `sample_count`). |

The procedure returns one row with four columns:

| Column            | Type   | Description                                                                                                                                              |
| ----------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `estimated_count` | double | FaSTest's unbiased estimate of the total number of embeddings of the pattern in the data graph.                                                          |
| `sample_count`    | int64  | Number of embeddings actually written to `result_file`.                                                                                                  |
| `result_file`     | string | Path to the CSV file holding the sampled embeddings (see schema below).                                                                                  |
| `props_file`      | string | Path to the JSON file holding the requested vertex/edge properties, or an empty string if no `required_props` were declared in the pattern JSON.         |

```cypher
CALL SAMPLED_MATCH('/tmp/sm/p.json', 1000)
RETURN estimated_count, sample_count, result_file, props_file;
```

Example output:

| estimated_count | sample_count | result_file                                  | props_file                                  |
| --------------- | ------------ | -------------------------------------------- | ------------------------------------------- |
| 12345.67        | 873          | `/tmp/neug_sample/sampled_match_…csv`        | `/tmp/neug_sample/sampled_match_…json`      |

### Step 7. Read the sample files

The `result_file` CSV has one column per pattern vertex (`v0`, `v1`, …) and one column per pattern edge (`v0-v1`, `v1-v2`, …). Vertex columns hold internal vertex ids; edge columns hold `src:dst:offset` triples that identify each matched edge instance.

```csv
v0,v1,v2,v0-v1,v1-v2,v2-v0
101,102,103,101:102:0,102:103:0,103:101:0
...
```

If any `required_props` were declared in the pattern JSON, the `props_file` is a JSON document keyed by sample index that lists the requested vertex and edge properties for each embedding. For the triangle pattern above (which asks for `name` on each vertex) a single record looks like:

```json
{
  "0": {
    "v0": {"name": "Alice"},
    "v1": {"name": "Bob"},
    "v2": {"name": "Carol"}
  }
}
```

You can join these ids back into the database with a regular Cypher query if you need the full property record for each match:

```cypher
MATCH (p:Person) WHERE p.id IN [101, 102, 103]
RETURN p.id, p.name, p.age;
```
