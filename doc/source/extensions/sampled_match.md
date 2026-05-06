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

### Example Pattern File

A directed triangle over `Person` nodes, requiring an `age >= 18` filter on vertex `0` and asking the engine to return each matched person's `name`:

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

## Using the Sampled Match Extension

The typical workflow is: load the extension, build the cache once, run the estimator on each pattern, and (optionally) materialise properties for the sampled embeddings.

### 1. Describe the Pattern in JSON

Save the pattern from the [Example Pattern File](#example-pattern-file) section to a short path such as `/tmp/sm/p.json`. The file must contain at least the `vertices` and `edges` arrays; `constraints` and `required_props` are optional per element.

### 2. Run `SAMPLED_MATCH`

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

The `result_file` CSV looks like:

```csv
v0,v1,v2,v0-v1,v1-v2,v2-v0
101,102,103,101:102:0,102:103:0,103:101:0
...
```

If any `required_props` were declared in the pattern JSON, the `props_file` is a JSON document keyed by sample index that lists the requested vertex and edge properties for each embedding.
