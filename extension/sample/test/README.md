# Subgraph Matching Test

This directory contains tests for the subgraph matching functionality using the FaSTest algorithm.

## Prerequisites

1. Build neug with the sample extension enabled:

```bash
cd /path/to/neug
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TEST=ON -DBUILD_EXTENSIONS="sample" -DBUILD_HTTP_SERVER=ON
make -j$(nproc)
```

## Data Preparation

The test uses the small LDBC-like dataset located at `sampling/dataset/small/`.

### Option 1: Using neug CLI to import data

First, start the neug server:

```bash
./bin/rt_server --data-path ./sampling/dataset/small
```

Then the data will be loaded automatically from the graph.yaml and import.yaml configuration.

### Option 2: Using the test program directly

The test program `test_sample_match` can directly load data from a directory containing `graph.yaml`:

```bash
./build/extension/sample/test/test_sample_match \
    ./sampling/dataset/small \
    ./sampling/pattern/pattern_with_constraints.json
```

## Pattern File Format

The pattern file is a JSON file describing the subgraph pattern to match:

```json
{
  "vertices": [
    {"id": 0, "label": "person"},
    {"id": 1, "label": "person"},
    {"id": 2, "label": "comment"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "comment_hasCreator_person"}
  ]
}
```

## Running the Test

### Using the shell script:

```bash
cd extension/sample/test
./run_test.sh
```

### Manually:

```bash
# From the neug project root
./build/extension/sample/test/test_sample_match \
    ./sampling/dataset/small \
    ./sampling/pattern/pattern_with_constraints.json
```

## Expected Output

The test will output:
1. Graph loading statistics (vertices, edges, degrees)
2. Pattern information
3. Cardinality estimation process
4. Estimated number of embeddings
5. Sample embeddings (first 5)

Example output:
```
============================================
  Subgraph Matching Test (FaSTest)
============================================
Data path: ./sampling/dataset/small
Pattern file: ./sampling/pattern/pattern_with_constraints.json

Opening database...
Database loaded in 0.5 seconds

[0] Building label mappings...
  Edge triplet: person -[person_knows_person]-> person
  Edge triplet: comment -[comment_hasCreator_person]-> person
  Edge triplet: post -[post_hasCreator_person]-> person

[1] Preprocessing data graph...
  Vertices: 10
  Edges: 12
  Max degree: 6
  Degeneracy: 2

[2] Loading pattern graph...
  Pattern: 5 vertices, 5 edges

...

=== Results ===
  Estimated embedding count: 42
  Number of sampled embeddings: 15

============================================
  Test completed successfully!
============================================
```

## Notes

- The FaSTest algorithm provides an estimated count of embeddings, not exact count
- The sampling-based approach trades accuracy for speed on large graphs
- For exact matching, consider using the brute-force algorithm (slower)
