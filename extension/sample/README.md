# Sample Extension: Subgraph Matching

This extension provides brute-force subgraph matching functionality for NeuG.

## Functions

### SUBGRAPH_MATCH(pattern_file)

Performs subgraph matching using a pattern defined in a JSON file.

**Input**: Path to a JSON file describing the pattern to match.

**Output**: All subgraph matches found in the graph.

## Pattern JSON Format

The pattern file should be a JSON file with the following structure:

```json
{
  "description": "Optional description of the pattern",
  "vertices": [
    {
      "id": "a",           // Unique identifier within the pattern
      "label": "Person"    // Vertex label in the graph
    },
    {
      "id": "b",
      "label": "Person"
    }
  ],
  "edges": [
    {
      "src": "a",          // Source vertex id (must match a vertex id above)
      "dst": "b",          // Destination vertex id
      "label": "KNOWS"     // Edge label in the graph
    }
  ]
}
```

## Example Patterns

### Simple Edge Pattern (example_pattern.json)

Find two people who know each other:

```json
{
  "vertices": [
    {"id": "a", "label": "person"},
    {"id": "b", "label": "person"}
  ],
  "edges": [
    {"src": "a", "dst": "b", "label": "person_knows_person"}
  ]
}
```

### Triangle Pattern (triangle_pattern.json)

Find a triangle of three people who all know each other:

```json
{
  "vertices": [
    {"id": "a", "label": "person"},
    {"id": "b", "label": "person"},
    {"id": "c", "label": "person"}
  ],
  "edges": [
    {"src": "a", "dst": "b", "label": "person_knows_person"},
    {"src": "b", "dst": "c", "label": "person_knows_person"},
    {"src": "c", "dst": "a", "label": "person_knows_person"}
  ]
}
```

## Usage in Cypher

```cypher
-- Load the extension
LOAD EXTENSION sample;

-- Run subgraph matching
CALL SUBGRAPH_MATCH('/path/to/pattern.json') YIELD *;

-- Or use the alias
CALL MATCH_SUBGRAPH('/path/to/triangle_pattern.json') YIELD *;
```

## Algorithm

This extension uses a **brute-force backtracking algorithm** for subgraph isomorphism:

1. For the first pattern vertex, try all data graph vertices with the same label
2. For each subsequent pattern vertex:
   - Try all candidate vertices with matching label
   - Check edge constraints with already-matched vertices
   - If all constraints satisfied, recursively match the next vertex
3. When all pattern vertices are matched, record the result
4. Backtrack and try other combinations

### Complexity

- Time complexity: O(n^k) where n is the number of vertices in the data graph and k is the number of vertices in the pattern
- This is intentionally simple ("brute-force") and suitable for small patterns on small graphs

### Limitations

- Performs **subgraph isomorphism** (each data vertex can only be matched once per result)
- Does not support property predicates on vertices/edges (only label matching)
- Single-threaded execution

## Building

```bash
cmake -DBUILD_EXTENSIONS="sample" ..
make -j
```

## Files

```
extension/sample/
├── CMakeLists.txt              # Build configuration
├── README.md                   # This file
├── example_pattern.json        # Simple edge pattern example
├── triangle_pattern.json       # Triangle pattern example
├── include/
│   └── sample_functions.h      # Core implementation
│       ├── Pattern structures  # PatternVertex, PatternEdge, Pattern
│       ├── JSON parsing        # parsePatternFromJson()
│       ├── BruteForceSubgraphMatcher  # Main algorithm
│       └── SubgraphMatchFunction      # TableFunction wrapper
└── src/
    └── sample_extension.cpp    # Extension entry point
```
