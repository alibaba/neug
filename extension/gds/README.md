# Graph Data Science (GDS) Extension

A high-performance graph analytics extension for NeuG, providing scalable algorithms for graph analysis and machine learning.

## Features

### Shortest Path Algorithms with Path Return

BFS and SSSP algorithms now support returning the actual shortest path (not just the distance).

#### Usage

```cypher
// Load GDS extension
LOAD gds;

// Project a graph
CALL project_graph('my_graph', ['Person'], ['KNOWS']);

// BFS - returns hop count and path
CALL bfs('my_graph', {source: 'Alice'}) 
YIELD node, distance, path 
RETURN node, distance, path;

// SSSP - returns weighted distance and path
CALL sssp('my_graph', {source: 'Alice', weight: 'distance'}) 
YIELD node, distance, path 
RETURN node, distance, path;
```

#### Path Encoding Modes

Control the amount of path information returned:

- **Lightweight mode** (default): Only structural information (`_ID`, `_LABEL`)
  ```cypher
  CALL bfs('my_graph', {source: 'Alice'}) 
  YIELD node, distance, path 
  RETURN node, distance, path;
  ```

- **Full mode**: Include all properties
  ```cypher
  CALL bfs('my_graph', {source: 'Alice', path_properties: 'full'}) 
  YIELD node, distance, path 
  RETURN node, distance, path;
  ```

#### Performance

On LDBC SNB SF10 (65K nodes, 1.9M edges, 62K paths):

| Mode | Time |
|------|------|
| No path | 0.025s |
| Lightweight path | 0.54s |
| Full path | 1.26s |

### Other Algorithms

- **PageRank**: Identify influential nodes
- **WCC (Weakly Connected Components)**: Find connected subgraphs
- **CDLP (Community Detection Label Propagation)**: Detect communities
- **LCC (Local Clustering Coefficient)**: Measure local connectivity
- **K-Core**: Find densely connected subgraphs
- **Louvain/Leiden**: Advanced community detection

## Building

```bash
cd build
cmake -DBUILD_EXTENSIONS=gds ..
make -j$(sysctl -n hw.ncpu)
```

## Testing

```bash
# Run GDS tests
cd tools/python_bind
python3 -m pytest tests/test_gds.py -v

# Run path return tests
python3 -m pytest tests/test_gds_path.py tests/test_gds_path_ldbc.py -v
```

## Documentation

- [Shortest Path Return Specification](docs/gds-shortest-path-spec.md) - Detailed technical specification

## License

Apache License 2.0
