# Graph Data Science (GDS) Extension

Parallel graph analytics algorithms for NeuG, running natively inside the query engine via `LOAD gds`.

## Algorithms

| Category | Algorithm | Output |
|----------|-----------|--------|
| Traversal | `bfs` | hop-count distance, optional path |
| Traversal | `sssp` | weighted distance, optional path |
| Centrality | `page_rank` | node rank (normalized) |
| Connectivity | `wcc` | component ID |
| Community | `cdlp` | community label |
| Community | `louvain` | community ID |
| Community | `leiden` | community ID |
| Structural | `lcc` | clustering coefficient (0–1) |
| Structural | `kcore` | core number |

All algorithms operate on **projected subgraphs** created via `project_graph()`.

## Usage

```cypher
LOAD gds;

-- Project a subgraph
CALL project_graph('g', ['Person'], {'[Person, KNOWS, Person]': ''});

-- Run an algorithm
CALL bfs('g', {source: 'Alice'})
YIELD node, distance, path
RETURN node.id, distance, path;

CALL page_rank('g', {max_iterations: 20})
YIELD node, rank
RETURN node.id, rank ORDER BY rank DESC LIMIT 10;
```

Full documentation with all options and examples: [`doc/source/extensions/load_gds.md`](../../doc/source/extensions/load_gds.md)

## Building

```bash
# From repo root
make cpp-build EXTRA_CMAKE_FLAGS="-DBUILD_EXTENSIONS=gds"
```

## Testing

```bash
cd tools/python_bind
python3 -m pytest tests/test_gds.py tests/test_gds_path.py -v
```

## License

Apache License 2.0
