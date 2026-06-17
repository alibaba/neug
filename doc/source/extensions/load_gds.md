# GDS (Graph Data Science) Extension

The GDS extension provides a collection of graph algorithms that run on projected
subgraphs. It enables common analytical workloads — community detection, centrality
analysis, shortest-path computation — directly inside NeuG through the `CALL`
interface.

## Quick Start

```cypher
-- 1. Load the extension
LOAD gds;

-- 2. Project a subgraph
CALL project_graph(
    'social',
    ['person'],
    {'[person, knows, person]': ''}
);

-- 3. Run an algorithm
CALL page_rank('social', {max_iterations: 20})
RETURN node.fName, rank;

-- 4. Clean up
CALL drop_projected_graph('social');
```

## Graph Projection

Before running any GDS algorithm, you must create a **projected graph** — a named
subgraph view that defines which vertex labels, edge triplets, and optional
predicates the algorithm operates on.

### `project_graph`

```cypher
CALL project_graph(
    '<graph_name>',
    <vertex_entries>,
    <edge_entries>
);
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `graph_name` | STRING | A unique alias for the projected subgraph |
| `vertex_entries` | LIST or MAP | Vertex labels, optionally with predicates |
| `edge_entries` | MAP | Edge triplets `[src, edge, dst]` mapped to predicates |

**Vertex entries** can be a simple list of labels or a map with predicates:

```cypher
-- List form (no predicates)
['person']

-- Map form (with predicates)
{'person': 'n.age > 20', 'organisation': 'n.name = "MIT"'}
```

**Edge entries** are always a map from triplet to predicate (use empty string `''`
for no predicate):

```cypher
{'[person, knows, person]': ''}
{'[person, studyat, organisation]': 'r.year > 2010'}
```

### `drop_projected_graph`

```cypher
CALL drop_projected_graph('<graph_name>');
```

Removes a previously projected graph alias.

### `SHOW_PROJECTED_GRAPHS`

```cypher
CALL SHOW_PROJECTED_GRAPHS() RETURN *;
```

Lists all currently registered projected graph aliases.

### `PROJECTED_GRAPH_INFO`

```cypher
CALL PROJECTED_GRAPH_INFO('<graph_name>') RETURN *;
```

Returns the labels and predicates of a projected graph. Each row contains:

| Column | Description |
|---|---|
| label | Vertex label name or edge triplet (e.g., `[person,knows,person]`) |
| predicate | The filter predicate string, or empty if none |

## Algorithms

All algorithms follow the same calling convention:

```cypher
CALL <algorithm_name>('<projected_graph>', {<options>})
RETURN <columns>;
```

Every algorithm returns a `node` column (the matched vertices) plus one or more
result columns. The `node` column is of type `NODE`, so you can access vertex
properties via `node.<property>` in the `RETURN` clause.

> **Note:** Most algorithms (except Label Propagation) require a **homogeneous graph**
> subgraph — exactly one vertex label and one edge triplet where the source and
> destination labels match the vertex label.

---

### PageRank

Computes the PageRank centrality score for each vertex. Higher scores indicate
more influential nodes in the graph.

```cypher
CALL page_rank('<graph_name>', {<options>})
RETURN node, rank;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `damping_factor` | DOUBLE | `0.85` | Probability of following a link (vs. random jump) |
| `max_iterations` | INT | `20` | Maximum number of iterations |
| `directed` | BOOL | `false` | Whether to treat edges as directed |
| `concurrency` | INT | CPU cores | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `rank` | DOUBLE | PageRank score |

**Example:**

```cypher
CALL project_graph('social', ['person'], {'[person, knows, person]': ''});
LOAD gds;

CALL page_rank('social', {damping_factor: 0.85, max_iterations: 30})
RETURN node.fName, rank
ORDER BY rank DESC;
```

**Predicate support:** Vertex predicates are supported; edge predicates are **not**.

---

### BFS (Breadth-First Search)

Computes the shortest hop distance from a source vertex to all reachable vertices.

```cypher
CALL bfs('<graph_name>', {<options>})
RETURN node, distance;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `source` | STRING | *(required)* | The source vertex's primary key value |
| `directed` | BOOL | `false` | Whether to follow edges in their stored direction only |
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `distance` | INT64 | Hop count from the source vertex |

**Example:**

```cypher
CALL bfs('social', {source: '0'})
RETURN node.fName, distance
ORDER BY distance;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### SSSP (Single-Source Shortest Path)

Computes the shortest weighted path distance from a source vertex to all
reachable vertices. Without a weight property, it behaves like BFS but returns
`DOUBLE` distances.

```cypher
CALL sssp('<graph_name>', {<options>})
RETURN node, distance;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `source` | STRING | *(required)* | The source vertex's primary key value |
| `directed` | BOOL | `false` | Whether to follow edges in their stored direction only |
| `weight` | STRING | `""` | Edge property name to use as weight (empty = unit weight) |
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `distance` | DOUBLE | Shortest path distance from the source |

**Example:**

```cypher
CALL sssp('social', {source: '0', weight: 'cost', directed: true})
RETURN node.fName, distance;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### WCC (Weakly Connected Components)

Assigns each vertex a component ID. Vertices in the same connected component
share the same ID.

```cypher
CALL wcc('<graph_name>', {<options>})
RETURN node, comp;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `comp` | INT64 | Component identifier |

**Example:**

```cypher
CALL wcc('social', {concurrency: 8})
RETURN node.fName, comp
ORDER BY comp;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### LCC (Local Clustering Coefficient)

Measures how close a vertex's neighbors are to forming a complete graph
(clique). Values range from 0.0 to 1.0.

```cypher
CALL lcc('<graph_name>', {<options>})
RETURN node, lcc;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `directed` | BOOL | `false` | Whether to compute the directed clustering coefficient |
| `degree_threshold` | INT | MAX_INT | Skip vertices with degree above this threshold |
| `concurrency` | INT | CPU cores | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `lcc` | DOUBLE | Local clustering coefficient |

**Example:**

```cypher
CALL lcc('social', {degree_threshold: 1000})
RETURN node.fName, lcc
ORDER BY lcc DESC;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### K-Core Decomposition

Computes the core number for each vertex. A vertex has core number *k* if it
belongs to a *k*-core (a maximal subgraph where every vertex has degree >= *k*)
but not a *(k+1)*-core.

```cypher
CALL kcore('<graph_name>', {<options>})
RETURN node, core;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `k` | INT | `2` | Minimum core number threshold (must be >= 0) |
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `core` | INT64 | Core number of the vertex |

**Example:**

```cypher
CALL kcore('social', {k: 3})
RETURN node.fName, core
ORDER BY core DESC;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### CDLP (Community Detection using Label Propagation)

A community detection algorithm that propagates labels through the network.
Each vertex is initially assigned a unique label; in each iteration, every vertex
adopts the most frequent label among its neighbors.

```cypher
CALL cdlp('<graph_name>', {<options>})
RETURN node, label;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `max_iterations` | INT | `5` | Maximum number of propagation iterations |
| `concurrency` | INT | `1` | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `label` | INT64 | Community label assigned to this vertex |

**Example:**

```cypher
CALL project_graph(
    'study_net',
    {'person': 'n.age > 20', 'organisation': 'n.name = "MIT"'},
    {'[person, studyat, organisation]': 'r.year > 2010'}
);
LOAD gds;

CALL cdlp('study_net', {concurrency: 10})
RETURN node.id, node.fName, node.name, label;
```

**Predicate support:** Both vertex and edge predicates are supported. CDLP
also supports heterogeneous vertex labels in the projected subgraph
(unlike other algorithms which require a single vertex label).

---

### Louvain

A community detection algorithm that optimizes modularity by iteratively moving
vertices between communities and aggregating the graph into super-nodes.

```cypher
CALL louvain('<graph_name>', {<options>})
RETURN node, community;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `resolution` | DOUBLE | `1.0` | Resolution parameter (gamma). Values > 1 favor smaller communities, < 1 favor larger communities |
| `directed` | BOOL | `false` | Whether to treat the graph as directed |
| `threshold` | DOUBLE | `1e-7` | Modularity gain threshold for convergence |
| `concurrency` | INT | CPU cores | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `community` | INT64 | Community ID (0-based) |

**Example:**

```cypher
CALL louvain('social', {resolution: 1.0, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

---

### Leiden

A community detection algorithm that improves upon Louvain by adding a refinement
phase. This refinement allows communities to be split during execution, leading
to better detection of small communities and higher-quality partitions.

```cypher
CALL leiden('<graph_name>', {<options>})
RETURN node, community;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `resolution` | DOUBLE | `1.0` | Resolution parameter (gamma). Values > 1 favor smaller communities, < 1 favor larger communities |
| `directed` | BOOL | `false` | Whether to treat the graph as directed |
| `threshold` | DOUBLE | `1e-7` | Modularity gain threshold for convergence |
| `concurrency` | INT | CPU cores | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `community` | INT64 | Community ID (0-based) |

**Example:**

```cypher
CALL leiden('social', {resolution: 1.5, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Predicate support:** Neither vertex nor edge predicates are supported.

**Leiden vs. Louvain:** Use Leiden when you need higher-quality community partitions
or better detection of small communities. Use Louvain when you need the fastest
possible execution.

---

### Personalized PageRank

> **Status:** Implemented but not yet registered in the extension. Available in
> a future release.

A variant of PageRank that biases the random-walk restart probability toward
specific vertices using node weights, and incorporates edge weights into the
transition probabilities.

```cypher
CALL personalized_page_rank('<graph_name>', {<options>})
RETURN node, personalized_page_rank;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `damping_factor` | DOUBLE | `0.85` | Random walk continuation probability |
| `max_iterations` | INT | `20` | Maximum number of iterations |
| `edge_weight` | STRING | *(required)* | Edge property name for transition weights (DOUBLE) |
| `node_weight` | STRING | *(required)* | Vertex property name for restart bias (DOUBLE) |
| `concurrency` | INT | `1` | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The vertex |
| `personalized_page_rank` | DOUBLE | Personalized PageRank score |

**Predicate support:** Neither vertex nor edge predicates are supported.

## Algorithm Summary

| Algorithm | CALL Name | Output Columns | Key Options |
|---|---|---|---|
| PageRank | `page_rank` | `node`, `rank` | `damping_factor`, `max_iterations`, `directed` |
| BFS | `bfs` | `node`, `distance` | `source` (required) |
| SSSP | `sssp` | `node`, `distance` | `source` (required), `weight`, `directed` |
| WCC | `wcc` | `node`, `comp` | `concurrency` |
| LCC | `lcc` | `node`, `lcc` | `directed`, `degree_threshold` |
| K-Core | `kcore` | `node`, `core` | `k` |
| CDLP | `cdlp` | `node`, `label` | `max_iterations` |
| Louvain | `louvain` | `node`, `community` | `resolution`, `directed`, `threshold`, `concurrency` |
| Leiden | `leiden` | `node`, `community` | `resolution`, `directed`, `threshold`, `concurrency` |
| Personalized PageRank* | `personalized_page_rank` | `node`, `personalized_page_rank` | `edge_weight`, `node_weight` |

\* Not yet registered in the extension; available in a future release.

## Common Options

All algorithms accept a `concurrency` option that controls the number of
threads used for parallel computation. The default depends on the algorithm:

- **Most algorithms:** defaults to the number of CPU cores
- **Label Propagation, Personalized PageRank:** defaults to `1`

## Limitations

- Most algorithms require a **homogeneous graph** subgraph (exactly one vertex
  label and one edge triplet `[A, edge, A]`). Label Propagation is the exception.
- Edge predicates are only supported by Label Propagation.
- Personalized PageRank requires that both `edge_weight` and `node_weight`
  properties exist and are of type `DOUBLE`.
