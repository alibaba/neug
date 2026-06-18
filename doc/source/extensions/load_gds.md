# GDS (Graph Data Science) Extension

Since NeuG **v0.1.3**, we have introduced the GDS extension, which provides a collection of graph algorithms that run on projected subgraphs.
It enables common analytical workloads — community detection, centrality analysis, shortest-path computation — directly inside NeuG through the `CALL` interface.

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
subgraph view that defines which node labels, edge triplets, and optional
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
| `node_entries` | LIST or MAP | Node labels, optionally with predicates |
| `edge_entries` | MAP | Edge triplets `[src, edge, dst]` mapped to predicates |

**Node entries** can be a simple list of labels or a map with predicates:

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
| label | Node label name or edge triplet (e.g., `[person,knows,person]`) |
| predicate | The filter predicate string, or empty if none |

## Algorithms

All algorithms follow the same calling convention:

```cypher
CALL <algorithm_name>('<projected_graph>', {<options>})
RETURN <columns>;
```

Every algorithm returns a `node` column (the matched nodes) plus one or more
result columns. The `node` column is of type `NODE`, so you can access node
properties via `node.<property>` in the `RETURN` clause.

> **Note:** Most algorithms (except Label Propagation) require a **homogeneous graph**
> subgraph — exactly one node label and one edge triplet where the source and
> destination labels match the node label.

---

### PageRank

Computes the PageRank centrality score for each node. Higher scores indicate
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
| `node` | NODE | The node |
| `rank` | DOUBLE | PageRank score |

**Example:**

```cypher
CALL project_graph('social', ['person'], {'[person, knows, person]': ''});
LOAD gds;

CALL page_rank('social', {damping_factor: 0.85, max_iterations: 30})
RETURN node.fName, rank
ORDER BY rank DESC;
```

**Predicate support:** Both node and edge predicates are supported.

---

### BFS (Breadth-First Search)

Computes the shortest hop distance from a source node to all reachable nodes.

```cypher
CALL bfs('<graph_name>', {<options>})
RETURN node, distance;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `source` | STRING | *(required)* | The source node's primary key value |
| `directed` | BOOL | `false` | Whether to follow edges in their stored direction only |
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The node |
| `distance` | INT64 | Hop count from the source node |

**Example:**

```cypher
CALL bfs('social', {source: '0'})
RETURN node.fName, distance
ORDER BY distance;
```

**Predicate support:** Both node and edge predicates are supported.

---

### SSSP (Single-Source Shortest Path)

Computes the shortest weighted path distance from a source node to all
reachable nodes. Without a weight property, it behaves like BFS but returns
`DOUBLE` distances.

```cypher
CALL sssp('<graph_name>', {<options>})
RETURN node, distance;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `source` | STRING | *(required)* | The source node's primary key value |
| `directed` | BOOL | `false` | Whether to follow edges in their stored direction only |
| `weight` | STRING | `""` | Edge property name to use as weight (empty = unit weight) |
| `concurrency` | INT | CPU cores | Number of threads |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The node |
| `distance` | DOUBLE | Shortest path distance from the source |

**Example:**

```cypher
CALL sssp('social', {source: '0', weight: 'cost', directed: true})
RETURN node.fName, distance;
```

**Predicate support:** Both node and edge predicates are supported.

---

### WCC (Weakly Connected Components)

Assigns each node a component ID. Nodes in the same connected component
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
| `node` | NODE | The node |
| `comp` | INT64 | Component identifier |

**Example:**

```cypher
CALL wcc('social', {concurrency: 8})
RETURN node.fName, comp
ORDER BY comp;
```

**Predicate support:** Both node and edge predicates are supported.

---

### LCC (Local Clustering Coefficient)

Measures how close a node's neighbors are to forming a complete graph
(clique). Values range from 0.0 to 1.0.

```cypher
CALL lcc('<graph_name>', {<options>})
RETURN node, lcc;
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `directed` | BOOL | `false` | Whether to compute the directed clustering coefficient |
| `degree_threshold` | INT | MAX_INT | Skip nodes with degree above this threshold |
| `concurrency` | INT | CPU cores | Number of threads for parallel execution |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The node |
| `lcc` | DOUBLE | Local clustering coefficient |

**Example:**

```cypher
CALL lcc('social', {degree_threshold: 1000})
RETURN node.fName, lcc
ORDER BY lcc DESC;
```

**Predicate support:** Both node and edge predicates are supported.

---

### K-Core Decomposition

Computes the core number for each node. A node has core number *k* if it
belongs to a *k*-core (a maximal subgraph where every node has degree >= *k*)
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
| `node` | NODE | The node |
| `core` | INT64 | Core number of the node |

**Example:**

```cypher
CALL kcore('social', {k: 3})
RETURN node.fName, core
ORDER BY core DESC;
```

**Predicate support:** Both node and edge predicates are supported.

---

### CDLP (Community Detection using Label Propagation)

A community detection algorithm that propagates labels through the network.
Each node is initially assigned a unique label; in each iteration, every node
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
| `node` | NODE | The node |
| `label` | INT64 | Community label assigned to this node |

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

**Predicate support:** Both node and edge predicates are supported.

**Note:** CDLP currently requires a homogeneous graph like other algorithms.
Multi-label support is planned for a future release.

---

### Louvain

A community detection algorithm that optimizes modularity by iteratively moving
nodes between communities and aggregating the graph into super-nodes.

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
| `node` | NODE | The node |
| `community` | INT64 | Community ID (0-based) |

**Example:**

```cypher
CALL louvain('social', {resolution: 1.0, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Predicate support:** Neither node nor edge predicates are supported.

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
| `node` | NODE | The node |
| `community` | INT64 | Community ID (0-based) |

**Example:**

```cypher
CALL leiden('social', {resolution: 1.5, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Predicate support:** Neither node nor edge predicates are supported.

**Leiden vs. Louvain:** Use Leiden when you need higher-quality community partitions
or better detection of small communities. Use Louvain when you need the fastest
possible execution.

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

## Common Options

All algorithms accept a `concurrency` option that controls the number of
threads used for parallel computation. The default depends on the algorithm:

- **Most algorithms:** defaults to the number of CPU cores
- **Label Propagation, Personalized PageRank:** defaults to `1`

## Limitations

- All algorithms require a **homogeneous graph** subgraph (exactly one node
  label and one edge triplet `[A, edge, A]`). Support for heterogeneous graphs
  is planned for a future release.
- Node and edge predicates are supported by all algorithms except Louvain and
  Leiden. 
- CDLP does not actually support heterogeneous graphs yet — it only processes
  the first node label and edge triplet. True multi-label support is planned.
