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
{'[person, studyAt, organisation]': 'r.year > 2010'}
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

> **Note:** Most algorithms (except Label Propagation, Leiden, and Louvain) require
> a **homogeneous graph** subgraph — exactly one node label and one edge triplet where
> the source and destination labels match the node label. 

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
| `path` | PATH | Shortest path from source to this node (optional, only when YIELDed) |

**Example:**

```cypher
CALL bfs('social', {source: '0'})
RETURN node.fName, distance
ORDER BY distance;
```

**With path return:**

```cypher
-- Return the actual shortest path
CALL bfs('social', {source: '0'})
YIELD node, distance, path
RETURN node.fName, distance, path;

-- Extract path details
CALL bfs('social', {source: '0'})
YIELD node, distance, path
RETURN node.fName, distance,
       nodes(path) AS path_nodes,
       relationships(path) AS path_edges;
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
| `path` | PATH | Shortest path from source to this node (optional, only when YIELDed) |

**Example:**

```cypher
CALL sssp('social', {source: '0', weight: 'cost', directed: true})
RETURN node.fName, distance;
```

**With path return:**

```cypher
-- Return the actual shortest path
CALL sssp('social', {source: '0', weight: 'cost'})
YIELD node, distance, path
RETURN node.fName, distance, path;

-- Find path to a specific target
CALL sssp('social', {source: '0', weight: 'cost'})
YIELD node, distance, path
WHERE node.id = '42'
RETURN distance, path;
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
    {'[person, studyAt, organisation]': 'r.year > 2010'}
);
LOAD gds;

CALL cdlp('study_net', {concurrency: 10})
RETURN node.id, node.fName, node.name, label;
```

**Predicate support:** Both node and edge predicates are supported.

**Note:** CDLP currently requires a homogeneous graph like most other algorithms.
Multi-label support is planned for a future release.

---

### Louvain

A community detection algorithm that optimizes modularity by iteratively moving
nodes between communities and aggregating the graph into super-nodes.

Louvain supports **multi-label graphs** — a projected graph with multiple vertex
labels and multiple edge triplets is treated as one unified graph.

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
| `initial_community_property` | STRING | `""` | Vertex property name to seed community IDs for incremental updates. When set, existing vertices are frozen by default (freeze-assign mode). |
| `allow_relocation` | BOOL | `false` | When `true`, allows existing vertices to be re-assigned to different communities (warm-start mode). When `false` (default), existing vertices keep their community assignments unchanged. |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The node |
| `community` | INT64 | Community ID (0-based) |
| `previous_community` | INT64 | *(optional)* Previous community ID. Only available when `initial_community_property` is set. NULL for vertices that had no previous community assignment. |

**Example (single-label):**

```cypher
CALL louvain('social', {resolution: 1.0, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Example (multi-label graph):**

```cypher
CALL project_graph(
    'social_multi',
    ['person', 'organisation'],
    {'[person, knows, person]': '', '[person, studyAt, organisation]': ''}
);

CALL louvain('social_multi', {concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Incremental mode:** To preserve community IDs across runs, write back the
results and re-run with `initial_community_property`. By default, this uses
**freeze-assign** mode: existing vertices keep their community assignments
unchanged, and only new vertices (without a previous community) are clustered.

```cypher
-- 1. Add a property column to store community IDs
ALTER TABLE person ADD COLUMN comm INT64 DEFAULT -1;

-- 2. First run: compute communities and write back to vertex property
CALL louvain('social', {concurrency: 8}) YIELD node, community
MATCH (n:person) WHERE n.id = node.id
SET n.comm = community;

-- 3. Re-run with freeze-assign (default): old communities frozen, new vertices assigned
CALL louvain('social', {initial_community_property: 'comm', concurrency: 8})
RETURN node.fName, community
ORDER BY community;

-- 3b. Alternative: warm-start mode (allow old vertices to be re-assigned)
CALL louvain('social', {initial_community_property: 'comm', allow_relocation: true, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Incremental delta analysis:** When running with `initial_community_property`,
you can YIELD the optional `previous_community` column to analyze community
changes at the vertex level. In freeze-assign mode, `previous_community` is
`NULL` for new vertices and equals `community` for existing vertices.

```cypher
-- Migration matrix: how vertices moved between communities
CALL louvain('social', {initial_community_property: 'comm', allow_relocation: true, concurrency: 8})
YIELD node, community, previous_community
RETURN previous_community, community, count(*) AS members
ORDER BY previous_community, community;

-- New vertices (freeze-assign mode): previous_community is NULL
CALL louvain('social', {initial_community_property: 'comm', concurrency: 8})
YIELD node, community, previous_community
WHERE previous_community IS NULL
RETURN node.id, community;
```

**Predicate support:** Neither node nor edge predicates are supported.

---

### Leiden

A community detection algorithm that improves upon Louvain by adding a refinement
phase. This refinement allows communities to be split during execution, leading
to better detection of small communities and higher-quality partitions.

Leiden supports **multi-label graphs** — a projected graph with multiple vertex
labels and multiple edge triplets is treated as one unified graph.

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
| `initial_community_property` | STRING | `""` | Vertex property name to seed community IDs for incremental updates. When set, existing vertices are frozen by default (freeze-assign mode). |
| `allow_relocation` | BOOL | `false` | When `true`, allows existing vertices to be re-assigned to different communities (warm-start mode). When `false` (default), existing vertices keep their community assignments unchanged. |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `node` | NODE | The node |
| `community` | INT64 | Community ID (0-based) |
| `previous_community` | INT64 | *(optional)* Previous community ID. Only available when `initial_community_property` is set. NULL for vertices that had no previous community assignment. |

**Example (single-label):**

```cypher
CALL leiden('social', {resolution: 1.5, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Example (multi-label graph):**

```cypher
CALL project_graph(
    'social_multi',
    ['person', 'organisation'],
    {'[person, knows, person]': '', '[person, studyAt, organisation]': ''}
);

CALL leiden('social_multi', {concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Incremental mode:** To preserve community IDs across runs, write back the
results and re-run with `initial_community_property`. By default, this uses
**freeze-assign** mode: existing vertices keep their community assignments
unchanged, and only new vertices (without a previous community) are clustered.

```cypher
-- 1. Add a property column to store community IDs
ALTER TABLE person ADD COLUMN comm INT64 DEFAULT -1;

-- 2. First run: compute communities and write back to vertex property
CALL leiden('social', {concurrency: 8}) YIELD node, community
MATCH (n:person) WHERE n.id = node.id
SET n.comm = community;

-- 3. Re-run with freeze-assign (default): old communities frozen, new vertices assigned
CALL leiden('social', {initial_community_property: 'comm', concurrency: 8})
RETURN node.fName, community
ORDER BY community;

-- 3b. Alternative: warm-start mode (allow old vertices to be re-assigned)
CALL leiden('social', {initial_community_property: 'comm', allow_relocation: true, concurrency: 8})
RETURN node.fName, community
ORDER BY community;
```

**Incremental delta analysis:** When running with `initial_community_property`,
you can YIELD the optional `previous_community` column to analyze community
changes at the vertex level. In freeze-assign mode, `previous_community` is
`NULL` for new vertices and equals `community` for existing vertices.

```cypher
-- Migration matrix: how vertices moved between communities
CALL leiden('social', {initial_community_property: 'comm', allow_relocation: true, concurrency: 8})
YIELD node, community, previous_community
RETURN previous_community, community, count(*) AS members
ORDER BY previous_community, community;

-- New vertices (freeze-assign mode): previous_community is NULL
CALL leiden('social', {initial_community_property: 'comm', concurrency: 8})
YIELD node, community, previous_community
WHERE previous_community IS NULL
RETURN node.id, community;
```

**Predicate support:** Neither node nor edge predicates are supported.

**Leiden vs. Louvain:** Use Leiden when you need higher-quality community partitions
or better detection of small communities. Use Louvain when you need the fastest
possible execution.

## Shortest Path Return

BFS and SSSP algorithms support returning the actual shortest path (sequence of nodes
and relationships) in addition to the distance. The path is returned as a `PATH` type
that supports all standard Cypher path functions.

### Requesting the Path

The path column is optional and only returned when explicitly YIELDed:

```cypher
-- Without path (default, fastest)
CALL bfs('graph', {source: '0'})
RETURN node, distance;

-- With path (returns shortest path from source to each node)
CALL bfs('graph', {source: '0'})
YIELD node, distance, path
RETURN node, distance, path;
```


### Working with Paths

Use standard Cypher path functions to extract information:

```cypher
-- Get nodes and relationships in the path
CALL bfs('graph', {source: '0'})
YIELD node, distance, path
RETURN nodes(path) AS path_nodes,
       relationships(path) AS path_rels,
       length(path) AS path_length;

-- Filter paths by length
CALL bfs('graph', {source: '0'})
YIELD node, distance, path
WHERE length(path) > 2
RETURN node, distance, path;

-- Find specific target
CALL sssp('graph', {source: '0', weight: 'cost'})
YIELD node, distance, path
WHERE node.id = '42'
RETURN distance, path;
```

### Performance Considerations

- **Zero overhead when not YIELDed**: If you don't request the `path` column, there is
  no performance penalty compared to distance-only queries.
- **Default is full mode**: By default, paths include all vertex and edge properties,
  matching the behavior of standard `MATCH p = ...` queries for backward compatibility.
- **Large result sets**: When returning paths for many nodes, be aware that
  path serialization includes all vertex and edge properties, which can be
  memory-intensive for large graphs.

## Algorithm Summary

| Algorithm | CALL Name | Output Columns | Key Options |
|---|---|---|---|
| PageRank | `page_rank` | `node`, `rank` | `damping_factor`, `max_iterations`, `directed` |
| BFS | `bfs` | `node`, `distance`, `path` | `source` (required), `directed` |
| SSSP | `sssp` | `node`, `distance`, `path` | `source` (required), `weight`, `directed` |
| WCC | `wcc` | `node`, `comp` | `concurrency` |
| LCC | `lcc` | `node`, `lcc` | `directed`, `degree_threshold` |
| K-Core | `kcore` | `node`, `core` | `k` |
| CDLP | `cdlp` | `node`, `label` | `max_iterations` |
| Louvain | `louvain` | `node`, `community`, `previous_community` *(optional)* | `resolution`, `directed`, `threshold`, `concurrency`, `initial_community_property`, `allow_relocation` |
| Leiden | `leiden` | `node`, `community`, `previous_community` *(optional)* | `resolution`, `directed`, `threshold`, `concurrency`, `initial_community_property`, `allow_relocation` |

**Note:** The `path` column for BFS and SSSP is optional and only returned when explicitly YIELDed. The `previous_community` column for Louvain and Leiden is optional and only available when `initial_community_property` is set. See the individual algorithm sections for details.

## Common Options

All algorithms accept a `concurrency` option that controls the number of
threads used for parallel computation. The default depends on the algorithm:

- **Most algorithms:** defaults to the number of CPU cores
- **Label Propagation, Personalized PageRank:** defaults to `1`

## Limitations

- Most algorithms require a **homogeneous graph** subgraph (exactly one node
  label and one edge triplet `[A, edge, A]`). **Leiden and Louvain** are the
  exception — they support multi-label graphs with multiple vertex labels and
  edge triplets. Support for heterogeneous graphs in other algorithms is planned
  for a future release.
- Node and edge predicates are supported by all algorithms except Louvain and
  Leiden. 
- CDLP does not actually support heterogeneous graphs yet — it only processes
  the first node label and edge triplet. True multi-label support is planned.
