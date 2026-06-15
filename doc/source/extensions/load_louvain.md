# Louvain Extension

The Louvain extension provides a community detection algorithm for graph
clustering. It implements the multi-level Louvain method, which optimizes
modularity by iteratively moving vertices between communities and aggregating
the graph into super-nodes.

## Quick Start

```cypher
-- 1. Load the extension
LOAD louvain;

-- 2. Run community detection with default parameters
CALL louvain()
RETURN node_id, community;

-- 3. Run with custom resolution
CALL louvain(1.5)
RETURN node_id, community;

-- 4. Run with extra properties
CALL louvain(1.0, false, 1e-7, "name,age")
RETURN node_id, community, properties;
```

## Algorithm Overview

The Louvain algorithm detects communities in large networks by optimizing
modularity, a measure of the density of links inside communities compared
to links between communities. The algorithm works in two phases that are
repeated iteratively:

1. **Local Moving Phase**: Each vertex is moved to the neighboring community
   that yields the largest increase in modularity.
2. **Aggregation Phase**: Communities are aggregated into super-nodes, forming
   a new, smaller graph. The two phases repeat until no further improvement
   is possible.

## Usage

### Basic Syntax

```cypher
CALL louvain([resolution], [directed], [threshold], [prop_names])
RETURN node_id, community[, properties];
```

### Parameters

All parameters are optional:

| Parameter   | Type   | Default | Description                                                        |
|-------------|--------|---------|--------------------------------------------------------------------|
| `resolution`| DOUBLE | `1.0`   | Resolution parameter (gamma). Values > 1 favor smaller communities, values < 1 favor larger communities |
| `directed`  | BOOL   | `false` | Whether to treat the graph as directed                             |
| `threshold` | DOUBLE | `1e-7`  | Modularity gain threshold for convergence                          |
| `prop_names`| STRING | `""`    | Comma-separated property names to include in the output as JSON    |

### Output Columns

| Column       | Type   | Description                                                     |
|--------------|--------|-----------------------------------------------------------------|
| `node_id`    | INT64  | Internal global vertex ID encoded as `(label_id << 56) \| vertex_id` |
| `community`  | INT64  | Community ID (0-based)                                          |
| `properties` | STRING | (Optional) JSON map of requested vertex property values         |

## Examples

### Basic Community Detection

```cypher
LOAD louvain;

CALL louvain()
RETURN node_id, community
ORDER BY community;
```

### Custom Resolution

Higher resolution values produce more, smaller communities:

```cypher
CALL louvain(2.0)
RETURN node_id, community;
```

### Directed Graph

```cypher
CALL louvain(1.0, true)
RETURN node_id, community;
```

### With Vertex Properties

Return vertex properties alongside community assignments:

```cypher
CALL louvain(1.0, false, 1e-7, "name,age")
RETURN node_id, community, properties;
```

Example output:

| node_id | community | properties                    |
|---------|-----------|-------------------------------|
| 123     | 0         | {"name":"Alice","age":30}     |
| 456     | 0         | {"name":"Bob","age":25}       |
| 789     | 1         | {"name":"Charlie","age":35}   |

## Resolution Parameter

The resolution parameter controls the scale of communities detected:

- **Low resolution (< 1)**: Tends to merge vertices into fewer, larger communities
- **Default (1.0)**: Standard modularity optimization
- **High resolution (> 1)**: Tends to split vertices into more, smaller communities

Choosing the right resolution depends on the graph structure and the desired
granularity of community detection.

## Limitations

- The algorithm operates on the entire graph across all vertex and edge labels.
- Edge weights are not currently supported (all edges have weight 1.0).
- The `node_id` output uses an internal encoding; use it for identification
  purposes rather than direct interpretation.
