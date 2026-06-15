# Leiden Extension

The Leiden extension provides a community detection algorithm that improves
upon the Louvain method by adding a refinement phase. This refinement allows
communities to be split during the algorithm's execution, leading to better
detection of small communities and higher-quality partitions, especially in
large networks.

## Quick Start

```cypher
-- 1. Load the extension
LOAD leiden;

-- 2. Run community detection with default parameters
CALL leiden()
RETURN node_id, community;

-- 3. Run with custom resolution
CALL leiden(1.5)
RETURN node_id, community;

-- 4. Run with extra properties
CALL leiden(1.0, false, 1e-7, "name,age")
RETURN node_id, community, properties;
```

## Algorithm Overview

The Leiden algorithm shares the same multi-level approach as Louvain but adds
a crucial **refinement phase** between the local moving phase and the
aggregation phase:

1. **Local Moving Phase**: Same as Louvain — vertices are moved to neighboring
   communities to optimize modularity.
2. **Refinement Phase** (new): Each community from the local moving phase is
   examined, and its vertices may be split into sub-communities. This step
   prevents the "well-separated" problem that can occur in Louvain, where
   vertices that should be in separate communities get merged.
3. **Aggregation Phase**: The refined partition is used to build the aggregated
   graph for the next iteration.

This refinement step makes Leiden particularly effective for:

- Detecting small communities within large networks
- Avoiding the merging of distinct communities
- Producing higher-quality partitions with better modularity scores

## Usage

### Basic Syntax

```cypher
CALL leiden([resolution], [directed], [threshold], [prop_names])
RETURN node_id, community[, properties];
```

### Parameters

All parameters are optional:

| Parameter    | Type   | Default | Description                                                        |
|--------------|--------|---------|--------------------------------------------------------------------|
| `resolution` | DOUBLE | `1.0`   | Resolution parameter (gamma). Values > 1 favor smaller communities, values < 1 favor larger communities |
| `directed`   | BOOL   | `false` | Whether to treat the graph as directed                             |
| `threshold`  | DOUBLE | `1e-7`  | Modularity gain threshold for convergence                          |
| `prop_names` | STRING | `""`    | Comma-separated property names to include in the output as JSON    |

### Output Columns

| Column       | Type   | Description                                                     |
|--------------|--------|-----------------------------------------------------------------|
| `node_id`    | INT64  | Internal global vertex ID encoded as `(label_id << 56) \| vertex_id` |
| `community`  | INT64  | Community ID (0-based)                                          |
| `properties` | STRING | (Optional) JSON map of requested vertex property values         |

## Examples

### Basic Community Detection

```cypher
LOAD leiden;

CALL leiden()
RETURN node_id, community
ORDER BY community;
```

### Custom Resolution

```cypher
CALL leiden(2.0)
RETURN node_id, community;
```

### Directed Graph

```cypher
CALL leiden(1.0, true)
RETURN node_id, community;
```

### With Vertex Properties

```cypher
CALL leiden(1.0, false, 1e-7, "name,age")
RETURN node_id, community, properties;
```

Example output:

| node_id | community | properties                    |
|---------|-----------|-------------------------------|
| 123     | 0         | {"name":"Alice","age":30}     |
| 456     | 0         | {"name":"Bob","age":25}       |
| 789     | 1         | {"name":"Charlie","age":35}   |

## Leiden vs. Louvain

| Feature              | Louvain | Leiden |
|----------------------|---------|--------|
| Local moving phase   | Yes     | Yes    |
| Refinement phase     | No      | Yes    |
| Community splitting  | No      | Yes    |
| Small community detection | Moderate | Better |
| Scalability          | O(n)    | O(n)   |
| Modularity quality   | Good    | Better |

Use **Leiden** when:
- You need higher-quality community partitions
- Your graph has small or well-separated communities
- You want to avoid the merging problem common in Louvain

Use **Louvain** when:
- You need the fastest possible execution
- Your graph has clear, well-separated large communities
- Simplicity is preferred

## Resolution Parameter

The resolution parameter controls the scale of communities detected:

- **Low resolution (< 1)**: Tends to merge vertices into fewer, larger communities
- **Default (1.0)**: Standard modularity optimization
- **High resolution (> 1)**: Tends to split vertices into more, smaller communities

## Limitations

- The algorithm operates on the entire graph across all vertex and edge labels.
- Edge weights are not currently supported (all edges have weight 1.0).
- The `node_id` output uses an internal encoding; use it for identification
  purposes rather than direct interpretation.

## References

- Traag, V. A., Waltman, L., & van Eck, N. J. (2019). "From Louvain to Leiden:
  guaranteeing well-connected communities." *Scientific Reports*, 9(1), 5233.
