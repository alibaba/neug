# Match Clause

The `MATCH` clause is used to search for patterns in the graph database. It allows you to find nodes, edges, and paths that match specific criteria.

## Match Nodes

### Match Nodes with Single Label

Find all nodes with a specific label. This query returns all nodes labeled as `person`.

```cypher
MATCH (p:Person) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: Person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: Person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: Person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Match Nodes with Multiple Labels

Find nodes with any of the specified labels. This query returns all nodes labeled as either `person` or `software`.

**Note**: Unlike Neo4j, NeuG does not support multi-label nodes. In Neo4j, `(p:Person:Software)` represents nodes that have both `person` and `software` labels simultaneously. In NeuG, this syntax represents a union of nodes with either `person` or `software` labels.

```cypher
MATCH (p:Person:Software) RETURN p;
```

output:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: Person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: Person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: Person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: Software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: Software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

### Match Nodes with Any Label

Match nodes without specifying a label. NeuG supports queries without explicit labels and infers unknown labels automatically during compilation based on defined schema constraints.

```cypher
MATCH (p) RETURN p;
```

output:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: Person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: Person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: Person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: Software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: Software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Match Nodes with Conditions

In addition to label constraints, you can specify property-based filtering conditions.

```cypher
MATCH (p:Person {name: 'marko'}) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: Person, name: marko, age: 29} |
+-------------------------------------------------------+
```

## Match Edges

### Match Edges with Single Label

```cypher
MATCH (p:Person)-[k:KNOWS]->(f:Person) RETURN k;
```

output:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 1, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5} |
+------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

### Match Edges with Multiple Labels

```cypher
MATCH (p:Person)-[k:KNOWS|CREATED]->(f) RETURN k;
```

output:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

### Match Edges with Any Label

```cypher
MATCH (p:Person)-[k]->(f) RETURN k;
```

output:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Match Edges with Conditions

Filter edges based on their properties.

```cypher
MATCH (p:Person)-[k:KNOWS {weight: 1.0}]->(f:Person) RETURN k;
```

output:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

## Match Repeated Paths

NeuG supports variable-length repeated path exploration, which is a common feature in graph queries.

### Match Repeated Path with Variable Length

Find paths with a variable number of hops. This query returns all paths consisting of 1 or 2 edges.

```cypher
MATCH (p:Person)-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Match Repeated Paths with Source Conditions

Specify filtering conditions based on the source node's properties. This query finds 1-hop or 2-hop paths starting from the node with name 'marko'.

```cypher
MATCH (p:Person {name: 'marko'})-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Match Repeated Paths with Target Conditions

Specify filtering conditions based on the target node's properties. This query finds paths ending at the node with name 'josh'.

```cypher
MATCH (p:Person {name: 'marko'})-[k*1..2]->(f {name: 'josh'}) RETURN k;
```

output:
```
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                                 |
+===================================================================================================================================================+
| {_ID: 2, _LABEL: Person}, {_ID: 2097152, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 2, _DST_ID: 0}, {_ID: 0, _LABEL: Person} |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Match Repeated Paths with Edge Conditions

Reference [Kuzu's specification](https://docs.kuzudb.com/cypher/query-clauses/match/#filter-recursive-relationships), NeuG also supports property filtering on each edge during the path.

This query requires each edge in the path to satisfy the constraint `r.weight < 1.0`.

```cypher
MATCH (p:Person {name: 'marko'})-[k:KNOWS*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:Person)
RETURN k;
```

<!-- todo: output is incorrect -->

### Match Trail Path

Using the `TRAIL` option, you can further restrict repeated paths to ensure no edges are repeated, guaranteeing that path expansion iterations terminate without infinite loops.

```cypher
MATCH (p:Person {name: 'marko'})-[k:KNOWS* TRAIL 1..2]->(f:Person)
RETURN k;
```

### Match Simple Path

Using the `ACYCLIC` option, you can further restrict repeated paths to ensure no nodes are repeated, guaranteeing output of simple paths.

```cypher
MATCH (p:Person {name: 'marko'})-[k:KNOWS* ACYCLIC 1..2]->(f:Person)
RETURN k;
```

### Match Unweighted Shortest Path

Specify the `SHORTEST` option to output the unweighted shortest path between two given nodes.

```cypher
MATCH (p:Person {name: 'marko'})-[k:KNOWS* SHORTEST 1..2]->(f:Person {name: 'josh'})
RETURN k;
```

### Named Path

A path can be bound to a variable with `p =` and returned or passed to a path function. A named path can contain one or more relationship segments. Each segment can be either a single edge or a repeated path, and the relationship may also have its own variable.

```cypher
MATCH p = (a:Person {name: 'marko'})-[:KNOWS]->(b:Person {name: 'vadas'})
RETURN p AS path;
```

output:
```
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| path                                                                                                                                                                                                 |
+======================================================================================================================================================================================================+
| {nodes: {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}, {_ID: 1, _LABEL: person, id: 2, name: vadas, age: 27}, rels: {_ID: 1, _LABEL: knows, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}, length: 1} |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

```cypher
MATCH p = (a:Person {name: 'marko'})-[r:KNOWS*1..3]->(b:Person {name: 'josh'})
RETURN LENGTH(p) AS path_length, LENGTH(r) AS relationship_path_length;
```

output:
```
+-------------+----------------------------+
| path_length | relationship_path_length   |
+=============+============================+
| 1           | 1                          |
+-------------+----------------------------+
```

Multiple single-edge segments are concatenated into one path:

```cypher
MATCH p = (a:Person {name: 'marko'})-[]->
          (b:Person {name: 'josh'})-[]->
          (c:Software {name: 'lop'})
RETURN LENGTH(p) AS path_length,
       PROPERTIES(NODES(p), 'name') AS node_names,
       PROPERTIES(RELS(p), 'weight') AS rel_weights;
```

output:
```
+-------------+-------------------+-------------+
| path_length | node_names        | rel_weights |
+=============+===================+=============+
| 2           | marko, josh, lop  | 1.0, 0.4    |
+-------------+-------------------+-------------+
```

Repeated and single-edge segments can also be combined:

```cypher
MATCH p = (a:Person {name: 'marko'})-[r:KNOWS*1..2]-
          (b:Person {name: 'josh'})-[:CREATED]->
          (c:Software {name: 'ripple'})
RETURN LENGTH(p) AS path_length, LENGTH(r) AS repeated_segment_length;
```

output:
```
+-------------+-------------------------+
| path_length | repeated_segment_length |
+=============+=========================+
| 2           | 1                       |
+-------------+-------------------------+
```

Named paths can be used with path functions such as `LENGTH`, `NODES`, `RELS`, `PROPERTIES`, and `COST`:

```cypher
MATCH p = (a:Person {name: 'marko'})-[:KNOWS*1..3]->(b:Person {name: 'josh'})
RETURN LENGTH(p) AS path_length,
       NODES(p) AS path_nodes,
       RELS(p) AS path_rels,
       PROPERTIES(NODES(p), 'name') AS node_names,
       PROPERTIES(RELS(p), 'weight') AS rel_weights;
```

output:
```
+-------------+-------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+-------------+-------------+
| path_length | path_nodes                                                                                                  | path_rels                                                    | node_names  | rel_weights |
+=============+=============================================================================================================+==============================================================+=============+=============+
| 1           | {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}, {_ID: 2, _LABEL: person, id: 4, name: josh, age: 32} | {_ID: 2, _LABEL: knows, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} | marko, josh | 1.0         |
+-------------+-------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+-------------+-------------+
```

`COST` remains restricted to a named path containing exactly one weighted repeated segment; it is not supported for a concatenated path.

```cypher
MATCH p = (a:Person {name: 'marko'})
          -[:KNOWS* WSHORTEST(weight) 1..3]->
          (b:Person {name: 'josh'})
RETURN p AS path, COST(p) AS path_cost;
```

output:
```
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| path                                                                                                                                                                                                | path_cost |
+=====================================================================================================================================================================================================+===========+
| {nodes: {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}, {_ID: 2, _LABEL: person, id: 4, name: josh, age: 32}, rels: {_ID: 2, _LABEL: knows, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}, length: 1} | 1.0       |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
```

For the available path functions and their usage, see [Repeated Path Function](../expression/graph_func.md#repeated-path-function).

## Match Patterns

The `MATCH` clause supports complex pattern matching that combines nodes, edges, and conditions in various ways to express sophisticated graph queries.

Below are some classic graph query patterns that are widely used in various graph query benchmarks:
- Triangle Pattern
```
MATCH (a:Person)-[:CREATED]->(b:Software),
      (c:Person)-[:CREATED]->(b:Software),
      (a:Person)-[:KNOWS]->(c:Person)
WHERE a.name <> b.name AND b.name <> c.name
RETURN count(*);
```

- Square Pattern
```
MATCH (a:Person)-[:CREATED]->(b:Software),
      (c:Person)-[:CREATED]->(b:Software),
      (a:Person)-[:KNOWS]->(d:Person),
      (c:Person)<-[:KNOWS]-(d:Person)
WHERE a.name <> b.name AND b.name <> c.name AND c.name <> d.name
RETURN count(*);
```

- Long Path
```
MATCH (a:Person)-[:KNOWS]->(b:Person),
      (b:Person)-[:KNOWS]->(c:Person),
      (c:Person)-[:CREATED]->(d:Software),
      (d:Software)<-[:CREATED]-(e:Person),
      (e:Person)-[:KNOWS]->(f:Person)
WHERE a.name <> b.name AND b.name <> c.name
    AND c.name <> d.name AND d.name <> e.name
RETURN count(*);
```

- Clique Path
```
MATCH (a:Person)-[:CREATED]->(b:Software),
      (c:Person)-[:CREATED]->(b:Software),
      (a:Person)-[:KNOWS]->(d:Person),
      (c:Person)<-[:KNOWS]-(d:Person),
      (a:Person)-[:KNOWS]->(c:Person),
      (d:Person)-[:CREATED]->(b:Software)
WHERE a.name <> b.name AND b.name <> c.name AND c.name <> d.name
RETURN count(*);
```

## Optional Match

The `OPTIONAL MATCH` clause allows you to match patterns that may or may not exist in the graph, returning `null` for parts of the pattern that don't match.

Here's how to use Optional Match:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
OPTIONAL MATCH (b:Person)-[:CREATED]->(c:Software)
RETURN a.name, b.name, c.name
```

<!-- todo: the feature is not included in current pip package -->

In the output results above, for each (a, b) pair:
- If b has connected nodes c, all (a, b, c) triples are returned. For example, for ('marko', 'josh'), the corresponding triples are {('marko', 'josh', 'lop'), ('marko', 'josh', 'ripple')}.
- If b has no connected nodes c, a row with c=null is preserved for the current (a,b) pair. For example, for ('marko', 'vadas'), the output triple is {('marko', 'vadas', null)}.

This is the main purpose of the OPTIONAL MATCH clause - to preserve rows from the main MATCH even when the optional pattern doesn't match.
