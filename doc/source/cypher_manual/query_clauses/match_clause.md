# Match Clause

The `MATCH` clause is used to search for patterns in the graph database. It allows you to find nodes, edges, and paths that match specific criteria.

## Match Nodes

### Match Nodes with Single Label

Find all nodes with a specific label. This query returns all nodes labeled as `person`.

```cypher
MATCH (p:person) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Match Nodes with Multiple Labels

Find nodes with any of the specified labels. This query returns all nodes labeled as either `person` or `software`.

**Note**: Unlike Neo4j, NeuG does not support multi-label nodes. In Neo4j, `(p:person:software)` represents nodes that have both `person` and `software` labels simultaneously. In NeuG, this syntax represents a union of nodes with either `person` or `software` labels.

```cypher
MATCH (p:person:software) RETURN p;
```

output:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
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
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Match Nodes with Conditions

In addition to label constraints, you can specify property-based filtering conditions.

```cypher
MATCH (p:person {name: 'marko'}) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
```

## Match Edges

### Match Edges with Single Label

```cypher
MATCH (p:person)-[k:knows]->(f:person) RETURN k;
```

output:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5} |
+------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

### Match Edges with Multiple Labels

```cypher
MATCH (p:person)-[k:knows|created]->(f) RETURN k;
```

output:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

### Match Edges with Any Label

```cypher
MATCH (p:person)-[k]->(f) RETURN k;
```

output:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Match Edges with Conditions

Filter edges based on their properties.

```cypher
MATCH (p:person)-[k:knows {weight: 1.0}]->(f:person) RETURN k;
```

output:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

## Match Repeated Paths

NeuG supports variable-length repeated path exploration, which is a common feature in graph queries.

### Match Repeated Path with Variable Length

Find paths with a variable number of hops. This query returns all paths consisting of 1 or 2 edges.

```cypher
MATCH (p:person)-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Match Repeated Paths with Source Conditions

Specify filtering conditions based on the source node's properties. This query finds 1-hop or 2-hop paths starting from the node with name 'marko'.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Match Repeated Paths with Target Conditions

Specify filtering conditions based on the target node's properties. This query finds paths ending at the node with name 'josh'.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f {name: 'josh'}) RETURN k;
```

output:
```
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                                 |
+===================================================================================================================================================+
| {_ID: 2, _LABEL: person}, {_ID: 2097152, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 2, _DST_ID: 0}, {_ID: 0, _LABEL: person} |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Match Repeated Paths with Edge Conditions

Reference [Kuzu's specification](https://docs.kuzudb.com/cypher/query-clauses/match/#filter-recursive-relationships), NeuG also supports property filtering on each edge during the path.

This query requires each edge in the path to satisfy the constraint `r.weight < 1.0`.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person)
Return k;
```

<!-- todo: output is incorrect -->

### Match Trail Path

Using the `TRAIL` option, you can further restrict repeated paths to ensure no edges are repeated, guaranteeing that path expansion iterations terminate without infinite loops.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* TRAIL 1..2]->(f:person)
Return k;
```

### Match Simple Path

Using the `SIMPLE` option, you can further restrict repeated paths to ensure no nodes are repeated, guaranteeing output of simple paths.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* ACYCLIC 1..2]->(f:person)
Return k;
```

### Match Unweighted Shortest Path

Specify the `SHORTEST` option to output the unweighted shortest path between two given nodes.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* SHORTEST 1..2]->(f:person {name: 'josh'})
RETURN k;
```

## Match Patterns

The `MATCH` clause supports complex pattern matching that combines nodes, edges, and conditions in various ways to express sophisticated graph queries.

Below are some classic graph query patterns that are widely used in various graph query benchmarks:
- Triangle Pattern
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
Where a.name <> b.name AND b.name <> c.name
Return count(*);
```

- Square Pattern
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

- Long Path
```
Match (a:person)-[:knows]->(b:person),
      (b:person)-[:knows]->(c:person),
      (c:person)-[:created]->(d:software),
      (d:software)<-[:created]-(e:person),
      (e:person)-[:knows]->(f:person)
Where a.name <> b.name AND b.name <> c.name 
    AND c.name <> d.name AND d.name <> e.name
Return count(*);
```

- Clique Path
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person),
      (a:person)-[:knows]->(c:person),
      (d:person)-[:created]->(b:software)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

## Optional Match

The `OPTIONAL MATCH` clause allows you to match patterns that may or may not exist in the graph, returning `null` for parts of the pattern that don't match.

Here's how to use Optional Match:

```cypher
MATCH (a:person)-[:knows]->(b:person)
OPTIONAL MATCH (b:person)-[:created]->(c:software)
RETURN a.name, b.name, c.name
```

<!-- todo: the feature is not included in current pip package -->

In the output results above, for each (a, b) pair:
- If b has connected nodes c, all (a, b, c) triples are returned. For example, for ('marko', 'josh'), the corresponding triples are {('marko', 'josh', 'lop'), ('marko', 'josh', 'ripple')}.
- If b has no connected nodes c, a row with c=null is preserved for the current (a,b) pair. For example, for ('marko', 'vadas'), the output triple is {('marko', 'vadas', null)}.

This is the main purpose of the OPTIONAL MATCH clause - to preserve rows from the main MATCH even when the optional pattern doesn't match.
