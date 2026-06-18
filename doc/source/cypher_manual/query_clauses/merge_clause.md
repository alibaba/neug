# Merge Clause

The `MERGE` clause ensures that a pattern exists in the graph. If the pattern already exists, it is matched and returned; otherwise, it is created. You can think of `MERGE <pattern>` as: if `MATCH <pattern>` succeeds then return the result, otherwise `CREATE <pattern>`.

Additionally, `MERGE` supports `ON CREATE SET` and `ON MATCH SET` operations, which allow you to specify different property updates depending on whether the pattern was newly created or already existed.

## MERGE Nodes

### Match an Existing Node

If the node already exists in the graph, `MERGE` simply matches it and returns the result without creating anything new.

```cypher
MERGE (n:User {name: 'Adam'}) RETURN n.*;
```

Assuming `Adam` already exists in the graph, this query returns Adam's properties directly.

### Create a New Node

If the node does not exist, `MERGE` acts like `CREATE` and inserts the node.

```cypher
MERGE (n:User {name: 'Bob', age: 45}) RETURN n.*;
```

If `Bob` does not exist in the graph, a new `User` node with `name='Bob'` and `age=45` is created.

## MERGE Edges

When merging edges, the source and destination nodes must be matched first (typically via a preceding `MATCH` clause), and then `MERGE` checks whether the specified edge exists between them.

### Match an Existing Edge

```cypher
MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'})
MERGE (u1)-[e:FOLLOWS {date: 2012}]->(u2)
RETURN u1.name, e.date, u2.name;
```

If an edge `FOLLOWS` with `date=2012` already exists between Adam and marko, it is simply returned.

### Create a New Edge

```cypher
MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'})
MERGE (u1)-[e:FOLLOWS {date: 2012}]->(u2)
RETURN u1.name, e.date, u2.name;
```

If no `FOLLOWS` edge with `date=2012` exists between Adam and Bob, a new edge is created.

### MERGE Edge for Multiple Pairs

```cypher
MATCH (u1:User), (u2:User)
WHERE id(u1) < id(u2)
MERGE (u1)-[e:FOLLOWS {date: 2012}]->(u2)
RETURN u1.name, e.date, u2.name;
```

For each `<u1, u2>` pair, `MERGE` checks individually whether a `FOLLOWS` edge with `date=2012` exists. If it does, the existing edge is returned; otherwise, a new edge is created for that pair.

## ON CREATE / ON MATCH

`ON CREATE SET` executes the SET operation only when the pattern is newly created. `ON MATCH SET` executes the SET operation only when the pattern already existed in the graph.

### ON CREATE SET

```cypher
MERGE (n:User {name: 'Bob'}) ON CREATE SET n.age = 60 RETURN n.name, n.age;
```

If `Bob` does not exist and is newly created, the `SET n.age = 60` is applied:

```
┌────────┬───────┐
│ n.name │ n.age │
│ STRING │ INT64 │
├────────┼───────┤
│ Bob    │ 60    │
└────────┴───────┘
```

### ON MATCH SET

```cypher
MERGE (n:User {name: 'Adam'}) ON MATCH SET n.age = 35 RETURN n.name, n.age;
```

If `Adam` already exists, the `SET n.age = 35` is applied:

```
┌────────┬───────┐
│ n.name │ n.age │
│ STRING │ INT64 │
├────────┼───────┤
│ Adam   │ 35    │
└────────┴───────┘
```

### ON CREATE SET Does Not Fire on Match

```cypher
MERGE (n:User {name: 'Adam'}) ON CREATE SET n.age = 35 RETURN n.name, n.age;
```

If `Adam` already exists, `ON CREATE SET` is **not** triggered and the original property value is preserved:

```
┌────────┬───────┐
│ n.name │ n.age │
│ STRING │ INT64 │
├────────┼───────┤
│ Adam   │ 29    │
└────────┴───────┘
```

## Limitations

The following MERGE patterns are **not** supported in NeuG.

### MERGE Multiple Nodes

Merging multiple nodes in a single `MERGE` clause is not supported:

```cypher
-- NOT supported
MERGE (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'})
```

This has ambiguous semantics: if either node is missing, both nodes would be re-created regardless of whether one already exists. Instead, use separate `MERGE` statements:

```cypher
-- Supported: merge each node individually
MERGE (u1:User {name: 'Adam'})
MERGE (u2:User {name: 'Bob'})
```

### MERGE Path

Merging a full path pattern is not supported:

```cypher
-- NOT supported
MERGE (:User {name: 'A'})-[:FOLLOWS {date: 2012}]->(:User {name: 'B'})
```

This would create all vertices and the edge as a whole without checking whether individual vertices already exist, potentially causing duplicates. Instead, merge the nodes and edge separately:

```cypher
-- Supported: merge nodes first, then merge the edge
MERGE (u1:User {name: 'A'})
MERGE (u2:User {name: 'B'})
MERGE (u1)-[:FOLLOWS {date: 2012}]->(u2)
```
