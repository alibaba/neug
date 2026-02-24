# Union Clause
The Union operator in NeuG is used to combine the results of multiple subqueries into a single result set. All participating subqueries must produce a consistent output schema—i.e., the same number of columns with matching names and data types.

Currently, NeuG supports the `UNION ALL` variant, which concatenates results without performing deduplication. Two syntactic forms are available:
- **Standard Union**: Similar to the standard syntax in [Kùzu](https://docs.kuzudb.com/cypher/query-clauses/union/).
- **Call Union**: An extended form inspired by [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union), enabling more flexible query composition.

## Standard Union

In standard usage, `UNION ALL` is used to merge the output of multiple subqueries. The union must appear as the terminal operator, combining the outputs of all preceding branches.

```cypher
MATCH (n {name: 'marko'}) RETURN n.age
UNION ALL
MATCH (n {name: 'josh'}) RETURN n.age;
```

## Call Union

Inspired by [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union), NeuG extends union semantics through a `CALL {}` block with parameterized input, enabling more expressive and modular query composition. This construct allows:
- Executing additional logic after the union.
- Sharing precomputed context (e.g., bound variables) across union branches.

Example：
```cypher
MATCH (person:person {id: 123})
WITH person
CALL (person) {
  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  RETURN friend

  UNION ALL

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight < 1.0
  RETURN friend
}
RETURN friend.id, friend.name

```

This query can be decomposed into three stages:
- **PreQuery**: Executed prior to the `CALL {}` block (e.g., MATCH (person)), perform precomputed context which will be shared across union subqueries.
- **Union Subqueries**: Defined within the `CALL {}` block. Each branch has access to the shared context (e.g., person).
- **PostQuery**: Executed after the `CALL {}`, consuming the unified result set (e.g., RETURN friend.id, friend.name).

The `CALL (person)` syntax injects external variables into the union scope, enabling each subquery to access and operate on a shared context. This pattern is particularly useful when applying multiple filtering or traversal strategies over the same input entity.