# Where Clause

The WHERE clause is used to further filter the results produced by previous query operations based on predicates or subqueries. The filtering is primarily based on logical expressions, which we will introduce in detail in the [expression section](../../expression). It only outputs data that meets the specified conditions.

## Filter by Properties

In the previous chapter, we introduced how to restrict node and relationship property key-value pairs through expressions like `(a:person {name: 'marko'})`. Here we further supplement how to express the same effect through the WHERE clause.

### Filter by Node Properties
```cypher
MATCH (a:person) 
WHERE a.name = 'marko' OR a.age > 27
RETURN a.name, a.age;
```

output:
```
+-------------+------------+
| _0_a.name   |   _0_a.age |
+=============+============+
| marko       |         29 |
+-------------+------------+
| josh        |         32 |
+-------------+------------+
| peter       |         35 |
+-------------+------------+
```

### Filter by Node/Relationship Properties
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name = 'marko' AND b.weight = 1.0
RETURN a.name, b.weight;
```

output:
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| marko       |             1 |
+-------------+---------------+
```

### Filter by Correlated Properties
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name <> c.name AND a.age > c.age 
RETURN a.name, a.age, c.name, c.age;
```

output:
```
+-------------+------------+-------------+------------+
| _0_a.name   |   _0_a.age | _2_c.name   |   _2_c.age |
+=============+============+=============+============+
| marko       |         29 | vadas       |         27 |
+-------------+------------+-------------+------------+
```

## Filter with NULL

NULL values are inevitable in graph data storage and computation processes. To preserve or remove these NULL values, we can use `IS NULL` or `IS NOT NULL` in the WHERE clause.

### Filter Property Data with NULL
```cypher
MATCH (a) 
WHERE a.age IS NULL 
RETURN a.name;
```

```
+-------------+
| _0_a.name   |
+=============+
| lop         |
+-------------+
| ripple      |
+-------------+
```

### Filter Optional Data with NULL
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### Filter Out Optional Data with IS NOT NULL
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## WHERE with Subquery

The WHERE clause can also be used with subqueries to perform more complex filtering operations.

### Exists Pattern
```cypher
MATCH (a) 
WHERE (a)-[:knows]->(b) 
RETURN a.name;
```
This query returns all `a.name` values that have a `knows` relationship.

### Not Exists Pattern
```cypher
MATCH (a) 
WHERE NOT (a)-[:knows]->(b) 
RETURN a.name;
```
This query returns all `a.name` values where there are no `knows` relationships, equivalent to the ANTI_JOIN semantics in SQL.

<!-- todo: where subquery is unsupported yet -->

