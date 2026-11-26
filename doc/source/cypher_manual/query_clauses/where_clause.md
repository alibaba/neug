# Where Clause

The WHERE clause is used to further filter the results produced by previous query operations based on predicates or subqueries. The filtering is primarily based on logical expressions, which we will introduce in detail in the [expression section](../../expression). It only outputs data that meets the specified conditions.

## Filter by Properties

In the previous chapter, we introduced how to restrict node and relationship property key-value pairs through expressions like `(a:Person {name: 'marko'})`. Here we further supplement how to express the same effect through the WHERE clause.

### Filter by Node Properties
```cypher
MATCH (a:Person) 
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
MATCH (a:Person)-[b:KNOWS]->(c:Person) 
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
MATCH (a:Person)-[b:KNOWS]->(c:Person) 
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
OPTIONAL MATCH (a)-[:KNOWS]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### Filter Out Optional Data with IS NOT NULL
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:KNOWS]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## WHERE with Subquery

The WHERE clause can also be used with subqueries to perform more complex filtering operations.

### Exists Pattern
```cypher
MATCH (a) 
WHERE (a)-[:KNOWS]->(b) 
RETURN a.name;
```
This query returns all `a.name` values that have a `knows` relationship.

### Not Exists Pattern
```cypher
MATCH (a) 
WHERE NOT (a)-[:KNOWS]->(b) 
RETURN a.name;
```
This query returns all `a.name` values where there are no `knows` relationships, equivalent to the ANTI_JOIN semantics in SQL.

### EXISTS and NOT EXISTS Subqueries

`WHERE EXISTS { MATCH ... }` keeps an outer row when the subquery finds at
least one matching row. `WHERE NOT EXISTS { MATCH ... }` keeps it when the
subquery finds none. Multiple inner matches do not duplicate the outer row.

A correlated subquery refers to a variable or property from the outer query.
NeuG supports property-equality correlation, including multiple equalities
joined with `AND`, and equality against a property projected through `WITH`.

The following examples use this small dataset:

```cypher
CREATE NODE TABLE Note(id INT64 PRIMARY KEY, text STRING);
CREATE NODE TABLE Tag(id INT64 PRIMARY KEY, note_id INT64, text STRING);
CREATE (:Note {id: 0, text: 'a'}), (:Note {id: 1, text: 'b'}),
       (:Note {id: 2, text: 'c'});
CREATE (:Tag {id: 0, note_id: 0, text: 'a'}),
       (:Tag {id: 1, note_id: 0, text: 'a'});
```

This query returns only `0`, even though two tags match:

```cypher
MATCH (m:Note)
WHERE EXISTS {
    MATCH (t:Tag) WHERE t.note_id = m.id AND t.text = m.text
}
RETURN m.id ORDER BY m.id;
```

The negated query returns `1` and `2`:

```cypher
MATCH (m:Note)
WHERE NOT EXISTS { MATCH (t:Tag) WHERE t.note_id = m.id }
RETURN m.id ORDER BY m.id;
```

A projected property can also be the correlation key. This query returns `0`:

```cypher
MATCH (m:Note) WITH m.id AS mid
WHERE EXISTS { MATCH (t:Tag) WHERE t.note_id = mid }
RETURN mid ORDER BY mid;
```

### Current Subquery Limitations

The supported equality forms above do not imply support for all Cypher
subqueries. The following forms currently return compilation errors. In the
inner-predicate examples, `m` is an outer `Note` and `t` is an inner `Tag`.

| Unsupported form | Example |
| --- | --- |
| Non-equality correlation | Inner `WHERE t.note_id < m.id` |
| Correlation through an arithmetic expression | Inner `WHERE t.note_id = m.id + 1` |
| Correlation through a string predicate | Inner `WHERE t.text STARTS WITH m.text` |
| Correlated predicates combined with `OR` | Inner `WHERE t.note_id = m.id OR t.id = 13` |
| An outer `OR` containing a subquery | Outer `WHERE m.id = 0 OR EXISTS { MATCH (t:Tag) WHERE t.note_id = m.id }` |
| An additional predicate combined with a subquery after `WITH` | `WITH m.id AS mid WHERE mid > 0 AND EXISTS { MATCH (t:Tag) WHERE t.note_id = mid }` |

The last two forms require a boolean subquery result (called a MARK join in
the planner), which is not currently supported for execution. The restrictions
also apply to the corresponding `NOT EXISTS` forms. A direct `MATCH ... WHERE`
filter can combine an outer property predicate with `EXISTS` using `AND`, but
the same combination immediately after `WITH` is not yet supported.

These are query-support limitations in both embedded and service modes;
changing the client or mode does not enable the unsupported forms.

