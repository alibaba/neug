# Order Clause

Order is used to sort the current results based on properties to ensure deterministic output. We currently support two sorting options: `ASC` for ascending order and `DESC` for descending order. If not specifically specified, the default is ascending order. Notably, Order can be used in combination with Limit, which is equivalent to a TopK operation. We will introduce the following common usage patterns.

## Order by Single Property

```
Match (a)
Return a.name
ORDER BY a.name ASC;
```

output:
```
+-------------+
| _0_a.name   |
+=============+
| josh        |
+-------------+
| lop         |
+-------------+
| marko       |
+-------------+
| peter       |
+-------------+
| ripple      |
+-------------+
| vadas       |
+-------------+
```

## Order by Multiple Properties

```
Match (a)-[b]->(c)
Return a.name, b.weight
ORDER BY a.name ASC, b.weight ASC;
```

output:
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| josh        |           0.4 |
+-------------+---------------+
| josh        |           1   |
+-------------+---------------+
| marko       |           0.4 |
+-------------+---------------+
| marko       |           0.5 |
+-------------+---------------+
| marko       |           1   |
+-------------+---------------+
| peter       |           0.2 |
+-------------+---------------+
```

## Order by Expressions

In addition to sorting by properties directly, the Order BY key can also be more complex expressions, such as arithmetic operation results, scalar function returns, etc.

### Order by Pre-computed Expression

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name;
```

output:
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
|         29 | vadas       |
+------------+-------------+
|         32 | lop         |
+------------+-------------+
|         32 | ripple      |
+------------+-------------+
|         35 | lop         |
+------------+-------------+
```

### Order by Scalar Function Results

```
Match (a)-[b]->(c)
Return a.name, c.name
Order BY label(a);
```

<!-- todo: label function is not included in current pip package -->

For more Scalar Function operations, see the [Function Section](../../expression).

## Order with Limit

Additionally, in BI (Business Intelligence) query scenarios, TopK is one of the most common operations, truncating and outputting only the most significant results. NeuG also supports such queries.

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name ASC
Limit 2;
``` 

output:
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
```