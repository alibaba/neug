# Limit Clause

`LIMIT` controls the maximum number of rows returned by a query. It can be
used alone or together with `ORDER BY` to express a Top-K query. `LIMIT`
accepts a non-negative integer literal, a constant integer expression, or a
dynamic parameter.

`SKIP` removes rows from the beginning of the result. `SKIP` and `LIMIT` can
be combined to select the half-open row range `[skip, skip + limit)`. Because
row order is not guaranteed unless `ORDER BY` is present, use `ORDER BY` when
the selected rows must be deterministic.

The value of both `SKIP` and `LIMIT` must be between `0` and `4294967295`
(`UINT32_MAX`), whether it is supplied as a literal, a constant expression, or
a dynamic parameter. Negative, fractional, `NULL`, Boolean, string, and larger
integer values are rejected. The execution range's exclusive upper bound is
also capped at `UINT32_MAX`; if `skip + limit` exceeds it, the upper bound is
saturated at `UINT32_MAX`.

## Limit with Integer Value

```cypher
MATCH (a:Person)
RETURN a.age
LIMIT 2;
```
Since there is no ordering of the output, the result may be any two results.

Output:

```text
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

## Limit with Integer Expression

```cypher
MATCH (a:Person)
RETURN a.age
LIMIT 1+1;
```

Output:

```text
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

## Limit with Dynamic Parameter

Use a dynamic parameter when an application needs to reuse the same statement
with different limits:

```cypher
MATCH (a:Person)
RETURN a.age
LIMIT $row_limit;
```

For example, execute the statement with `row_limit = 2` in Python:

```python
result = connection.execute(
    "MATCH (a:Person) RETURN a.age LIMIT $row_limit",
    parameters={"row_limit": 2},
)
```

Output:

```text
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

# Skip Clause

`SKIP` selects the row range `[skip, +∞)`, assuming row positions start at
zero. Without `ORDER BY`, the rows that are skipped are not deterministic.

## Skip with Integer Value

```cypher
MATCH (a:Person)
RETURN a.age
SKIP 2;
```
The query is used to skip the first two rows of results.

Output:

```text
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Skip with Integer Expression

```cypher
MATCH (a:Person)
RETURN a.age
SKIP 1+1;
```

Output:

```text
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Skip with Dynamic Parameter

```cypher
MATCH (a:Person)
RETURN a.age
SKIP $row_offset;
```

For `row_offset = 2`, the query skips the first two rows:

```python
result = connection.execute(
    "MATCH (a:Person) RETURN a.age SKIP $row_offset",
    parameters={"row_offset": 2},
)
```

Output:

```text
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Use Limit and Skip Together

Place `SKIP` before `LIMIT` to select a bounded range. The following query
skips the first row and returns the next two rows:

```cypher
MATCH (a:Person)
RETURN a.age
SKIP 1
LIMIT 2;
```

Output:

```text
+------------+
|   _0_a.age |
+============+
|         27 |
+------------+
|         32 |
+------------+
```

Both bounds can be dynamic parameters:

```cypher
MATCH (a:Person)
RETURN a.age
ORDER BY a.age ASC
SKIP $row_offset
LIMIT $row_limit;
```

```python
result = connection.execute(
    """
    MATCH (a:Person)
    RETURN a.age
    ORDER BY a.age ASC
    SKIP $row_offset
    LIMIT $row_limit
    """,
    parameters={"row_offset": 1, "row_limit": 2},
)
```
