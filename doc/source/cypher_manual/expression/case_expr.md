# CASE Expressions

`CASE` evaluates alternatives in order and returns the result of the first
matching alternative. NeuG supports both simple and searched `CASE`
expressions.

## Simple CASE

A simple `CASE` compares one expression with each `WHEN` value:

```cypher
CASE expression
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ELSE default_result
END;
```

For non-`NULL` values, each alternative is evaluated as an equality
comparison. For example:

```cypher
RETURN CASE 2
    WHEN 1 THEN 'one'
    WHEN 2 THEN 'two'
    ELSE 'other'
END;
// "two"
```

When `expression` is `NULL`, a `WHEN NULL` alternative is interpreted as
`expression IS NULL` rather than `expression = NULL`. This provides an explicit
branch for `NULL`. If no `WHEN NULL` alternative is provided, a `NULL`
expression falls through to the `ELSE` branch.

```cypher
RETURN CASE NULL
    WHEN NULL THEN 'null value'
    ELSE 'not matched'
END;
// "null value"
```

## Searched CASE

A searched `CASE` evaluates each `WHEN` condition directly:

```cypher
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END;
```

Only a condition that evaluates to `TRUE` matches. Conditions that evaluate to
`FALSE` or `NULL` do not match.

Use `IS NULL` to test for a `NULL` value explicitly:

```cypher
RETURN CASE
    WHEN expression IS NULL THEN 'null value'
    ELSE 'not matched'
END;
```

Do not use `expression = NULL` since the comparison evaluates to
`NULL` rather than `TRUE`:

```cypher
RETURN CASE
    WHEN expression = NULL THEN 'null value'
    ELSE 'not matched'
END;
// "not matched"
```

## ELSE

The `ELSE` clause is optional. If no `WHEN` alternative matches and `ELSE` is
omitted, the result is `NULL`.
