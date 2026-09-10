# String Functions

String functions transform string values, while string predicates test whether
a string matches a specified pattern.

## String Transformation Functions

| Function | Description |
|----------|-------------|
| `UPPER(value)` | Converts `value` to uppercase |
| `LOWER(value)` | Converts `value` to lowercase |
| `REVERSE(value)` | Reverses the characters in `value` |

`TOUPPER` and `UCASE` are aliases of `UPPER`. `TOLOWER` and `LCASE` are aliases
of `LOWER`.

```cypher
RETURN UPPER('Alice'), LOWER('Alice'), REVERSE('Alice');
// 'ALICE', 'alice', 'ecilA'
```

String transformation functions return `NULL` when the input is `NULL`.

```cypher
RETURN UPPER(NULL), LOWER(NULL), REVERSE(NULL);
// NULL, NULL, NULL
```

## String Predicates

String predicates compare a string with a literal pattern and return a Boolean
value.

| Predicate | Description |
|-----------|-------------|
| `value STARTS WITH pattern` | Tests whether `value` begins with `pattern` |
| `value ENDS WITH pattern` | Tests whether `value` ends with `pattern` |
| `value CONTAINS pattern` | Tests whether `value` contains `pattern` |

```cypher
RETURN 'Alice' STARTS WITH 'Al',
       'Alice' ENDS WITH 'ice',
       'Alice' CONTAINS 'lic';
// TRUE, TRUE, TRUE
```

If either the original string or the substring is `NULL`, the result is
`NULL`.

```cypher
RETURN NULL STARTS WITH 'A',
       'Alice' STARTS WITH NULL,
       NULL ENDS WITH 'e',
       'Alice' ENDS WITH NULL,
       NULL CONTAINS 'lic',
       'Alice' CONTAINS NULL;
// NULL, NULL, NULL, NULL, NULL, NULL
```
