# List and Array Operators

NeuG supports the list-like operations shown in the table below. These operations work with `LIST` values and, where noted, fixed-size `ARRAY` values.

Operator | Description | Example
---------|------------ | --------
`IN` | return true if an element is contained in the given list | `1 IN [1, 2, 3] `
`[]` | extract an element from a list or fixed-size array by zero-based index | `[10, 20, 30][0]`
`UNWIND` | expand a list or fixed-size array into one row per element | `MATCH (s:Sensor) UNWIND s.readings AS x RETURN x`

## Array Values

`ARRAY` is a fixed-size list-like type declared with `T[N]`. It can be used with `UNWIND`:

```cypher
CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));
CREATE (s:Sensor {id: 1, readings: [3, 1, 2]});

MATCH (s:Sensor)
UNWIND s.readings AS reading
RETURN reading
ORDER BY reading;
```

The result is one row per array element: `1`, `2`, `3`. Fixed-size `ARRAY`
properties also support direct zero-based indexing, for example
`s.readings[2]`.

## Null Value Behavior

### `IN`

`IN` uses three-valued logic when either operand or an element of the list is
`NULL`:

- A `NULL` list produces `NULL`.
- An empty list produces `FALSE`, including when the searched value is `NULL`.
- A definite match produces `TRUE`, even if another list element is `NULL`.
- If there is no match but the list contains `NULL`, the result is `NULL`.
- If there is no match and the list contains no `NULL`, the result is `FALSE`.

| Expression | Result |
|------------|--------|
| `1 IN NULL` | `NULL` |
| `NULL IN NULL` | `NULL` |
| `1 IN []` | `FALSE` |
| `NULL IN []` | `FALSE` |
| `1 IN [1, 2]` | `TRUE` |
| `1 IN [NULL, 1, 2]` | `TRUE` |
| `2 IN [1, NULL, 3]` | `NULL` |
| `2 IN [1, 3]` | `FALSE` |

### `UNWIND`

`UNWIND` preserves `NULL` elements that are already present in a non-NULL list
or fixed-size array. For example:

```cypher
UNWIND [1, NULL, 3] AS value
RETURN value;
// 1, NULL, 3
```

If the expanded values are passed to `collect()`, the aggregation filters out
the `NULL` elements:

(For details about how aggregate functions handle `NULL` values, see
[NULL Value Handling](./agg_func.md#null-value-handling).)

```cypher
UNWIND [1, NULL, 3] AS value
RETURN collect(value);
// [1, 3]
```

`UNWIND` requires a non-NULL list or array value as its input. Attempting to
expand a `NULL` list directly raises an error:

```cypher
UNWIND NULL AS value
RETURN value;
// Error: UNWIND cannot expand a NULL value
```
