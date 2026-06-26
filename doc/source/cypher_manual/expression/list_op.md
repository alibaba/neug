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
