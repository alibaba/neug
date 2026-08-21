# List Functions

Since v0.2.0, NeuG provides built-in functions for working with list-like values. These functions provide common operations for constructing, combining, and manipulating collection values in Cypher queries.

The currently supported list functions are summarized below.

| Function                          | Description                            | Example                              |
| --------------------------------- | -------------------------------------- | ------------------------------------ |
| `list_append(list_like, element)` | Appends one element to a list or array | `RETURN list_append([1, 2], 3)`      |
| `list_concat(left, right)`        | Concatenates two lists or arrays       | `RETURN list_concat([1, 2], [3, 4])` |

The accepted argument types, return types, type inference rules, and behavior of each function are described in the corresponding sections below.

## `list_append`

`list_append` appends a single element to the end of a `LIST` or `ARRAY`.
The input and appended element can also be nested `LIST` or `ARRAY` values,
provided their nested element types are compatible.

### Syntax

```cypher
list_append(list_like, element)
```

`list_like` must be a `LIST<T>` or an `ARRAY<T, N>`.

`element` can be a constant, dynamic parameter, property, or another Cypher expression.

NeuG determines a common type between the existing element type and the appended value. When necessary, both inputs are implicitly converted to that common type before the result is constructed.

The function returns a new `LIST`; the input `LIST` or `ARRAY` is not modified.

### Examples

#### Append a constant

```cypher
RETURN list_append([1, 2], 3);
// [1, 2, 3]
```

#### Append a dynamic parameter

Dynamic parameters can be used as the element to append:

```cypher
WITH $value AS value
RETURN list_append([1, 2], value);
```

For example, when `$value` is `3`, the result is:

```text
[1, 2, 3]
```

The parameter type participates in common-type inference in the same way as a constant. The `WITH` clause binds the dynamic parameter as a query value before it is passed to the list function.

#### Append to a `LIST` property

Given the following schema:

```cypher
CREATE NODE TABLE Item(
    id INT64,
    tags INT64[],
    coordinates INT64[3],
    PRIMARY KEY(id)
);
```

where `tags` is a `LIST<INT64>` property:

```cypher
MATCH (item:Item)
RETURN list_append(item.tags, 10);
```

The returned value contains the original elements of `item.tags`, followed by `10`.

The stored property itself is not modified.

#### Append to an `ARRAY` property

`ARRAY` values are also accepted:

```cypher
MATCH (item:Item)
RETURN list_append(item.coordinates, 10);
```

If `item.coordinates` is:

```text
[1, 2, 3]
```

the result is:

```text
[1, 2, 3, 10]
```

The result is a `LIST<INT64>`, not an `ARRAY<INT64, 4>`. The original fixed-size `ARRAY<INT64, 3>` remains unchanged.

#### Append to an empty list

```cypher
RETURN list_append([], 1);
// [1]
```

When the input is an untyped empty list, the appended value provides the element type of the result.

#### Append `NULL`

When the input already provides a concrete element type, `NULL` can be appended directly:

```cypher
RETURN list_append([1, 2], NULL);
// [1, 2, NULL]
```

In this example, the existing list determines the element type as `INT64`, and the returned list preserves the appended `NULL` value.

#### Append a compatible type

If the existing elements and the appended value have different but compatible types, NeuG determines a common type and converts the values when necessary:

```cypher
RETURN list_append([1, 2], 3.5);
// [1.0, 2.0, 3.5]
```

Here, the integer values are promoted to the common numeric type used by the result.

#### Append to a nested list

Nested lists are supported. The appended element must itself be a compatible
list value:

```cypher
RETURN list_append([[1, 2], [3, 4]], [5, 6]);
// [[1, 2], [3, 4], [5, 6]]
```

### Error Handling

`list_append` reports an error in the following cases:

* the function does not receive exactly two arguments;
* the first argument is not a `LIST` or `ARRAY`;
* the existing element type and the appended value have no compatible common type;
* a required implicit conversion to the inferred common type is not supported.

For example, the following call is invalid because the first argument is a scalar rather than a `LIST` or `ARRAY`:

```cypher
RETURN list_append(1, 2);
```

## `list_concat`

`list_concat` concatenates two `LIST` or `ARRAY` values.
Nested `LIST` and `ARRAY` values are supported when their nested element types
are compatible.

### Syntax

```cypher
list_concat(left_list_like, right_list_like)
```

Both arguments must be `LIST` or `ARRAY` values.

The two inputs:

* do not need to use the same container type;
* do not need to have the same length;
* may have different but compatible element types.

NeuG determines a common element type, converts elements when necessary, and returns a new `LIST` containing all elements from the left-hand input followed by all elements from the right-hand input.

Neither input value is modified.

### Examples

#### Concatenate two lists

```cypher
RETURN list_concat([1, 2], [3, 4]);
// [1, 2, 3, 4]
```

#### Concatenate `LIST` and `ARRAY` properties

Using the `Item` schema defined above:

```cypher
MATCH (item:Item)
RETURN list_concat(item.tags, item.coordinates);
```

`item.tags` is a variable-length `LIST<INT64>`, while `item.coordinates` is a fixed-size `ARRAY<INT64, 3>`. Both are accepted as inputs, and the result is a `LIST<INT64>`.

The arguments can also appear in the opposite order:

```cypher
MATCH (item:Item)
RETURN list_concat(item.coordinates, item.tags);
```

#### Concatenate arrays

Fixed-size arrays can be concatenated directly:

```cypher
MATCH (item:Item)
RETURN list_concat(item.coordinates, item.coordinates);
```

Even when both inputs are `ARRAY` values, the result is a `LIST`.

#### Concatenate values with different lengths

The two inputs do not need to contain the same number of elements:

```cypher
RETURN list_concat([1], [2, 3, 4]);
// [1, 2, 3, 4]
```

#### Concatenate empty lists

Empty lists are supported on either side:

```cypher
RETURN list_concat([], [1, 2]);
// [1, 2]
```

```cypher
RETURN list_concat([1, 2], []);
// [1, 2]
```

Two empty lists can also be concatenated:

```cypher
RETURN list_concat([], []);
// []
```

When one side has a known element type, that type can be used to infer the type of an untyped empty list.

#### Concatenate compatible element types

If the two inputs have different but compatible element types, NeuG promotes them to a common type:

```cypher
RETURN list_concat([1, 2], [3.5, 4.5]);
// [1.0, 2.0, 3.5, 4.5]
```

#### Concatenate nested lists

Nested lists can be concatenated when their child list types are compatible:

```cypher
RETURN list_concat([[1, 2]], [[3, 4], [5, 6]]);
// [[1, 2], [3, 4], [5, 6]]
```

### Error Handling

`list_concat` reports an error in the following cases:

* the function does not receive exactly two arguments;
* either argument is not a `LIST` or `ARRAY`;
* the element types of the two inputs have no compatible common type;
* a required implicit conversion to the inferred common type is not supported.

For example, the following call is invalid because the second argument is a scalar:

```cypher
RETURN list_concat([1], 2);
```

## Type Inference and Conversion

List functions determine a common element type from their inputs when required by the function.

When all values already have the same element type, no type promotion is required:

```cypher
RETURN list_concat([1, 2], [3, 4]);
// LIST<INT64>
```

When compatible numeric types are mixed, NeuG promotes the values to a common numeric type:

```cypher
RETURN list_append([1, 2], 3.5);
// [1.0, 2.0, 3.5]
```

```cypher
RETURN list_concat([1, 2], [3.5, 4.5]);
// [1.0, 2.0, 3.5, 4.5]
```

Untyped values such as empty list literals and `NULL` may derive their type from the other inputs.

For example:

```cypher
RETURN list_append([], 1);
// [1]
```

Here, `1` determines the result element type.

Similarly:

```cypher
RETURN list_append([1, 2], NULL);
// [1, 2, NULL]
```

Here, `[1, 2]` provides the element type for the result.

A typed top-level `NULL` list input propagates to a `NULL` result:

```cypher
RETURN list_append(CAST(NULL, 'INT64[]'), 3);
// NULL
```

```cypher
RETURN list_concat(CAST(NULL, 'INT64[]'), [1]);
// NULL
```

If NeuG cannot determine a compatible common element type, or if the required implicit conversion is unsupported, the function reports an error.

## NULL Values in List Functions

`list_append` supports appending a `NULL` value when the element type can be determined from the input list:

```cypher
RETURN list_append([1, 2], NULL);
// [1, 2, NULL]
```

The returned `LIST` can preserve the appended `NULL` value. For example, the result above contains three elements, with the last element being `NULL`.

This behavior applies to the value returned by the list function. Stored list properties currently do not support `NULL` as an individual list element. When a list containing a `NULL` element is written to a list property, the `NULL` element is converted to the default value of the property's element type.

For example, if the target property has type `INT64[]`:

```text
[1, 2, NULL]
```

it is stored as:

```text
[1, 2, 0]
```

where `0` is the default value for `INT64`.

Therefore, a `LIST` returned by `list_append` may contain `NULL`, but persisting that value to a list property does not preserve the `NULL` element.
