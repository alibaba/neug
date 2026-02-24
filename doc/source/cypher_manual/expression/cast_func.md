# CAST Function

The `CAST` function is used to convert values between different data types in NeuG. It supports type conversion for literal values, dynamic parameters, variables, and node/relationship properties.

## Syntax

```cypher
CAST(expression, 'TARGET_TYPE')
```

**Parameters:**
- `expression`: The value or expression to be converted. Can be a literal, variable, property, or any valid Cypher expression.
- `'TARGET_TYPE'`: A string literal specifying the target data type (e.g., `'INT32'`, `'DOUBLE'`, `'STRING'`, `'DATE'`).

**Returns:** The value converted to the specified target type.

## Usage Examples

### Basic Type Conversions

The following examples demonstrate basic type conversions using literal values:

```cypher
# Convert integer to double (up-cast)
RETURN CAST(123, 'DOUBLE');

# Convert double to integer (down-cast, truncates decimal part)
RETURN CAST(123.7, 'INT32');

# Convert string to integer (parsing)
RETURN CAST('123', 'INT32');

# Convert integer to string
RETURN CAST(123, 'STRING');

# Convert string to temporal type
RETURN CAST('2012-01-02', 'DATE');
```

### Converting Property Values

The `CAST` function can also be applied to node and relationship properties:

```cypher
# Convert node property from INT32 to DOUBLE
MATCH (n:person)
RETURN CAST(n.age, 'DOUBLE');

# Convert relationship property from DOUBLE to INT32
MATCH (n:person)-[k:knows]->(m:person)
RETURN CAST(k.weight, 'INT32');
```

### Converting Variables and Expressions

You can cast any valid expression, including variables and computed values:

```cypher
# Cast a variable
MATCH (n:person)
WITH n.age AS age
RETURN CAST(age, 'DOUBLE') AS age_double;

# Cast computed expressions
MATCH (n:person)
RETURN CAST(n.age * 2.5, 'INT32') AS rounded_age;
```

## Supported Type Categories

NeuG's type system includes four major categories:

| Category     | Description                     | CAST Support |
|--------------|----------------------------------|--------------|
| **Numeric**  | `INT32`, `INT64`, `UINT32`, `UINT64`, `FLOAT`, `DOUBLE` | ✅ (with overflow rules) |
| **String**   | `STRING`                         | ✅ |
| **Temporal** | `DATE`, `DATETIME`               | ✅ (limited, see conversion table below) |
| **Composite**| `ARRAY`, `LIST`, `TUPLE`, `MAP`  | ❌ (not supported currently) |

## Type Conversion Rules

### Numeric-to-Numeric Conversions

When converting between numeric types, **precision loss or overflow may occur** (e.g., `INT64` → `INT32`). The conversion behavior depends on the source and target types, as well as the build mode.

#### Conversion Matrix

The following table shows the safety and overflow behavior for numeric type conversions:

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|-----------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0) | ✅ Safe | ⚠️ May Overflow (if value < 0) | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX) | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0) | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX) | ⚠️ May Overflow (if value > UINT32_MAX) | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |

**Legend:**
- ✅ **Safe**: Conversion is always safe and preserves the value
- ⚠️ **May Overflow**: Conversion may fail or produce unexpected results if the value is outside the target type's range

#### Overflow Handling

The behavior when overflow occurs depends on the build mode:

| Mode    | Behavior |
|---------|----------|
| **Release** | Silently returns an undefined/overflowed value (e.g., wrap-around or maximum representable value). **No exception is thrown** for performance reasons. |
| **Debug**   | **Throws a clear `OverflowError`** with a message indicating the source type, target type, and the value that caused the overflow. |

**Example:**
```cypher
# In Release mode, this may silently wrap or return max value
# In Debug mode, this throws OverflowError
RETURN CAST(999999999999, 'INT32');
```

### Cross-Category Conversions

The following table summarizes allowed conversions between different type categories:

| From \ To     | Numeric | String | Temporal |
|---------------|---------|--------|----------|
| **Numeric**   | ✅ (see numeric conversion table above) | ✅ (e.g., `123` → `"123"`) | ❌ (not supported currently) |
| **String**    | ✅ (if parseable, e.g., `"42"` → `42`) | ✅ (identity conversion) | ✅ (if ISO-compliant, e.g., `"2025-01-01"` → `DATE`) |
| **Temporal**  | ❌ (not supported currently) | ✅ (converts to ISO string format) | ✅ (between compatible temporal types, e.g., `DATE` ↔ `DATETIME`) |

#### String to Numeric Conversion

When converting from `STRING` to a numeric type, the string must be parseable as the target numeric type:

```cypher
# Valid conversions
RETURN CAST('42', 'INT32');        # Returns 42
RETURN CAST('3.14', 'DOUBLE');    # Returns 3.14
RETURN CAST('-100', 'INT64');     # Returns -100

# Invalid conversions will result in errors
# RETURN CAST('abc', 'INT32');    # Error: cannot parse 'abc' as INT32
```

#### String to Temporal Conversion

When converting from `STRING` to temporal types, the string must be in ISO-compliant format:

```cypher
# Valid DATE conversion
RETURN CAST('2012-01-02', 'DATE');

# Valid DATETIME conversion
RETURN CAST('2012-01-02 10:30:00', 'TIMESTAMP');
```

#### Temporal to String Conversion

Temporal types are converted to ISO string format:

```cypher
# DATE to STRING
RETURN CAST(DATE('2012-01-02'), 'STRING');  # Returns '2012-01-02'

# DATETIME to STRING
RETURN CAST(DATETIME('2012-01-02 10:30:00'), 'STRING'); # Returns '2012-01-02 10:30:00'
```

## Error Handling

The `CAST` function may fail in the following scenarios:

1. **Invalid type conversion**: Attempting to convert between incompatible types (e.g., `ARRAY` to `INT32`)
2. **Parse errors**: Converting a string that cannot be parsed as the target type (e.g., `CAST('abc', 'INT32')`)
3. **Overflow errors** (Debug mode only): Converting a value that exceeds the target type's range
