# Data Model

## Data Types


`Neug` provides robust support for a wide range of data types utilized in both vertex and edge properties. These data types are organized into five distinct categories:

- **Primitive Types:** These include fundamental data types such as integers, floats, and booleans, which are essential for representing basic numerical and logical values.

- **String Types:** This category comprises types that manage textual data, allowing for efficient storage and manipulation of strings within vertices and edges.

- **Temporal Types:** These types are designed to handle date and time information, accommodating various time representations to enable temporal analysis and queries.

- **Array Types:** Array types allow for the storage of collections of elements, enabling the representation of lists or sets of values associated with a graph element.

- **Complex Types:** (Currently not supported) These may include user-defined objects or compound structures that combine multiple primitive types, enabling more sophisticated data modeling and relationships within the graph.


### Primitive Types

Certainly! Here's a Markdown table with the specified header and first line entry:

| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| BOOL   | 1 bit   | One-bit boolean                |
| INT8   | 1 byte  | One-byte integer               |
| UINT8  | 1 byte  | Unsigned one-byte integer      |
| INT16  | 2 bytes | Two-byte integer               |
| UINT16 | 2 bytes | Unsigned two-byte integer      |
| INT32  | 4 bytes | Four-byte integer              |
| UINT32 | 4 bytes | Unsigned four-byte integer     |
| INT64  | 8 bytes | Eight-byte integer             |
| UINT64 | 8 bytes | Unsigned eight-byte integer    |
| FLOAT  | 4 bytes | Single-precision float         |
| DOUBLE | 8 bytes | Double-precision float         |



### String Types


| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| STRING | -       | Variable length string         |

### Temporal types


#### DATE

`DATE` denotes a calendar day characterized by the components of year, month, and day, formatted to comply with the ISO-8601 standard (YYYY-MM-DD). It takes 4 bytes in storage.

```cypher
RETURN date('2022-06-06') as x;
```

#### DATE32

`DATE32` represents a calendar day by counting the number of days since January 1, 1970. Days preceding this date are indicated by negative values. It takes 4 bytes in storage.(TODO: (xiaoli): make sure it works)

```cypher
RETURN date32('2022-06-06') as x;
```

#### TIMESTAMP

`TIMESTAMP` integrates both the date and time components—hour, minute, second, and millisecond—formatted in the ISO-8601 standard (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]]). This format specifies the date (YYYY-MM-DD), time (hh:mm:ss[.zzzzzz]), and optional time offset [+-TT[:tt]]. The date portion is mandatory, while the time component is optional, allowing inclusion of milliseconds [.zzzzzz] and a time offset if desired. It takes 8 bytes in storage. For example: "1970-01-01 00:00:00.004666-10"

```cypher
RETURN timestamp("1970-01-01 00:00:00.004666-10") as x;
```

#### INTERVAL

`INTERVAL` represents a period of time, could be characterized by the components of year, month, and day, hour, minutes, microseconds. We follow the definition for Interval in [kuzu](https://docs.kuzudb.com/cypher/data-types/#interval).

```cypher
RETURN interval("1 year 2 days") as x;
```

### Array Types

Array types are currently not supported, but are planned to be supported in the near future.
Once supported, albeit requiring that every element within the array adheres to one of the previously mentioned primitive types. 


### Complex Types

Complex types are currently not supported, including `STRUCT`, `MAP`, `UNION`, `BLOB`.


## Type conversion

We support for type [casting between numeric types](https://github.com/GraphScope/neug/issues/416).

- INT32
- UINT32
- INT64
- UINT64
- FLOAT
- DOUBLE

**All pairs of these types are allowed to convert between each other**. During casting, the compiler will perform overflow checks to ensure conversions are safe. The table below summarizes potential overflow scenarios handled during these conversions:


| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX)         | ⚠️ May Overflow (if value > UINT32_MAX)       | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |