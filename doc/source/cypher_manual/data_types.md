# Data Types

This document provides a comprehensive overview of all data types supported by NeuG.

## Data Types Summary Table

The following table showcases all data types supported by NeuG and their differences with Neo4j. The `System Default Value` column indicates the value automatically assigned by the system during data import when the user does not explicitly define a default value in the schema and the corresponding data field is not given (or given as `Null` value) in the raw data. This mechanism prevents `Null` values, ensures data consistency, and provides stable defaults for subsequent queries and computations.

| Category | Type | System Default value | NeuG Example | Neo4j Example |
|----------|------|---------------------|--------------|---------------|
| Primitive | INT32 | `0` | `RETURN CAST(42, 'INT32')` | `RETURN 42` |
| Primitive | UINT32 | `0` | `RETURN CAST(42, 'UINT32')` | unsupported |
| Primitive | INT64 | `0` | `RETURN 9223372036854775807` | `RETURN 9223372036854775807` |
| Primitive | UINT64 | `0` | `RETURN CAST(9223372036854775807, 'UINT64')` | unsupported |
| Primitive | FLOAT | `0.0` | `RETURN CAST(3.14, 'FLOAT')` | `RETURN 3.14f` |
| Primitive | DOUBLE | `0.0` | `RETURN 3.14159265359` | `RETURN 3.14159265359d` |
| Primitive | BOOL | `false` | `RETURN true` | `RETURN true` |
| Primitive | NULL | `null` | `RETURN null` | `RETURN null` |
| String | VARCHAR | `''` (empty string) | `RETURN 'Hello World'` | `RETURN 'Hello World'` |
| Temporal | DATE | `1970-01-01` | `RETURN date('2022-06-06')` | `RETURN date('2022-06-06')` |
| Temporal | DATETIME | `1970-01-01 00:00:00` | `RETURN timestamp('2022-06-06 12:00:00')` | `RETURN datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `0 year 0 month 0 day` (zero interval) | `RETURN interval('1 year 2 month 3 day')` | `RETURN duration('P1Y2M3D')` |
| Composite | LIST | `[]` (empty list) | `RETURN [1, 2, 3]` | `RETURN [1, 2, 3]` |
| Composite | ARRAY | fixed-size child defaults, for example `[0, 0, 0]` for `INT32[3]` | `readings INT32[3]` in a schema | unsupported as a separate fixed-size type |
| Pattern | NODE | `{}` (empty node) | `{_ID: 0, _LABEL: Person, id: 1, name: marko, age: 29}` | `(:Person {name: 'Alice', age: 30})` |
| Pattern | REL | `{}` (empty edge) | `{_ID: 2, _LABEL: KNOWS, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:KNOWS {weight: 1.0}]` |
| Pattern | REPEATED PATH | `[]` (empty path) | `{_ID: 0, _LABEL: Person}, {_ID: 4294967298, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: Person}, {_ID: 4297064449, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: Software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## Detailed Introduction

### Primitive Types

#### INT32
- **Description**: 32-bit signed integer type
- **Range**: [2,147,483,648, 2,147,483,647]
- **Query Example**: `RETURN CAST(42, 'INT32') AS int32_value;`

#### UINT32
- **Description**: 32-bit unsigned integer type
- **Range**: [0, 4,294,967,295]
- **Query Example**: `RETURN CAST(42, 'UINT32') AS uint32_value;`

#### INT64
- **Description**: 64-bit signed integer type, default type of integer values
- **Range**: [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
- **Query Example**: `RETURN 9223372036854775807 AS int64_value;`

#### UINT64
- **Description**: 64-bit unsigned integer type
- **Range**: [0, 18,446,744,073,709,551,615]
- **Query Example**: `RETURN CAST(18446744073709551615, 'UINT64') AS uint64_value;`

#### FLOAT
- **Description**: Single-precision floating-point number
- **Precision**: ~7 decimal digits
- **Query Example**: `RETURN CAST(3.14, 'FLOAT') AS float_value;`

#### DOUBLE
- **Description**: Double-precision floating-point number, default type of float values
- **Precision**: ~15-17 decimal digits
- **Query Example**: `RETURN 3.14159265359 AS double_value;`

#### BOOL
- **Description**: Boolean type representing true or false values
- **Values**: `true`, `false`
- **Query Example**: `RETURN true AS bool_value;`

#### NULL
- **Description**: Represents missing or undefined values
- **Query Example**: `RETURN null AS null_value;`

### String Types

We currently support only the VARCHAR type for strings. You can specify a maximum character length using the `VARCHAR(max_length)` syntax. The default value of `max_length` is 256, and the maximum limit is 65536.
Alternatively, you can use STRING to specify the character type directly; STRING is equivalent to VARCHAR(256), i.e., a varchar type with a default maximum length of 256 characters.

#### VARCHAR
- **Description**: Variable-length character string with UTF-8 encoding
- **Query Example**: `RETURN 'Hello World' AS string_value;`
- **Length**: Variable, limited by system constraints, default is `256`

### Temporal Types

#### DATE
- **Description**: Date type for storing calendar dates
- **Format**: YYYY-MM-DD
- **Query Example**: `RETURN date('2022-06-06') AS date_value;`

#### DATETIME
- **Description**: Combination of date and time type
- **Format**: YYYY-MM-DD HH:MM:SS
- **Query Example**: `RETURN timestamp('2022-06-06 12:00:00') AS datetime_value;`

#### INTERVAL
- **Description**: The INTERVAL type represents a duration or time interval and consists of the following fields: `year`, `month`, `day`, `hour`, `minute`, `second`, `millisecond`, and `microsecond` . The INTERVAL type supports two primary formats for specifying values: 
    - Date-based components (year, month, day): Specified using a natural language format. Example: `1 year 2 month 3 day`.
    - Time-based components (hour, minute, second, millisecond, microsecond): Specified using a natural language format. Example: `12 hour 12 minute 2 second` - represents 12 hours, 12 minutes, and 2 seconds.
- **Query Example**: `RETURN interval('1 year 2 month 3 day 12 hour 12 minute 2 second') AS interval_value;`

### Composite Types

#### LIST
- **Description**: Ordered collection of values with heterogeneous types
- **Query Example**: `RETURN [1, 2, 3] AS list_value;`

The following table shows all Component Types that LIST can support:

| Category | Type | Example |
|----------|------|---------|
| Numeric | INT32, INT64, UINT32, UINT64, DOUBLE, FLOAT | `RETURN [1, 2, 3.0];` |
| String | VARCHAR | `RETURN ['marko', 'josh'];` |
| Date | DATE, DATETIME | `RETURN [date('2011-01-25'), timestamp('2011-01-25 11:20:33')];` |
| BOOL | BOOL | `RETURN [true, false];` |
| Composite | LIST | `RETURN [[1, 2], [4, 5]];` |

**Important Note on LIST Component Types**: 

NeuG supports lists through tuple data types, meaning composite types can be heterogeneous. Here are some examples:

Mixing different primitive types in a single list:
```cypher
RETURN ['marko', 2];
```

Combining different property types from nodes in a list:
```cypher
MATCH (n:Person) RETURN [n.name, n.age];
```

Supporting nested list structures:
```cypher
MATCH (n:Person) RETURN [["name", n.name], ["age", n.age]];
```

**Key Technical Details:**
- Lists in NeuG can contain elements of different data types (heterogeneous lists)
- This is achieved through internal tuple data type support
- Type conversion is handled automatically when possible
- Nested lists are fully supported for complex data structures
- The system maintains type safety while allowing flexibility in list composition

#### ARRAY
- **Description**: Fixed-size ordered collection whose elements share the declared child type
- **Syntax**: Use `T[N]`, where `T` is the child type and `N` is a positive fixed length
- **Query Example**: `CREATE NODE TABLE Sensor(id INT64, readings INT64[3], PRIMARY KEY(id));`

`ARRAY` is NeuG's fixed-size counterpart to `LIST`. `T[]` declares a variable-length list, while `T[N]` declares an array with exactly `N` elements. Array literals use the same bracket syntax as lists; the declared schema or another typed context determines whether the value is stored as a `LIST` or an `ARRAY`. `CAST` is not a general `LIST`/`ARRAY` compatibility mechanism.

```cypher
CREATE NODE TABLE Sensor(
    id INT64,
    readings INT32[3],
    PRIMARY KEY(id)
);

CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});
MATCH (s:Sensor) RETURN s.readings;
```

Arrays can also be used on relationship properties:

```cypher
CREATE REL TABLE Knows(
    FROM Person TO Person,
    weights DOUBLE[2]
);
```

Multi-dimensional arrays are written by chaining fixed lengths. `INT32[2][3]` means an outer array with 3 elements, where each element is an `INT32[2]` array:

```cypher
CREATE NODE TABLE Matrix(
    id INT64,
    grid INT32[2][3],
    PRIMARY KEY(id)
);

CREATE (m:Matrix {id: 1, grid: [[1, 2], [3, 4], [5, 6]]});
```

**Key Technical Details:**
- Array values must match the declared length at every fixed-size dimension
- Missing or `NULL` array properties during `CREATE` use the child type's default value for each element
- Explicit array default literals in DDL, such as `prop INT32[3] DEFAULT [1, 2, 3]`, are supported
- `RETURN`, equality filters, zero-based indexing, `SET`, `MERGE`, `collect()`, and `UNWIND` support array-valued properties
- `CAST` does not convert between `LIST` and `ARRAY`; array property values are typed by schema-aware compiler contexts
- Setting an existing array property to `NULL` is not supported yet

### Graph Types

#### NODE
- **Description**: Represents a node in the graph
- **Internal Structure** (order is insignificant): `_ID` (internal identifier), `_LABEL` (indication of node type) and property fields
- **Query Example**: `MATCH (n:Person) RETURN n AS node_value;`
- **NeuG Format**: `{_ID: 0, _LABEL: Person, id: 1, name: marko, age: 29}`

#### REL (Edge)
- **Description**: Represents an edge in the graph
- **Internal Structure** (order is insignificant): `_ID` (edge internal identifier),  `_LABEL` (indication of edge type), `_SRC_ID` (internal identifier of source node), `_SRC_LABEL` (label of source node), `_DST_ID` (internal identifier of destination node), `_DST_LABEL` (label of destination node), and property fields
- **Query Example**: `MATCH ()-[r:KNOWS]->() RETURN r AS rel_value;`
- **NeuG Format**: `{_ID: 2, _LABEL: KNOWS, _SRC_ID: 0, _SRC_LABEL: Person, _DST_ID: 2, _DST_LABEL: Person, weight: 1.0}`

#### PATH
- **Description**:  Represents a graph path formed by alternating nodes and edges.
- **Internal Structure**: An **ordered sequence** of nodes and edges along the path, including the starting and ending nodes.
- **Query Example**: `MATCH (a:Person)-[p*1..2]->(c) RETURN p AS path_value;`
- **NeuG Format**: `{_ID: 0, _LABEL: Person}, {_ID: 4294967298, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: Person}, {_ID: 4297064449, _LABEL: CREATED, _SRC_LABEL: Person, _DST_LABEL: Software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: Software}`
