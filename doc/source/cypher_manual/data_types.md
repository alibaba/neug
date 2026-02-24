# Data Types

This document provides a comprehensive overview of all data types supported by NeuG.

## Data Types Summary Table

The following table showcases all data types supported by NeuG and their differences with Neo4j. The `System Default Value` column indicates the value automatically assigned by the system during data import when the user does not explicitly define a default value in the schema and the corresponding data field is not given (or given as `Null` value) in the raw data. This mechanism prevents `Null` values, ensures data consistency, and provides stable defaults for subsequent queries and computations.

| Category | Type | System Default value | NeuG Example | Neo4j Example |
|----------|------|---------------------|--------------|---------------|
| Primitive | INT32 | `0` | `Return CAST(42, 'INT32')` | `Return 42` |
| Primitive | UINT32 | `0` | `Return CAST(42, 'UINT32')` | unsupported |
| Primitive | INT64 | `0` | `Return 9223372036854775807` | `Return 9223372036854775807` |
| Primitive | UINT64 | `0` | `Return CAST(9223372036854775807, 'UINT64')` | unsupported |
| Primitive | FLOAT | `0.0` | `Return CAST(3.14, 'FLOAT')` | `Return 3.14f` |
| Primitive | DOUBLE | `0.0` | `Return 3.14159265359` | `Return 3.14159265359d` |
| Primitive | BOOL | `false` | `Return true` | `Return true` |
| Primitive | NULL | `null` | `Return null` | `Return null` |
| String | VARCHAR | `''` (empty string) | `Return 'Hello World'` | `Return 'Hello World'` |
| Temporal | DATE | `1970-01-01` | `Return date('2022-06-06')` | `Return date('2022-06-06')` |
| Temporal | DATETIME | `1970-01-01 00:00:00` | `Return timestamp('2022-06-06 12:00:00')` | `Return datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `0 year 0 month 0 day` (zero interval) | `RETURN interval('1 year 2 month 3 day')` | `Return duration('P1Y2M3D')` |
| Composite | LIST | `[]` (empty list) | `Return [1, 2, 3]` | `Return [1, 2, 3]` |
| Pattern | NODE | `{}` (empty node) | `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}` | `(:person {name: 'Alice', age: 30})` |
| Pattern | REL | `{}` (empty edge) | `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:knows {weight: 1.0}]` |
| Pattern | REPEATED PATH | `[]` (empty path) | `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## Detailed Introduction

### Primitive Types

#### INT32
- **Description**: 32-bit signed integer type
- **Range**: [2,147,483,648, 2,147,483,647]
- **Query Example**: `Return CAST(42, 'INT32') as int32_value;`

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

#### VARCHAR
- **Description**: Variable-length character string with UTF-8 encoding
- **Query Example**: `RETURN 'Hello World' AS string_value;`
- **Length**: Variable, limited by system constraints, default is `65536`

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
MATCH (n:person) RETURN [n.name, n.age];
```

Supporting nested list structures:
```cypher
MATCH (n:person) RETURN [["name", n.name], ["age", n.age]];
```

**Key Technical Details:**
- Lists in NeuG can contain elements of different data types (heterogeneous lists)
- This is achieved through internal tuple data type support
- Type conversion is handled automatically when possible
- Nested lists are fully supported for complex data structures
- The system maintains type safety while allowing flexibility in list composition

### Graph Types

#### NODE
- **Description**: Represents a node in the graph
- **Internal Structure** (order is insignificant): `_ID` (internal identifier), `_LABEL` (indication of node type) and property fields
- **Query Example**: `MATCH (n:person) RETURN n AS node_value;`
- **NeuG Format**: `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}`

#### REL (Edge)
- **Description**: Represents an edge in the graph
- **Internal Structure** (order is insignificant): `_ID` (edge internal identifier),  `_LABEL` (indication of edge type), `_SRC_ID` (internal identifier of source node), `_SRC_LABEL` (label of source node), `_DST_ID` (internal identifier of destination node), `_DST_LABEL` (label of destination node), and property fields
- **Query Example**: `MATCH ()-[r:knows]->() RETURN r AS rel_value;`
- **NeuG Format**: `{_ID: 2, _LABEL: knows, _SRC_ID: 0, _SRC_LABEL: person, _DST_ID: 2, _DST_LABEL: person, weight: 1.0}`

#### PATH
- **Description**:  Represents a graph path formed by alternating nodes and edges.
- **Internal Structure**: An **ordered sequence** of nodes and edges along the path, including the starting and ending nodes.
- **Query Example**: `MATCH (a:person)-[p*1..2]->(c) RETURN p AS path_value;`
- **NeuG Format**: `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}`
