# Arithmetic Operator

NeuG supports common arithmetic operations including addition (+), subtraction (-), multiplication (*), division (/), and modulo (%).

## Result Type

The data types that can participate in arithmetic operations are: INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE. The result type of arithmetic operations is determined by the operand types, following these rules:
- If the operands have different bit sizes (e.g. 32-bit vs 64-bit), the result uses the larger bit size
- If the operands have the same bit size but different signs, the result promotes to the next larger signed type if available (e.g. `INT32` + `UINT32` -> `INT64`), otherwise to the unsigned type of the same size (e.g. `INT64` + `UINT64` -> `UINT64`)
- Floating-point types take precedence over integer types, with DOUBLE being the highest precision floating-point type

The following table details the result types for different operand combinations:

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT32    | INT32    | INT32  |
| INT32    | UINT32   | INT64  |
| INT32    | INT64    | INT64  |
| INT32    | UINT64   | UINT64 |
| INT32    | FLOAT    | FLOAT  |
| INT32    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT32   | INT32    | INT64  |
| UINT32   | UINT32   | UINT32 |
| UINT32   | INT64    | INT64  |
| UINT32   | UINT64   | UINT64 |
| UINT32   | FLOAT    | FLOAT  |
| UINT32   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT64    | INT32    | INT64  |
| INT64    | UINT32   | INT64  |
| INT64    | INT64    | INT64  |
| INT64    | UINT64   | UINT64 |
| INT64    | FLOAT    | FLOAT  |
| INT64    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT64   | INT32    | UINT64 |
| UINT64   | UINT32   | UINT64 |
| UINT64   | INT64    | UINT64 |
| UINT64   | UINT64   | UINT64 |
| UINT64   | FLOAT    | FLOAT  |
| UINT64   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| FLOAT    | INT32    | FLOAT  |
| FLOAT    | UINT32   | FLOAT  |
| FLOAT    | INT64    | FLOAT  |
| FLOAT    | UINT64   | FLOAT  |
| FLOAT    | FLOAT    | FLOAT  |
| FLOAT    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| DOUBLE   | INT32    | DOUBLE |
| DOUBLE   | UINT32   | DOUBLE |
| DOUBLE   | INT64    | DOUBLE |
| DOUBLE   | UINT64   | DOUBLE |
| DOUBLE   | FLOAT    | DOUBLE |
| DOUBLE   | DOUBLE   | DOUBLE |

## Error Handling

In addition to result types, arithmetic operations may encounter overflow, underflow, or divide-by-zero errors depending on the operand values. For these errors, NeuG provides different handling based on data type and deployment mode:

1. **Floating-point types**: No special handling is performed; the system relies on standard [IEEE 754]() behavior returning Infinity, -Infinity, or NaN values.

2. **Integer types**: Behavior differs between debug and release modes:
   - **Debug mode**: Lower performance requirements allow for input validation during execution with explicit exception throwing
   - **Release mode**: High performance requirements mean overflow/underflow returns undefined values, except for divide-by-zero which may cause explicit exception throwing


The following table details the error types that each operator may encounter:

| Operator | Overflow | Underflow | DivideByZero | Example |
|----------|----------|-----------|--------------|---------|
| +        | YES      | YES       | N/A           | Return CAST(2147483647, 'int32') + CAST(1, 'int32') |
| -        | YES      | YES       | N/A           | Return CAST(-2147483648, 'int32') - CAST(1, 'int32') |
| *        | YES      | YES       | N/A           | Return CAST(2147483647, 'int32') * CAST(2, 'int32') |
| /        | NO       | NO        | YES          | Return 5 / 0 |
| %        | NO       | NO        | YES          | Return 5 % 0 |

## Date Arithmetic

In addition to numeric types, arithmetic operations can also be performed on datetime and interval types. NeuG supports datetime arithmetic operations that allow you to add or subtract intervals from dates and timestamps, as well as calculate differences between temporal values.

### Supported Operations

The following table details the supported date arithmetic operations:

| Operation | Description | Example | Result |
|-----------|-------------|---------|--------|
| DATE + INTERVAL | Add interval to date | `DATE('2011-02-15') + INTERVAL('5 DAYS')` | `DATE('2011-02-20')` |
| DATE - INTERVAL | Subtract interval from date | `DATE('2011-02-15') - INTERVAL('5 DAYS')` | `DATE('2011-02-10')` |
| TIMESTAMP + INTERVAL | Add interval to timestamp | `TIMESTAMP('2011-10-21 14:25:13') + INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-22 20:25:33')` |
| TIMESTAMP - INTERVAL | Subtract interval from timestamp | `TIMESTAMP('2011-10-21 14:25:13') - INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-20 08:24:53')` |
| INTERVAL + INTERVAL | Add two intervals | `INTERVAL('5 DAYS') + INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('6 DAYS 6 HOURS 20 SECONDS')` |
| INTERVAL - INTERVAL | Subtract intervals | `INTERVAL('5 DAYS') - INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('3 DAYS 17 HOURS 39 MINUTES 40 SECONDS')` |
| TIMESTAMP - TIMESTAMP | Calculate time difference | `TIMESTAMP('2011-10-21 14:25:13') - TIMESTAMP('1989-10-21 14:25:13')` | `INTERVAL('8035 DAYS')` |
