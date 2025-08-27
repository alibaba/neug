# 算术运算符

Neug 支持常见的算术运算，包括加法 (+)、减法 (-)、乘法 (*)、除法 (/) 和取模 (%)。

## 结果类型

可以参与算术运算的数据类型有：INT32、UINT32、INT64、UINT64、FLOAT、DOUBLE。算术运算的结果类型由操作数类型决定，遵循以下规则：
- 如果左右操作数的宽度不同，则结果为宽度较大的类型
- 如果左右操作数宽度相同但符号不同，则结果为下一个更大的有符号类型
- 浮点数的最大宽度类型是 DOUBLE，整数的最大宽度类型是 INT64

下表详细列出了不同操作数组合的结果类型：

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

## 错误处理

除了结果类型之外，算术运算还可能根据操作数的值遇到溢出（overflow）、下溢（underflow）或除零（divide-by-zero）错误。对于这些错误，Neug 根据数据类型和部署模式提供了不同的处理方式：

1. **浮点数类型**：不进行特殊处理；系统依赖标准 [IEEE 754]() 行为，返回 `Infinity`、`-Infinity` 或 `NaN` 值。

2. **整数类型**：行为在 debug 和 release 模式下有所不同：
   - **Debug 模式**：性能要求较低，允许在运行时进行输入验证并显式抛出异常
   - **Release 模式**：高性能要求意味着溢出/下溢将返回未定义值，但除零操作可能导致 core dump

下表详细列出了每个运算符可能遇到的错误类型：

| Operator | Overflow | Underflow | DivideByZero | Example |
|----------|----------|-----------|--------------|---------|
| +        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') + CAST(1, 'int32') |
| -        | YES      | YES       | NO           | Return CAST(-2147483648, 'int32') - CAST(1, 'int32') |
| *        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') * CAST(2, 'int32') |
| /        | NO       | NO        | YES          | Return 5 / 0 |
| %        | NO       | NO        | YES          | Return 5 % 0 |

## 日期运算

除了数值类型，算术运算也可以在 datetime 和 interval 类型上进行。Neug 支持日期时间算术运算，允许你对日期和时间戳进行加减 interval 操作，以及计算时间值之间的差值。

### 支持的操作

下表详细列出了支持的日期算术操作：

| 操作 | 描述 | 示例 | 结果 |
|-----------|-------------|---------|--------|
| DATE + INTERVAL | 日期加上时间间隔 | `DATE('2011-02-15') + INTERVAL('5 DAYS')` | `DATE('2011-02-20')` |
| DATE - INTERVAL | 日期减去时间间隔 | `DATE('2011-02-15') - INTERVAL('5 DAYS')` | `DATE('2011-02-10')` |
| TIMESTAMP + INTERVAL | 时间戳加上时间间隔 | `TIMESTAMP('2011-10-21 14:25:13') + INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-22 20:25:33')` |
| TIMESTAMP - INTERVAL | 时间戳减去时间间隔 | `TIMESTAMP('2011-10-21 14:25:13') - INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-20 08:24:53')` |
| INTERVAL + INTERVAL | 两个时间间隔相加 | `INTERVAL('5 DAYS') + INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('6 DAYS 6 HOURS 20 SECONDS')` |
| INTERVAL - INTERVAL | 时间间隔相减 | `INTERVAL('5 DAYS') - INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('3 DAYS 17 HOURS 39 MINUTES 40 SECONDS')` |
| TIMESTAMP - TIMESTAMP | 计算时间差 | `TIMESTAMP('2011-10-21 14:25:13') - TIMESTAMP('1989-10-21 14:25:13')` | `INTERVAL('8035 DAYS')` |