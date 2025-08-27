# 数据模型

## 数据类型

`Neug` 为顶点和边属性中使用的各种数据类型提供了强大的支持。这些数据类型被组织成五个不同的类别：

- **Primitive Types:** 包括整数、浮点数和布尔值等基本数据类型，用于表示基础的数值和逻辑值。

- **String Types:** 这个类别包含处理文本数据的类型，允许在顶点和边中高效存储和操作字符串。

- **Temporal Types:** 这些类型用于处理日期和时间信息，支持各种时间表示方式，便于进行时间序列分析和查询。

- **Array Types:** 数组类型允许存储元素集合，能够表示与图元素关联的值列表或集合。

- **Complex Types:** （目前不支持）可能包括用户自定义对象或组合多个基本类型的复合结构，用于在图中实现更复杂的数据建模和关系。

### Primitive Types

当然！以下是包含指定标题和第一行条目的 Markdown 表格：

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

### 时间类型

#### DATE

`DATE` 表示一个日历日期，由年、月、日组成，格式遵循 ISO-8601 标准 (YYYY-MM-DD)。存储时占用 4 个字节。

```cypher
RETURN date('2022-06-06') as x;
```

#### DATE32

`DATE32` 通过计算自 1970 年 1 月 1 日以来的天数来表示一个日历日期。在此日期之前的天数用负值表示。存储时占用 4 个字节。(TODO: (xiaoli): 确认功能正常)

```cypher
RETURN date32('2022-06-06') as x;
```

#### TIMESTAMP

`TIMESTAMP` 集成了日期和时间组件——小时、分钟、秒和毫秒，采用 ISO-8601 标准格式 (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])。此格式指定了日期 (YYYY-MM-DD)、时间 (hh:mm:ss[.zzzzzz]) 和可选的时间偏移量 [+-TT[:tt]]。日期部分是必需的，而时间组件是可选的，允许包含毫秒 [.zzzzzz] 和时间偏移量（如果需要）。它在存储中占用 8 字节。例如："1970-01-01 00:00:00.004666-10"

```cypher
RETURN timestamp("1970-01-01 00:00:00.004666-10") as x;
```

#### INTERVAL

`INTERVAL` 表示一个时间段，可以由年、月、日、小时、分钟、微秒等组件来描述。我们遵循 [kuzu](https://docs.kuzudb.com/cypher/data-types/#interval) 中对 Interval 的定义。

```cypher
RETURN interval("1 year 2 days") as x;
```

### Array Types

数组类型目前还不支持，但计划在不久的将来支持。
一旦支持后，要求数组中的每个元素都必须符合前面提到的原始类型之一。

### Complex Types

复杂类型目前不支持，包括 `STRUCT`、`MAP`、`UNION`、`BLOB`。

## 类型转换

我们支持[数值类型之间的类型转换](https://github.com/GraphScope/neug/issues/416)。

- INT32
- UINT32
- INT64
- UINT64
- FLOAT
- DOUBLE

**这些类型之间所有成对的转换都是允许的**。在进行类型转换时，编译器会执行溢出检查以确保转换的安全性。下表总结了在这些转换过程中处理的潜在溢出场景：

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX)         | ⚠️ May Overflow (if value > UINT32_MAX)       | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |