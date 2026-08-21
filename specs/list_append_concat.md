# LIST_APPEND / LIST_CONCAT 设计方案

## 1. 背景与目标

在内置函数集合中新增两个 `NeugScalarFunction` 列表函数：

```text
LIST_APPEND(LIST<T>, T) -> LIST<T>
LIST_CONCAT(LIST<T>, LIST<T>) -> LIST<T>
```

示例：

```cypher
RETURN list_append([1, 2], 3)
-- [1, 2, 3]

RETURN list_concat([1, 2], [3, 4])
-- [1, 2, 3, 4]
```

本需求还允许输入为定长 `ARRAY`。绑定阶段在保持输入容器类型的前提下统一元素类型，执行回调同时读取 LIST/ARRAY；无论输入是 LIST、ARRAY，还是二者混用，返回值始终是变长 `LIST`。

本方案的目标是：

- 支持 LIST/ARRAY 输入以及元素类型的隐式转换；
- 在绑定期确定唯一的公共元素类型，并插入必要的 CAST；
- 保持与现有 `LIST + LIST` 相同的拼接顺序和深层值复制语义；
- 沿用扩展标量函数执行路径的 NULL 传播语义；
- 不改变现有 `LIST + LIST` 的语法和行为。

## 2. 当前实现分析

### 2.1 LIST 与 ARRAY

- 列表字面量 `[1, 2]` 当前由 `ListCreationFunction` 绑定为 `ARRAY<INT64, 2>`；空字面量 `[]` 绑定为 `LIST<UNKNOWN>`。
- LIST 与 ARRAY 在执行层都由 `list_entry_t` 表示，`ListVector` 也同时服务于两者，因此数据复制逻辑可以共用。
- `CastArrayHelper::checkCompatibleNestedTypes()` 和嵌套类型 cast executor 已支持 LIST/ARRAY 之间互转。
- 但 `CastFunction::hasImplicitCast()` 当前要求两个嵌套类型的逻辑类型 ID 相同，所以 ARRAY→LIST 虽然可显式 CAST，尚不能由普通函数参数匹配自动插入。

结论：底层值表示允许新函数同时读取 ARRAY 与 LIST。为避免扩大隐式转换对其他语法的影响，本需求不新增全局 ARRAY→LIST 规则，而是在函数绑定与 Value 回调内局部支持两种容器。

### 2.2 现有列表拼接

`ListConcat::operation()` 已实现 `LIST + LIST` 的向量执行内核：为结果分配 `left.size + right.size` 个元素，然后通过 `copyFromVectorData()` 依次复制两侧元素。命名函数需要走 gopt 的扩展函数路径，该路径只接受 `NeugScalarFunction` 的 `Value` 回调，因此不能直接挂接向量 executor；实现中保留原 operation 供 `+` 使用，并在 `LIST_CONCAT` 的 Value 回调中实现等价的左后右复制顺序。

当前 `AddFunction::getFunctionSet()` 已构造 LIST 重载，但创建出的 `func` 未加入结果集合。这属于现有代码问题，不在本需求中顺带修改；实现新函数时直接注册独立的 `LIST_CONCAT` 即可。

### 2.3 NeugScalarFunction 绑定机制

`ScalarFunction::bindFunc` 可以根据实参完整的 `DataType`：

1. 校验实参是否为 LIST/ARRAY；
2. 推导公共元素类型；
3. 返回精确的参数类型和结果类型；
4. 将调用转换为 gopt 扩展函数，并在执行期通过 `Value` 回调构造结果。

表达式绑定器会根据 `FunctionBindData::paramTypes` 调用 `implicitCastIfNecessary()`，把实参转换为绑定函数要求的类型。因此两个函数都采用一个 `UNKNOWN` 占位的 `NeugScalarFunction` 候选签名加自定义 `bindFunc`，而不是为每种元素类型枚举重载。

空列表字面量是零参数 `LIST_CREATION()` 函数表达式，不是普通 LiteralExpression。将其特化为 `LIST<T>` 时必须走 CAST binder 的空列表分支，不能调用基础 `Expression::cast()`。此外，`LIST<UNKNOWN>` 的空结果需要允许创建空的子列占位，但不会物化 UNKNOWN 元素。

## 3. 对外语义

### 3.1 LIST_APPEND

```text
LIST_APPEND(list_like, element) -> LIST<T>
```

- 第一个参数必须是 `LIST<A>` 或 `ARRAY<A, N>`。
- 通过 `A` 与第二个参数类型 `B` 推导公共类型 `T`。
- 第一个参数转换为 `LIST<T>`，第二个参数转换为 `T`。
- 返回 `LIST<T>`，元素顺序为原列表元素后跟新元素。

示例：

```cypher
list_append([1, 2], 3)       -- LIST<INT64>: [1, 2, 3]
list_append([1, 2], 3.5)     -- LIST<DOUBLE>: [1.0, 2.0, 3.5]
list_append([], 1)           -- LIST<INT64>: [1]
```

如果 `A` 与 `B` 不存在公共可转换类型，则在绑定期报错，不推迟到逐行执行阶段。

### 3.2 LIST_CONCAT

```text
LIST_CONCAT(left_list_like, right_list_like) -> LIST<T>
```

- 两个参数都必须是 LIST 或 ARRAY。
- 根据两侧元素类型 `A`、`B` 推导公共类型 `T`。
- 两侧统一转换为 `LIST<T>`。
- 返回 `LIST<T>`，其内容与现有 `LIST + LIST` 语义等价。

示例：

```cypher
list_concat([1, 2], [3, 4])       -- LIST<INT64>
list_concat([1, 2], [3.5, 4.5])   -- LIST<DOUBLE>
list_concat([], [1, 2])           -- LIST<INT64>
```

### 3.3 NULL 语义

沿用扩展标量函数回调的 NULL 传播规则：

- 任意顶层参数为 NULL，结果为 NULL；
- 列表内部的 NULL 元素原样保留；
- `list_append([1, 2], NULL)` 中，无类型 NULL 根据列表元素类型转换后追加为 NULL；
- 如果两侧都无法提供具体元素类型，例如 `list_concat([], [])`，结果类型保留为 `LIST<UNKNOWN>`，运行结果为空列表。

## 4. 类型推导与 CAST 规则

### 4.1 list-like 类型抽象

在列表函数实现文件内增加小型辅助逻辑：

```text
isListLike(type)       := type is LIST or ARRAY
getElementType(type)   := LIST child type or ARRAY child type
toListType(T)          := LIST<T>
```

不新增公开逻辑类型，也不把 ARRAY 的长度带入结果类型，因为两个函数都可能改变长度。

### 4.2 公共元素类型

使用现有 `LogicalTypeUtils::tryGetMaxLogicalType(A, B, T)` 推导公共元素类型。这样数值提升、UNKNOWN 以及嵌套类型的规则与 CASE、比较表达式等现有行为保持一致。

规则如下：

1. `A == UNKNOWN` 时取 `B`；
2. `B == UNKNOWN` 时取 `A`；
3. 两者均为 UNKNOWN 时保留 UNKNOWN；
4. 否则调用 `tryGetMaxLogicalType`；失败则抛出包含函数名及两个实参类型的 Binder/Conversion 异常。

即使可以求得 `T`，绑定器仍需验证每个源类型能转换到目标类型；不允许通过公共类型推导绕过 cast 合法性检查。

### 4.3 ARRAY 输入的局部转换策略

不修改全局 `CastFunction::hasImplicitCast()` 的 LIST/ARRAY 交叉规则。全局开放 ARRAY→LIST 会让 CREATE/SET 等原本要求显式 CAST 的路径也接受裸数组字面量，并会改变部分 LIST_CREATION/UNWIND 的既有绑定结果，超出本需求范围。

两个函数的 bindFunc 按源容器分别生成目标参数类型：

```text
ARRAY<A, N> -> ARRAY<T, N>
LIST<A>     -> LIST<T>
```

这样现有同容器嵌套 cast 只负责把子元素转换到公共类型 `T`；Value 回调根据运行时参数的顶层类型分别用 `ListValue` 或 `ArrayValue` 读取子元素，再统一构造 `LIST<T>` 结果。该策略将 ARRAY 支持严格限制在新函数内，并保持 LIST→ARRAY 仍只能显式发生。

## 5. 函数设计

### 5.1 LIST_APPEND

新增 `ListAppendFunction`。为了确保 ARRAY 在候选匹配阶段不会因顶层类型 ID 不同而被提前淘汰，候选签名使用：

```text
(UNKNOWN, UNKNOWN) -> LIST
```

真正的 `(LIST/ARRAY, element)` 约束由 `bindFunc` 严格检查。

其 `bindFunc`：

1. 校验参数数量为 2；
2. 校验参数 0 是 LIST/ARRAY；
3. 取得参数 0 的元素类型 `A` 与参数 1 类型 `B`；
4. 推导公共元素类型 `T`；
5. 参数 0 为 LIST 时设置 `paramTypes = [LIST<T>, T]`，为 ARRAY 时保留长度并设置为 `[ARRAY<T, N>, T]`；
6. 设置 `resultType = LIST<T>`；
7. 注册 `NeugScalarFunction` 的 Value 回调。

`ListAppend::operation()` 的执行步骤：

1. 顶层列表为 NULL 时返回同类型 NULL；
2. 复制输入 `Value` 的子元素；
3. 将第二个参数追加到子元素数组；
4. 使用推导出的元素类型构造 `Value::LIST`。

必须复制 `Value` 对象而不是对元素做裸内存复制，以正确保留 STRING、LIST、STRUCT 等嵌套值的所有权和 NULL 信息。

### 5.2 LIST_CONCAT

新增 `ListConcatFunction`，候选签名为：

```text
(LIST, LIST) -> LIST
```

为了让函数匹配阶段接受 ARRAY，可以注册 LIST/ARRAY 四种顶层组合，或把候选参数声明为 UNKNOWN 并在 `bindFunc` 中严格校验。推荐后者：

```text
(UNKNOWN, UNKNOWN) -> LIST
```

这样只保留一个候选，完整合法性由 bindFunc 控制，避免四个重载产生同成本歧义。其 `bindFunc`：

1. 校验两个参数都是 LIST/ARRAY；
2. 获取两侧元素类型 `A`、`B`；
3. 推导公共元素类型 `T`；
4. 两侧分别保持原容器种类，设置为 `LIST<T>` 或保留长度的 `ARRAY<T, N>`；
5. 设置 `resultType = LIST<T>`；
6. 设置 `NeugScalarFunction` 的 Value 回调，按左侧后右侧的顺序复制子元素。

现有 `ListConcat::operation()` 保持不变，继续服务 `+` 的向量执行；命名函数因后端接口不同使用等价的 Value 实现，并由相同的行为测试约束拼接顺序和 NULL 语义。

### 5.3 函数注册与查询转换

- 在 `vector_list_functions.h` 声明 `ListAppendFunction` 和 `ListConcatFunction`，名称分别为 `LIST_APPEND`、`LIST_CONCAT`。
- 在 `FunctionCollection::getFunctions()` 中注册两个 `NeugScalarFunction`。
- 在 `src/compiler/function/list/CMakeLists.txt` 中加入新增实现文件；若将 append 和 concat 的注册实现放在已有 `list_concat_function.cpp`，则无需新增构建项，但建议按函数拆分以保持结构清晰。
- 两者是普通函数调用，不是中缀运算符。`GExprConverter::convertScalarFunc()` 沿用 `convertExtensionFunc()` 回退路径，因此注册类型必须是 `NeugScalarFunction`，无需加入算术枚举或修改优先级。
- `convertToListFunc()` 允许零字段 `ToList`，以便空列表字面量作为函数实参进入执行后端。

## 6. 预计代码改动

| 文件 | 改动 |
| --- | --- |
| `include/neug/compiler/function/list/vector_list_functions.h` | 声明两个函数类和名称 |
| `include/neug/compiler/function/list/functions/list_append_function.h` | 声明 append operation |
| `src/compiler/function/list/list_append_function.cpp` | append 执行及绑定逻辑 |
| `src/compiler/function/list/list_concat_function.cpp` | 保留拼接 operation，增加 LIST_CONCAT 的绑定和函数集合 |
| `src/compiler/function/list/CMakeLists.txt` | 加入新增源文件 |
| `src/compiler/function/function_collection.cpp` | 注册两个函数 |
| `src/compiler/binder/expression/expression_util.cpp`、`expression_binder.cpp` | 识别零参数 LIST_CREATION，并通过 CAST binder 特化空列表类型 |
| `src/compiler/gopt/g_expr_converter.cpp` | 允许转换零元素列表 |
| `include/neug/common/columns/list_columns.h` | 支持完成空的 LIST<UNKNOWN> 结果列 |
| `tools/python_bind/tests/test_db_list.py` | 增加语法、类型、NULL、空列表和错误用例 |

为避免 append/concat 分别实现一遍元素类型推导，可在 list 目录下提供仅实现文件可见的辅助函数，或增加内部 header；首版不需要提升为公共 API。

## 7. 错误处理

以下情况应在绑定期失败，并给出函数名、实参类型和期望形态：

- `list_append(1, 2)`：第一个参数不是 LIST/ARRAY；
- `list_concat([1], 2)`：第二个参数不是 LIST/ARRAY；
- `list_append([1], {a: 1})`：元素类型不存在公共可转换类型；
- `list_concat([1], ['a'])`：两侧元素类型不存在公共可转换类型；
- 参数数量不是 2。

建议错误格式：

```text
Cannot bind LIST_APPEND with arguments (ARRAY<INT64, 1>, STRUCT{...}):
cannot find a common element type for INT64 and STRUCT{...}.
```

cast 对具体值失败（例如将字符串参数转换为数值）时，沿用现有 ConversionException，不在函数内吞掉或改写。

## 8. 测试方案

### 8.1 基本功能

- ARRAY + element：`list_append([1, 2], 3)`；
- ARRAY + ARRAY：`list_concat([1, 2], [3, 4])`；
- LIST 列属性作为输入；
- 结果继续参与 `list_extract`、`UNWIND` 或相等比较。

### 8.2 类型转换

- INT 与 DOUBLE 的公共类型提升；
- ARRAY 与 LIST 混用；
- 两侧 ARRAY 长度不同仍能 concat；
- STRING、DATE 等非数值同类型；
- STRUCT、嵌套 LIST 等元素通过 `Value` 复制正确保留；
- 不可转换类型在绑定期报错。

### 8.3 空列表与 NULL

- `list_append([], 1)`；
- `list_concat([], [1])`、`list_concat([1], [])`、`list_concat([], [])`；
- 顶层 NULL 参数返回 NULL；
- append NULL 元素；
- 输入列表包含 NULL 元素。

### 8.4 多行执行

- 常量列表 + 列值、列列表 + 常量值；
- 两侧均为非 flat vector；
- 多行结果长度不同，验证 auxiliary buffer 的 offset/size；
- 大列表与嵌套值，确认无越界、无结果间数据串扰。

### 8.5 回归

- 现有 `LIST + LIST`（若当前测试可达）行为不变；
- 显式 LIST/ARRAY CAST 行为不变；
- CREATE/SET 等既有路径不会获得新的 ARRAY→LIST 隐式转换能力；
- 现有 `LIST_CONTAINS`、`LIST_EXTRACT` 和列表字面量测试通过。

## 9. 实施顺序

1. 实现 LIST/ARRAY 输入的局部类型归一与元素读取；
2. 实现公共元素类型推导辅助逻辑；
3. 注册并实现 LIST_CONCAT 的等价 Value 回调；
4. 实现 LIST_APPEND 的 Value 回调与注册；
5. 增加绑定错误、NULL、混合类型及多行执行测试；
6. 运行相关 e2e 测试、Python 查询测试和格式检查。

## 10. 非目标与后续事项

- 本需求不新增 `LIST_PREPEND`、可变参数 concat 或原地修改列表；
- 不让 LIST 隐式转换为定长 ARRAY；
- 不改变列表字面量当前“非空为 ARRAY、空为 LIST”的类型规则；
- 不处理现有 `AddFunction` 中 LIST 重载未加入 function set 的问题，建议另开修复并补充 `LIST + LIST` 回归测试；
- 首版不对拼接做零拷贝优化。结果 `Value::LIST` 拥有独立的子值集合，保证后续算子生命周期安全。
