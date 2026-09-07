# Dynamic Parameter Support

本文档记录 NeuG `main` 分支在 `3ef11080` 上的动态参数支持边界，并对
[#991](https://github.com/alibaba/neug/issues/991) 中暴露的 compiler/engine
能力错位进行归类。

## 判定标准

Binder 将 `$name` 表示为 `ParameterExpression`，converter 再将其编码为
`common.DynamicParam`。Engine 侧的通用表达式路径已经具备运行时绑定能力：

1. `PlanParser::parse_params_type()` 从 physical plan 收集参数名和类型；
2. `parseJsonParameters()` 按类型将请求参数转换成 `ParamsMap`；
3. `parse_expression()` 将 `DynamicParam` 构造成 `ParamExpr`；
4. operator 在 `Eval()` 中将 `ParamsMap` 传给表达式，`ParamExpr::bind()` 将
   参数替换成 `ConstExpr`。

因此，proto 中存在 `common.Expression` 字段并不等于已经支持动态参数。一个
场景只有同时满足以下条件才算端到端支持：参数能够获得确定类型、参数所在的
嵌套表达式能被参数元数据收集器遍历到、operator 会使用 `ParamsMap` 绑定表达式，
并且该值不参与 plan shape、返回类型或 schema 的编译期决策。

主要实现位置：

- Compiler 编码：`src/compiler/gopt/g_expr_converter.cpp` 中的 `convertParam()`
- Engine 参数表达式：`src/execution/expression/accessors/const_accessor.cc`
- Engine 参数类型收集：`src/execution/execute/plan_parser.cc`
- 请求参数反序列化：`src/execution/common/params_map.cc`

## 当前已支持

以下场景已经在 compiler 与 engine 两侧对齐。

| 场景 | 示例 | 实现说明 |
| --- | --- | --- |
| 通用投影和谓词 | `RETURN lower($value)`、`WHERE n.age >= $age` | `Project`、`Select`、`Scan`、`GetV`、`EdgeExpand` 等通过通用 `ExprBase` 在 `Eval()` 时绑定参数。 |
| 算术、比较、布尔及 `CASE` | `WHERE n.x = $x AND n.y > $y` | `ArithExpr`、`BinaryLogicalExpr`、`CaseWhenExpr` 会递归绑定子表达式；参数类型仍须能从 operand 或 `CASE` 的其他分支推导。 |
| NeuG scalar function 参数 | `RETURN lower($value)` | `ScalarFunctionExpr::bind()` 会递归绑定全部 children。该能力由 #913 补齐。只适用于已实现 `NeugScalarFunction::neugExecFunc` 的函数。 |
| 字符串谓词 | `WHERE n.c STARTS WITH $prefix`、`ENDS WITH $suffix`、`CONTAINS $pattern` | 三个谓词通过 `NeugScalarFunction` 绑定两个 child expression，在执行阶段组装 regex，并复用 engine 的 regex evaluator。 |
| `IN`/list membership | `WHERE n.id IN $ids` | 普通过滤及主键索引路径均支持；主键 `IN` 参数优化由 #978、#993 补齐。 |
| 主键等值查询 | `MATCH (n:T {id: $id})` | `IndexPredicate.expression` 会在执行时通过 `ScanUtils` 绑定。 |
| 写操作的属性值 | `CREATE (:T {id: $id})`、`SET n.x = $x`、`MERGE` 的属性/on-create/on-match | Create、Set、Merge operator 都会在执行时绑定 property mapping expression，参数类型收集器也覆盖了这些字段。 |
| FTS/HNSW index scan 的查询值 | `CALL QUERY_FTS_INDEX(..., $query, ...)` 等 | `IndexScan.target_value`/`weights` 是表达式；FTS/HNSW 的 input 实现了 `bindParams()`。 |
| 支持 deferred binding 的 procedure | `CALL TEST_ECHO_PARAM($value)` | Procedure 参数可编码为 `DynamicParam`；具体 procedure 的 `CallFuncInputBase` 仍需实现 `bindParams()`。 |

每个支持场景由独立测试覆盖：

| 场景 | 回归测试 |
| --- | --- |
| 通用投影和谓词 | `test_dynamic_parameter_in_general_projection_and_predicate`，同时经过 `Scan`、`EdgeExpand`、`GetV`、`Select` 和 `Project`。 |
| 算术、比较、布尔及 `CASE` | `test_dynamic_parameters_in_arithmetic_comparison_boolean_and_case`。 |
| NeuG scalar function 参数 | `test_dynamic_parameter_in_neug_scalar_function`。 |
| 字符串谓词 | `test_dynamic_parameters_in_string_predicates`，在同一测试中分别验证 `STARTS WITH`、`ENDS WITH` 和 `CONTAINS`。 |
| `IN`/list membership | `test_dynamic_parameter_in_list_membership`，分别验证主键索引路径和非主键普通过滤路径。 |
| 主键等值查询 | `test_dynamic_parameter_in_primary_key_equality`。 |
| 写操作的属性值 | `test_dynamic_parameters_in_write_property_values`，分别验证 `CREATE`、`SET`、`MERGE ON MATCH` 和 `MERGE ON CREATE`。 |
| FTS/HNSW index scan 的查询值 | `test_fts_dynamic_query_parameter_is_bound_per_execution`、`test_fts_multi_column_bm25_constant_and_dynamic_arguments` 和 `test_hnsw_index_scan_with_dynamic_target`。 |
| 支持 deferred binding 的 procedure | `test_call_echo_param`。 |

前七项位于 `tools/python_bind/tests/test_query.py`。FTS、HNSW 和 deferred
procedure 测试分别位于 `test_fts_index.py`、`test_hnsw_index.py` 和
`test_call_params.py`，需要在启用相应 extension 的构建中执行。

本次审计发现的唯一一个“engine 表达式层已有基础能力、compiler 仍主动拒绝”的
差异是 `CONTAINS`、`STARTS WITH`、`ENDS WITH` 的动态右操作数。本变更已将三个
谓词统一为接收两个 child expression 的 `NeugScalarFunction`：compiler 不再要求
右操作数为 literal，engine 在参数绑定后组装 `^x.*`、`.*x$` 或 `.*x.*`，并复用
已有的 regex evaluator。因此 literal 和动态参数现在走同一条执行链路。

## Engine 仍未端到端支持

### Operator 控制参数

| 场景 | 当前阻塞点 | 结论 |
| --- | --- | --- |
| `SKIP $skip` / `LIMIT $limit` | Binder 接受 parameter，但 converter 的 `convertRange()` 只接受 literal；physical `algebra.Range` 只有固定 `int32 lower/upper`，`LimitOpr` 和 `OrderByOpr` 也在 build 阶段保存固定范围。 | 不是单纯的 compiler 限制。需要先让 physical plan 和 operator 支持运行时 range，或在每次执行前实例化 range。 |
| repeated path hop range | `PathExpand.hop_range` 同样是固定 `algebra.Range`。 | `$min..$max` 不能通过现有 expression 参数链路实现。 |
| 文件路径、COPY/scan options、ATTACH/DDL options | 这些值决定 reader、schema、catalog 或 plan shape，binder 会读取 literal 值。 | 应继续要求 literal，除非引入专门的运行时配置协议。 |

`SKIP/LIMIT` 当前存在一处内部不一致：`bindSkipLimitExpression()` 声明接受
parameter，但 `GQueryConvertor::convertRange()` 必然拒绝它。短期应让 binder
与真实能力一致并给出明确错误；若要正式支持，则必须先修改 `Range` 和对应
operator，而不能只放宽 converter。

### 需要编译期决定类型或字段的函数参数

以下参数虽然在语法上也是 expression，但其值决定返回类型、字段下标或执行
函数签名，现有 engine 不能延迟到运行时处理：

| 场景 | 原因 |
| --- | --- |
| `CAST(value, $type)` | target type 决定表达式返回类型和函数签名，`castBindFunc()` 要求第二个参数为 literal。 |
| struct/union 动态 key | `StructExtractBindData` 在 bind 阶段计算固定 `fieldIdx` 和返回类型。 |
| `properties(nodes(path), $property)` | `PropertiesBindData` 在 bind 阶段按 property name 计算固定 `fieldIdx` 和 list element type。 |
| `date_part($field, value)` | field 被 converter 编码为固定的 `Extract::Interval` enum。 |

这些限制与 engine 当前的数据类型模型一致，不应仅在 compiler 中放开。

### Temporal constructor

`DATE($value)`、`DATETIME($value)`、`INTERVAL($value)` 当前不能正确支持。
Converter 没有保留 child expression，而是把 `child->toString()` 写入
`ToDate.date_str`、`ToDatetime.datetime_str` 或 `ToInterval.interval_str`。因此
`DATE($value)` 实际会尝试解析字符串 `"$value"`，而不会读取 `ParamsMap`。

要支持它们，需要把 temporal proto 改成持有 `common.Expression`，或统一走已有
runtime cast/scalar-function 路径。

### Data source 下推谓词

`DataSource.skip_rows` 虽然是 `common.Expression`，但当前仍不支持其中的动态
参数：

- `DataSourceOpr::Eval()` 没有把 `ParamsMap` 传给 reader；
- CSV/JSON 的 `RowExpressionFilter` 只识别 const、variable 和 logical token；
- Parquet 的 `ArrowExpressionConverter` 也不识别 `DynamicParam`。

因此不能根据 proto 字段类型将这一路径判断为已支持。需要先在 reader state
构造或每次 scan 执行时绑定参数，再执行下推转换。

### 参数类型收集器的遍历缺口

Engine 的表达式执行类可以递归绑定更多场景，但
`PlanParser::parse_params_type()` 尚未完整遍历 physical plan。未被收集的请求
参数会被 `parseJsonParameters()` 当作未知 key 忽略，随后 `ParamExpr::bind()`
找不到值。当前缺口包括：

- expression 内的 `ToList`、`ToArray`、`ToTuple`、`UserDefinedFunction` 和嵌套
  map/path expression children；目前只递归 `Case` 与 `ScalarFunction`；
- `Apply.sub_plan`；
- `PathExpand.base.get_V.params.predicate`、`PathExpand.condition` 和
  `PathExpand.extra_info.weight_expr`；
- `DataSource.skip_rows`；
- `GDSAlgo.sub_graph` 中的 vertex/edge predicates。

其中前几项的具体 evaluator/operator 大多已经接收 `ParamsMap`，主要缺的是一个
统一的递归参数收集函数。建议让 `expression_parse()` 覆盖 `ExprOpr` 中所有持有
child expression 的 oneof，并让 plan traversal 覆盖所有嵌套 plan 和 expression
字段，避免以后每增加一种表达式或 operator 都再次产生边界错位。

### 缺少上下文类型的独立参数

`RETURN $value` 和 `UNWIND $items AS item` 之类没有任何类型上下文的参数仍不
安全。`DynamicParam.data_type` 来自 binder；如果类型保持 `UNKNOWN`，engine
无法可靠地从 JSON 构造 `Value`。`UNWIND` 的 operator 还需要在 build 阶段知道
list/array 的 child type，裸参数无法提供该信息。参数用于比较、已签名 scalar
function、属性赋值等场景时，binder 可以从另一侧或函数签名推导类型，因此不受
此限制。

## 建议修复顺序

1. 将 `PlanParser` 的参数发现改造成完整的 expression/plan 递归遍历，并补充
   nested expression、`Apply`、`PathExpand` 的参数测试。
2. 决定是否支持运行时 `SKIP/LIMIT`；若支持，先修改 proto 和 operator，若不
   支持，则让 binder 直接给出一致的 unsupported 提示。
3. 将 temporal constructor 统一到 runtime scalar/cast 路径。
4. 对必须在 bind 阶段确定的 type、field、schema/config 参数保留 literal
   限制，并在用户文档中明确说明。
