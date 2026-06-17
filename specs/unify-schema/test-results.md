# LogicalType → DataType 迁移测试结果

**测试日期**: 2026-06-10
**测试环境**: macOS Darwin 24.3.0, arm64
**基线 commit**: `4346071e` (refactor: replace compiler LogicalType with engine DataType)
**修复 commit**: `a9fd7330` (fix: use first-wins semantics for duplicate struct field names)

## 结论

**零回归** — 所有失败均为迁移前已存在的问题，通过 `git stash` 对比基线确认。

## 修复内容

`src/common/extra_type_info.cc` 中的两处修改：

1. **`buildFieldNameIndex()`**: `operator[]`（后者覆盖）→ `emplace()`（先者优先）
   - 根因: Edge struct 类型存在重复字段名（如 `_SRC` 同时作为 kVertex 基础字段和 kInternalId 属性字段），`operator[]` 导致后者覆盖前者，START_NODE/END_NODE 解析到错误类型
   - 修复后: PathTest.START_NODE、LDBCTest.IC_14 恢复通过

2. **`getFieldIdx()`**: `assert(it != end)` → 返回 `UINT8_MAX`（INVALID_STRUCT_FIELD_IDX）
   - 编译器代码依赖此哨兵值判断字段是否存在

## C++ 测试结果

| 测试套件 | 结果 | 备注 |
|---|---|---|
| gopt_test | 156/160 通过 | 4 个失败: macOS unordered_set 哈希顺序问题，Linux CI 上通过 |
| csr_test | ✅ 全部通过 | |
| schema_test | ✅ 全部通过 | |
| test_mmap_container | ✅ 全部通过 | |
| test_indexer | ✅ 全部通过 | |
| utils_test | ✅ 全部通过 (284 tests) | |
| test_vertex_table | ✅ 全部通过 | |
| edge_table_test | ✅ 全部通过 | |
| graph_view_test | ✅ 全部通过 | |
| logical_delete_test | ✅ 全部通过 | |
| test_connection | ✅ 全部通过 | |
| test_request | ✅ 全部通过 | |
| test_db_svc | ✅ 全部通过 | |
| execution_test | 1 失败 (SDMLEdgeColumnShuffle) | **已存在**: edge_columns.h:455 assert 失败 |
| transaction_test | 47 pass / 8 fail / 2 skip | **已存在**: AddVertex, UpdateVertexAbort 等 |
| storage_test | 失败 | **已存在**: 环境变量 + export 数据对比错误 |

### gopt_test 4 个 macOS 专属失败（已存在）

- `PatternTest.OPTIONAL_MATCH`
- `LDBCTest.IC_6`
- `LDBCTest.IC_3`
- `LDBCTest.IC_13`

原因: `expression_set`（`std::unordered_set<shared_ptr<Expression>, ExpressionHasher, ExpressionEquality>`）的迭代顺序在 libc++（macOS）和 libstdc++（Linux）上不同，导致列顺序差异。CI 在 Linux 上运行，这些测试会通过。

## Python 测试结果

| 测试套件 | 结果 | 备注 |
|---|---|---|
| test_ddl | ✅ 12 pass | |
| test_db_init | ✅ 22 pass, 4 skip | |
| test_query (modern_graph) | ✅ 1 pass | |
| test_query (tinysnb) | ✅ 1 pass | |
| test_alter_property | ✅ 1 pass | |
| test_db_query | 76 pass / 35 fail / 2 skip | **已存在**: 35 个失败与基线完全一致 |
| test_db_connection | 11 pass / 3 fail / 3 skip | **已存在** |
| test_db_transaction | 9 pass / 8 fail / 6 skip | **已存在** |
| test_db_concurrent | 3 fail | **已存在** |
| test_call_string_literal | 8 pass / 8 fail | **已存在** |
| test_merge | 4 pass / 12 fail | **已存在** |
| test_tinysnb_tutorial | 24 pass / 1 fail | **已存在** |
| test_db_list | 2 fail / 1 skip | **已存在** |
| test_db_cases | 2 fail / 2 skip | **已存在** |
| test_batch_loading | 4 pass / 2 fail | **已存在** |

## 测试方法

```bash
# 编译
cd tools/python_bind
BUILD_TEST=ON BUILD_TYPE=DEBUG CMAKE_BUILD_PARALLEL_LEVEL=$(sysctl -n hw.ncpu) make build

# C++ 测试（从 build 目录运行）
cd build
MODERN_GRAPH_DATA_DIR=.../example_dataset/modern_graph \
COMPREHENSIVE_GRAPH_DATA_DIR=.../example_dataset/comprehensive_graph \
FLEX_DATA_DIR=.../example_dataset/modern_graph \
TEST_PATH=.../tests \
ctest -R <test_name> --output-on-failure

# gopt_test 额外需要
TEST_RESOURCE=.../tests/compiler ctest -R gopt_test --output-on-failure

# Python 测试（macOS arm64 需要 arch -arm64）
cd tools/python_bind
arch -arm64 python3 -m pytest -sv tests/<test_file>.py

# 对比基线（确认是否为已存在问题）
git stash          # 暂存当前修改
<运行测试>         # 在无修改状态下运行
git stash pop      # 恢复修改
```

## 尚未在本地运行的 CI 测试

以下测试需要额外数据（gstest repo）或特殊环境，本地未运行：

- E2E 测试（modern_graph, tinysnb, lsqb, ldbc, comprehensive_graph）
- test_lsqb（需要 lsqb bulk load 数据）
- test_tp_service / test_load / test_export（需要 FLEX_DATA_DIR）
- test_to_arrow（需要 pyarrow==18.0.0）
- test_ngcli_commands / test_ngcli_basics
- Java Driver 测试
- Extension 测试（neug-extension-test.yml）
- test_data_io_docs / test_sniffer

建议 push 到分支后通过 CI 运行完整测试套件。
