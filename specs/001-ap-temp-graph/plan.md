# Implementation Plan: AP Temporary Graph (临时点边)

**Branch**: `001-ap-temp-graph` | **Date**: 2025-12-09 | **Spec**: [spec.md](./spec.md)  
**Input**: Feature specification from `/specs/001-ap-temp-graph/spec.md`

## Summary

实现 NeuG 图数据库在 AP（Embedded）模式下的临时点边功能，支持从外部数据源（CSV/JSON/Parquet）载入临时点和边数据，与持久图进行统一的图查询。功能分三个优先级交付：P1 实现本地文件读取和关系型查询，P2 实现临时图载入和混合查询，P3 扩展网络资源支持（HTTP/S3/OSS）。

## Technical Context

**Language/Version**: C++20 (主实现), Python 3.8+ (bindings)  
**Primary Dependencies**: Apache Arrow (数据格式), RapidJSON (JSON解析), pybind11 (Python绑定)  
**Storage**: 内存临时存储 (Connection-local)  
**Testing**: GoogleTest (C++), pytest (Python)  
**Target Platform**: Linux (primary), macOS (development)  
**Project Type**: Single project (embedded library + optional server)  
**Performance Goals**: 百万行 CSV 10秒内完成 LOAD...RETURN  
**Constraints**: 临时数据内存上限为可用内存 50%  
**Scale/Scope**: 单 Connection 级别的临时数据隔离

## Project Structure

### Documentation (this feature)

```text
specs/001-ap-temp-graph/
├── spec.md              # 功能规格说明
├── plan.md              # 本文件 - 实现计划
├── tasks.md             # 任务列表
└── checklists/
    └── requirements.md  # 规格验证清单
```

### Source Code (conceptual structure)

> 具体文件路径在实现阶段确定，以下为概念性组织结构

```text
# NeuG 现有代码结构 + 新增模块

compiler/
├── parser/              # 扩展: LOAD 语法解析
├── binder/              # 扩展: LOAD 绑定
└── planner/             # 扩展: LOAD 计划生成

execution/
└── operators/           # 新增: DataSource, BatchInsertTemp* 算子

storages/
└── temp/                # [NEW] 临时图存储模块
    ├── TempVertexTable  # 临时点表
    ├── TempEdgeTable    # 临时边表
    └── ConnectionGraph  # Connection 级图封装

main/
└── Connection           # 扩展: 集成临时图管理

utils/
└── readers/             # [NEW] 文件读取器模块
    ├── CSV Reader
    ├── JSON Reader
    ├── Parquet Reader
    └── Remote Readers   # P3: HTTP, S3, OSS
```

**Structure Decision**: 遵循 NeuG 现有的目录组织，新增代码放在对应模块的子目录中。临时存储作为独立模块存放。

## Module Implementation Plan

### P1: 本地文件 + 关系型查询 (可并行开发)

| Module | 核心组件 | 依赖 |
|--------|----------|------|
| **M1: CSV** | CSV Sniffer, CSV Reader, Predicate Pushdown | Arrow CSV |
| **M2: JSON** | JSON Sniffer, JSON Reader, Nested Field Handler | RapidJSON |
| **M3: Parquet** | Parquet Reader, Column Pruning | Arrow Parquet |
| **M4: 关系型查询** | Query Parser, Relational Operators, Expression Evaluator | 现有 Expression 框架 |

### P2: 临时图载入 (依赖 P1)

| Module | 核心组件 | 依赖 |
|--------|----------|------|
| **M5: 临时点** | TempVertexTable, Schema Builder, BatchInsertTempVertex | P1 Readers |
| **M6: 临时边** | TempEdgeTable, Bridging Edge Resolver, BatchInsertTempEdge | M5 |
| **M7: 生命周期** | ConnectionGraph, Schema Registry, Resource Cleaner | M5/M6 |
| **M8: 统一查询** | Unified Schema View, Transparent Scan/Expand | M5/M6 |
| **M9: 边界控制** | Mode Validator, Error Provider | - |

### P3: 网络资源支持 (可并行开发)

| Module | 核心组件 | 依赖 |
|--------|----------|------|
| **M10: HTTP** | HTTP Client, Stream Adapter, URL Parser | cpp-httplib |
| **M11: S3** | S3 Client, Credential Manager | AWS SDK |
| **M12: OSS** | OSS Client, Credential Manager | aliyun-oss-cpp-sdk |

## Dependencies & Execution Order

```
Phase 0: 基础设施准备
    ├── 创建目录结构
    ├── 配置 CMake
    └── 定义公共接口 (EntrySchema, Reader Interface)

Phase 1: P1 模块并行开发
    ├── M1: CSV Reader      ─┐
    ├── M2: JSON Reader     ─┼─→ 可并行
    ├── M3: Parquet Reader  ─┤
    └── M4: 关系型查询      ─┘
    
Phase 2: P2 模块开发
    ├── M5: BatchInsertTempVertex  ─→ 先完成
    ├── M6: BatchInsertTempEdge    ─→ 依赖 M5
    ├── M7: 生命周期管理           ─┐
    ├── M8: 统一查询接口           ─┼─→ 可与 M5/M6 部分并行
    └── M9: 场景边界控制           ─┘

Phase 3: P3 模块并行开发
    ├── M10: HTTP Reader   ─┐
    ├── M11: S3 Reader     ─┼─→ 可并行
    └── M12: OSS Reader    ─┘

Phase 4: 集成测试与文档
    ├── E2E 测试
    ├── Python 绑定
    └── 文档更新
```

## Risk Assessment

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| Arrow 版本兼容性 | 中 | 使用项目已有的 Arrow 构建配置 |
| 临时图内存管理 | 高 | 添加内存使用监控和限制机制 |
| 桥接边 ID 解析性能 | 中 | 使用批量查询和缓存优化 |
| 多 Connection 并发 | 中 | 使用 Connection-local 存储确保隔离 |

## Success Metrics

- [ ] P1 完成后：LOAD...RETURN 查询可正常执行
- [ ] P2 完成后：LOAD AS 可创建临时点边，MATCH 可查询
- [ ] P3 完成后：支持 HTTP/S3/OSS 远程数据源
- [ ] 所有模块单元测试覆盖率 ≥ 80%
- [ ] E2E 测试全部通过

## Key Design Decisions

1. **临时数据存储位置**: Connection-local 内存存储，与基图隔离
2. **label_id 编码**: 临时点边从 `max-1` 开始倒序编码，避免与持久图冲突
3. **桥接边处理**: 写入时根据关联列查询持久图点 ID
4. **生命周期**: 严格绑定 Connection，关闭时自动清理
5. **场景限制**: 仅在 AP 模式 + read-write 下支持 LOAD AS
