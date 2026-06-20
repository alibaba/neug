# GDS Shortest Path Return — Product & Technical Specification

> **Status**: Draft  
> **Authors**: Product + Engineering  
> **Date**: 2026-06-19  
> **Scope**: `bfs` / `sssp` algorithms in GDS extension

---

## 1. Background & Problem Statement

NeuG's GDS extension currently provides BFS and SSSP algorithms that return **(node, distance)** pairs — the shortest distance from a source vertex to every other vertex in the projected graph. However, **the actual path (sequence of vertices and edges) is not returned**.

This is a critical gap for real-world use cases:

| Use Case | Need |
|----------|------|
| Fraud detection / money laundering tracing | Not just "how many hops" but "which accounts were involved" |
| Network routing | The actual route, not just the cost |
| Social network analysis | "How is A connected to B?" — the chain of relationships |
| Supply chain optimization | The specific chain of suppliers |
| Knowledge graph reasoning | The reasoning path between entities |

Without path return, users must run a second query (e.g., Cypher pattern matching `MATCH p = (a)-[*]->(b)`) to reconstruct the path, which is both **incorrect** (the matched path may not be the shortest) and **inefficient** (duplicates computation).

---

## 2. Goals & Non-Goals

### Goals

1. **Return the shortest path** as a first-class output of BFS/SSSP algorithms
2. **Backward compatible** — existing queries without path request must work unchanged with zero overhead
3. **Leverage existing infrastructure** — NeuG already has a mature `Path` type with `PathColumn` / `PathColumnBuilder`, full vertex + edge + weight support
4. **Ergonomic Cypher API** — natural, discoverable syntax
5. **Performance** — minimal overhead when path is not requested; acceptable overhead when it is

### Non-Goals (for this iteration)

- **Multiple shortest paths** — when multiple equal-cost paths exist, return one deterministic path (not all)
- **All-pairs shortest path** — out of scope
- **Top-K shortest paths** — separate feature (Yen's algorithm)
- **Path return for other algorithms** (PageRank, WCC, etc.) — not applicable

---

## 3. Current State Analysis

### 3.1 Current API

```cypher
-- BFS: returns hop count
CALL bfs('my_graph', {source: '0', directed: false})
YIELD node, distance
RETURN node.id, distance;

-- SSSP: returns weighted distance  
CALL sssp('my_graph', {source: '0', weight: 'cost', directed: true})
YIELD node, distance
RETURN node.id, distance;
```

**Output schema (BFS)**:

| Column | Type | Description |
|--------|------|-------------|
| `node` | `kVertex` | Each vertex in the projected graph |
| `distance` | `kInt64` | Hop count from source (-1 = unreachable) |

**Output schema (SSSP)**:

| Column | Type | Description |
|--------|------|-------------|
| `node` | `kVertex` | Each vertex in the projected graph |
| `distance` | `kDouble` | Weighted distance from source (-1.0 = unreachable) |

### 3.2 Internal Implementation Gaps

| Component | Current | Missing |
|-----------|---------|---------|
| BFS (parallel) | `uint32_t distances_[]` | No `predecessors_[]` array |
| SSSP (parallel) | `atomic<double> distances_[]` | No `predecessors_[]` array |
| BFS-Pred (sequential) | `uint32_t distances_[]` | No `predecessors_[]` array |
| SSSP-Pred (sequential/Dijkstra) | `double distances_[]` | No `predecessors_[]` array |
| Output columns | `{node, distance}` | No `path` column registered |
| Path infrastructure | ✅ `Path`, `PathColumn`, `PathColumnBuilder` exist | Not wired to GDS |

### 3.3 Existing Path Infrastructure

NeuG already has a mature `Path` type used by pattern matching (`MATCH p = (a)-[e*]->(b)`):

```cpp
struct Path {
  // Construct from vertex sequence + edge data
  Path(label_t label, label_t e_label, 
       const std::vector<vid_t>& vids,
       const std::vector<std::pair<Direction, const void*>>& edge_datas);
  
  int32_t length() const;                    // number of edges
  std::vector<VertexRecord> nodes() const;   // ordered vertex list
  std::vector<EdgeRecord> relationships() const; // ordered edge list
  double get_weight() const;                 // total path weight
  void set_weight(double weight);
};
```

`PathColumnBuilder` supports building a column of `Path` objects, and the Cypher `RETURN` clause already knows how to render `kPath` type.

---

## 4. Product Design

### 4.1 Cypher API — 两种方案对比

#### 方案 A: YIELD-Based（技术 agent 推荐）

直接在 `outputColumns` 中注册 `path` 列，用户通过 YIELD 控制是否返回路径：

```cypher
-- 距离 only（向后兼容，零开销）
CALL bfs('my_graph', {source: '0', directed: false})
YIELD node, distance
RETURN node.id, distance;

-- 显式请求路径
CALL bfs('my_graph', {source: '0', directed: false})
YIELD node, distance, path
RETURN node.id, distance, path;

-- 不写 YIELD → 默认只返回 node + distance（需修改 bindGDSFunction 逻辑）
CALL bfs('my_graph', {source: '0'}) RETURN *;
```

#### 方案 B: Config Option-Based（产品经理推荐）

通过 `return_path: true` 配置项开启路径返回：

```cypher
CALL bfs('my_graph', {
  source: '0', 
  directed: false, 
  return_path: true
})
YIELD node, distance, path
RETURN node.id, distance, path;
```

#### 对比分析

| 维度 | YIELD-Based | Config Option |
|------|------------|---------------|
| **Cypher 自然度** | ✅ 更符合 Cypher 范式（YIELD 控制输出） | ⚠️ 需要新配置项 |
| **向后兼容** | ⚠️ 需要特殊处理 no-YIELD 场景（默认不返回 path） | ✅ `return_path` 默认 false，完全兼容 |
| **显式性** | ⚠️ 路径计算是 YIELD 的隐式副作用 | ✅ 显式声明意图 |
| **实现复杂度** | ⚠️ 需修改 `bindGDSFunction` 处理默认列数 | ✅ 解析 option 即可 |
| **灵活性** | ✅ 用户可以 `YIELD node, path`（跳过 distance） | ⚠️ 列固定为 {node, distance, path} |
| **零开销保证** | ✅ 未 YIELD path 则不分配 predecessor 数组 | ✅ `return_path: false` 不分配 |

**最终推荐: 混合方案（Hybrid）**

同时支持两种方式，以 YIELD 为主控机制，`return_path` 选项为辅助：

```cypher
-- 方式1: 通过 YIELD 请求路径（推荐）
CALL bfs('g', {source: '0'}) YIELD node, distance, path RETURN path;

-- 方式2: 通过 option 开启 + YIELD
CALL bfs('g', {source: '0', return_path: true}) YIELD node, distance, path RETURN path;

-- 不写 YIELD + 无 return_path → 默认返回 {node, distance}（向后兼容）
CALL bfs('g', {source: '0'}) RETURN *;
-- → 输出: node, distance (无 path)
```

**实现逻辑**:
1. `outputColumns` 始终注册 3 列：`{node, distance, path}`
2. `bindGDSFunction` 中，当用户不写 YIELD 时，默认只绑定前 2 列（不含 path）
3. `bind()` 中通过 `meta_data_size()` 判断是否请求了 path
4. 只有当 path 出现在 YIELD 中时，才分配 `predecessors_[]` 数组
5. `return_path: true` 选项可作为冗余确认（不影响行为）

**Key design decisions**:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Opt-in mechanism | **Hybrid: YIELD-primary + option-secondary** | YIELD 更 Cypher-idiomatic；option 更显式 |
| Config option vs new function | **Config option** | Same algorithm, different output verbosity; avoids API explosion |
| YIELD column name | `path` | Intuitive, matches Cypher convention (`MATCH p = ...`) |
| No-YIELD default | **只返回 {node, distance}** | 向后兼容，需修改 bindGDSFunction |

### 4.2 Output Schema — Extended

When `return_path: false` (default), output is unchanged:

| Column | Type |
|--------|------|
| `node` | `kVertex` |
| `distance` | `kInt64` (BFS) / `kDouble` (SSSP) |

When `return_path: true`, an additional column is available for YIELD:

| Column | Type | Description |
|--------|------|-------------|
| `node` | `kVertex` | Target vertex |
| `distance` | `kInt64` / `kDouble` | Distance from source |
| `path` | `kPath` | Shortest path from source to this node |

**Path semantics**:
- `path` for the source node itself → single-node path (length 0)
- `path` for unreachable nodes → `null` (the column becomes optional)
- `path` for reachable nodes → ordered vertex + edge sequence from source to target
- When multiple shortest paths exist → one deterministic path is returned (first-discovered by the algorithm)

### 4.3 Path Object Contents

The returned `Path` object supports all standard NeuG path accessors in Cypher:

```cypher
CALL bfs('my_graph', {source: '0', return_path: true})
YIELD node, distance, path
WHERE distance > 0
RETURN 
  node.id AS target,
  distance AS hops,
  nodes(path) AS path_nodes,      -- [v0, v3, v7, v12]
  relationships(path) AS path_edges, -- [(v0)-[e1]->(v3), ...]
  length(path) AS path_length;     -- number of edges
```

### 4.4 Single-Target Optimization

A common use case is finding the path to a **specific target**, not all vertices. We propose adding a `target` option:

```cypher
-- Find shortest path from source to a specific target
CALL bfs('my_graph', {
  source: '0', 
  target: '42',
  directed: false, 
  return_path: true
})
YIELD node, distance, path
RETURN node.id, distance, path;
```

**Behavior with `target`**:
- Returns exactly **one row** (the target node with its distance and path)
- Returns **empty result** if the target is unreachable
- Algorithm can **early-terminate** when the target is reached (significant perf win for BFS)
- For SSSP, early termination is possible with Dijkstra (when target is settled) but not with Bellman-Ford

### 4.5 Predicate Variant Consistency

Path return works identically for predicate-filtered graphs:

```cypher
CALL project_graph('filtered', 
  {'person': 'n.age > 18'}, 
  {'[person, knows, person]': 'r.since > 2020'}
);

CALL bfs('filtered', {source: '0', return_path: true})
YIELD node, distance, path
RETURN path;
```

---

## 5. User Experience Walkthrough

### Scenario: Fraud Investigation

An analyst investigating money laundering wants to trace the shortest chain of transactions from a suspect account to a known offshore account.

```cypher
-- Step 1: Load GDS
LOAD gds;

-- Step 2: Project the transaction graph (only recent, high-value transfers)
CALL project_graph('tx_graph',
  {'account': 'n.risk_score > 0.5'},
  {'[account, transfer, account]': 'r.amount > 10000 AND r.date > Date("2025-01-01")'}
);

-- Step 3: Find shortest path
CALL sssp('tx_graph', {
  source: 'ACC-SUSPECT-001',
  target: 'ACC-OFFSHORE-999',
  weight: 'amount',
  directed: true,
  return_path: true
})
YIELD node, distance, path
RETURN 
  nodes(path) AS money_flow,
  relationships(path) AS transfers,
  distance AS total_amount;

-- Step 4: Cleanup
CALL drop_projected_graph('tx_graph');
```

### Scenario: Social Network "How do you know X?"

```cypher
CALL project_graph('social', ['person'], {'[person, knows, person]': ''});

CALL bfs('social', {
  source: 'alice',
  target: 'bob',
  directed: false,
  return_path: true
})
YIELD node, distance, path
RETURN 
  [n IN nodes(path) | n.name] AS connection_chain,
  distance AS degrees_of_separation;
```

---

## 6. Technical Design

### 6.1 Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                  Cypher Parser                   │
│  CALL bfs('g', {source:'0', return_path: true}) │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│              Binder (bindGDSFunction)            │
│  Parse return_path option → output_columns:      │
│  {node, distance} or {node, distance, path}      │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│           Physical Plan (GDSAlgo proto)          │
│  options map includes "return_path" → "true"     │
│  meta_data includes 2 or 3 output columns        │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│          GDSAlgoOpr::Eval()                      │
│  → BFS/SSSP::compute() with predecessor tracking │
│  → sink() builds PathColumn if return_path       │
└─────────────────────────────────────────────────┘
```

### 6.2 Core Algorithm Changes

#### Predecessor Array

Add a `predecessors_[]` array (type `vid_t[]`, size = vertex_count) to all four algorithm variants:

| Variant | Predecessor Type | Synchronization |
|---------|-----------------|-----------------|
| BFS (parallel) | `std::atomic<vid_t>[]` | CAS on first discovery (same CAS that sets distance) |
| SSSP (parallel) | `std::atomic<vid_t>[]` | CAS coupled with distance relaxation |
| BFS-Pred (sequential) | `vid_t[]` | No sync needed (single-threaded) |
| SSSP-Pred (sequential) | `vid_t[]` | No sync needed (single-threaded) |

**Memory overhead**: `O(V)` — one `vid_t` (typically `uint32_t` = 4 bytes) per vertex. For a 10M vertex graph: ~40MB. Acceptable.

#### Parallel BFS Predecessor Update

```cpp
// In the relax lambda, when CAS succeeds for distance:
auto relax = [&](vid_t dst, vid_t parent) {
  uint32_t expected = std::numeric_limits<uint32_t>::max();
  if (__atomic_compare_exchange_n(&distances_[dst], &expected,
                                   level, false, __ATOMIC_RELAXED,
                                   __ATOMIC_RELAXED)) {
    predecessors_[dst].store(parent, std::memory_order_relaxed);
    local_next[tid].push_back(dst);
  }
};
```

**Correctness**: The CAS ensures exactly one thread "wins" for each vertex at each level. The winner writes both `distances_` and `predecessors_`. No race condition.

**Note on dense pull mode**: In pull mode, the discovering parent must be recorded:

```cpp
// Pull mode: when a frontier neighbor is found
for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
  if (distances_[*it] == level - 1) {
    predecessors_[dst].store(*it, std::memory_order_relaxed);
    reachable = true;
    break;
  }
}
```

#### Parallel SSSP Predecessor Update

For SSSP, the predecessor must be updated atomically with the distance. We modify `relax_distance` to also set the predecessor:

```cpp
inline bool relax_distance_with_pred(std::atomic<double>* dist_ptr,
                                      std::atomic<vid_t>* pred_ptr,
                                      double candidate, vid_t parent) {
  double old = dist_ptr->load(std::memory_order_relaxed);
  while (candidate < old || old < 0) {
    if (dist_ptr->compare_exchange_weak(old, candidate, 
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
      pred_ptr->store(parent, std::memory_order_relaxed);
      return true;
    }
  }
  return false;
}
```

**Race condition note**: In SSSP, a vertex's distance may be relaxed multiple times. Each successful CAS writes a new predecessor. The final predecessor corresponds to the final (shortest) distance. This is correct because:
- The last successful CAS sets the true shortest distance
- The predecessor written in that CAS is the vertex that provided that shortest distance
- Any intermediate (wrong) predecessors are overwritten

#### Sequential Dijkstra (SSSPPred) Predecessor Update

Trivial — single-threaded, no atomic needed:

```cpp
// In SSSPPred::compute(), inside the relax lambda:
if (distances_[w] < 0 || cand < distances_[w]) {
  distances_[w] = cand;
  predecessors_[w] = u;  // plain write, no sync needed
  pq.push({cand, w});
}
```

This also naturally supports **early termination** when `target` is specified: when the target vertex is popped from the priority queue (settled), its shortest distance and predecessor chain are finalized.

### 6.3 Path Reconstruction

Path reconstruction happens in `sink()`, after `compute()` finishes.

#### 6.3.1 Basic Reconstruction (vertex-only path)

Walk backward from target to source using the predecessor array, then reverse:

```cpp
Path reconstruct_path(vid_t target, vid_t source, 
                      const PredecessorAccessor& pred,
                      label_t vertex_label, label_t edge_label,
                      const StorageReadInterface& graph) {
  if (target == source) {
    return Path(vertex_label, source);  // single-node path
  }
  
  std::vector<vid_t> reversed_vids;
  vid_t cur = target;
  while (cur != source) {
    reversed_vids.push_back(cur);
    cur = pred.get(cur);
  }
  reversed_vids.push_back(source);
  std::reverse(reversed_vids.begin(), reversed_vids.end());
  
  // Build Path with edge data lookup
  return build_path_with_edges(reversed_vids, vertex_label, edge_label, graph);
}
```

#### 6.3.2 Edge Data Lookup (关键设计决策)

**问题**: `predecessors_[]` 只存储前驱顶点 vid，不包含边的属性数据（如权重）。而 `Path` 对象的构造函数需要 `edge_datas` 参数。

**方案对比**:

| 方案 | 优点 | 缺点 |
|------|------|------|
| A: sink 阶段重新查找边 | 算法期间零额外内存；简单 | 每条路径边需要一次 CSR 查找 O(degree) |
| B: 存储 predecessor + edge_index | sink 阶段零查找 | 额外 `sizeof(edge_idx_t) * V` 内存 |

**推荐方案 A — sink 阶段重新查找**：

理由：
1. 路径重建发生在 `sink()`，是单次操作，不在热路径上
2. 对于每条边 `(pred[v], v)`，在 CSR 中查找是 O(degree) 操作
3. 总复杂度 O(V × avg_degree × avg_path_length)，对于实际场景可接受
4. 避免在计算期间存储额外 edge index 数组（对大图节省显著）

```cpp
Path build_path_with_edges(const std::vector<vid_t>& vids,
                           label_t vertex_label, label_t edge_label,
                           const StorageReadInterface& graph) {
  auto oe_view = graph.GetGenericOutgoingGraphView(
      vertex_label, vertex_label, edge_label);
  
  std::vector<std::pair<Direction, const void*>> edge_datas;
  edge_datas.reserve(vids.size() - 1);
  
  for (size_t i = 0; i + 1 < vids.size(); ++i) {
    vid_t from = vids[i];
    vid_t to = vids[i + 1];
    // Look up edge from → to in CSR
    auto edges = oe_view.get_edges(from);
    const void* prop = nullptr;
    Direction dir = Direction::kOutgoing;
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (*it == to) {
        prop = it.get_data_ptr();
        break;
      }
    }
    edge_datas.push_back({dir, prop});
  }
  
  return Path(vertex_label, edge_label, vids, edge_datas);
}
```

**注意**: 对于无向图，如果 outgoing 查找失败，还需要尝试 incoming 边。

#### 6.3.3 PredecessorAccessor — 统一接口

用模板统一 `atomic<vid_t>` 和 plain `vid_t` 的访问：

```cpp
struct AtomicPredecessorAccessor {
  const std::atomic<vid_t>* data;
  vid_t get(vid_t v) const { return data[v].load(std::memory_order_relaxed); }
};

struct PlainPredecessorAccessor {
  const vid_t* data;
  vid_t get(vid_t v) const { return data[v]; }
};
```

#### 6.3.4 无向图的边方向追踪（技术 agent 发现）

**问题**: 当 `directed_ == false` 时，BFS/SSSP 可能通过 incoming 边发现顶点（即反向遍历了一条 `(w → u)` 边来从 `u` 到达 `w`）。在路径重建时，需要正确标记这条边的方向。

**方案对比**:

| 方案 | 内存 | 复杂度 |
|------|------|--------|
| A: 存储 `uint8_t pred_dir_[]` 数组 | +1 byte/vertex | 简单，reconstruction 时直接读取 |
| B: reconstruction 时查 CSR 判断 | 0 | 需要双向查找，稍复杂 |
| C: 无向图统一标记为 kOutgoing | 0 | 最简单，但语义不精确 |

**推荐方案 A** — 添加 `pred_dir_[]` 方向数组：

```cpp
// 在 BFS compute() 中：
// outgoing edge: u -> dst (direction = kOutgoing)
predecessors_[dst].store(u, std::memory_order_relaxed);
pred_dir_[dst] = 0;  // kOutgoing

// incoming edge: w -> u, traversed as u -> w (direction = kIncoming)  
predecessors_[w].store(u, std::memory_order_relaxed);
pred_dir_[w] = 1;  // kIncoming (反向遍历)
```

**内存影响**: +1 byte/vertex，对 10M 顶点图约 +10MB，可接受。

**注意**: 对于 `directed_ == true` 的场景，不需要此数组（所有边都是 kOutgoing）。

### 6.4 Output Column Registration

Modify `getFunctionSet()` to include the `path` column:

```cpp
function::call_output_columns outputColumns = {
    {"node", common::DataTypeId::kVertex},
    {"distance", common::DataTypeId::kInt64},
    {"path", common::DataTypeId::kPath}      // NEW
};
```

The `path` column is always registered in the output schema, but:
- When `return_path: false` → only `{node, distance}` are bound; `path` column is never materialized
- When `return_path: true` → `{node, distance, path}` are all bound and materialized

### 6.5 Binder Changes (bindGDSFunction)

**核心挑战**: 当用户不写 YIELD 时，`bindGDSFunction` 当前会把 `outputColumns` 中所有列都加入 scope。如果我们注册了 3 列（node, distance, path），no-YIELD 查询会默认输出 3 列，**破坏向后兼容**。

**解决方案**: 引入 `defaultOutputColumns` 概念——当 no-YIELD 时，只绑定前 N 列。

具体修改 `bindGDSFunction`（`src/compiler/function/gds/gds_algo_function.cpp`）：

```cpp
// 在 no-YIELD 分支中：
} else {
    // Only bind the first 2 columns (node, distance) by default
    // path is "optional" — only available when explicitly YIELDed
    int default_col_count = 2;  // node + distance
    for (int i = 0; i < default_col_count && i < outputColumns.size(); ++i) {
        auto& outputColumn = outputColumns[i];
        // ... existing binding logic ...
    }
}
```

**path 列检测**: 在 `bind()` 中通过 meta_data 判断 path 是否被请求：

```cpp
// In BFSFunction::bind():
input->return_path = false;
input->path_alias = -1;

for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
    const auto& meta = plan.plan(op_idx).meta_data(i);
    // meta_data(0) = node, meta_data(1) = distance
    // If there's a 3rd entry, it's path
    if (i >= 2) {
        input->return_path = true;
        input->path_alias = meta.alias();
    }
}
```

**注意**: 这依赖于 `bindGDSFunction` 按 outputColumns 声明顺序填充 meta_data。当前实现确实如此（按 outputColumns 的顺序 push_back columns）。

### 6.6 Sink Changes

In `BFS::sink()` / `SSSP::sink()`, conditionally build the path column:

```cpp
void BFS::sink(execution::Context& ctx, int node_alias, 
               int distance_alias, int path_alias) {
  // ... existing node + distance building ...
  
  if (return_path_) {
    execution::PathColumnBuilder path_builder(/*is_optional=*/true);
    for (vid_t v : vertices_) {
      if (distances_[v] == std::numeric_limits<uint32_t>::max()) {
        path_builder.push_back_null();  // unreachable → null path
      } else {
        path_builder.push_back_opt(
            reconstruct_path(v, source_, predecessors_, ...));
      }
    }
    chunk.set(path_alias, path_builder.finish());
  }
  
  ctx.append_chunk(std::move(chunk));
}
```

### 6.7 Early Termination (target option)

When a `target` option is provided:

**BFS**: Stop as soon as the target vertex is added to the frontier (its distance is set). In the parallel BFS, check after each level if `distances_[target]` is no longer `max`.

**SSSP (Dijkstra variant, i.e., SSSPPred)**: Stop when the target is extracted from the priority queue (settled).

**SSSP (Bellman-Ford parallel)**: Cannot early-terminate safely in general; check after each round if the target's distance has stabilized (no relaxation in the last round). This is a heuristic optimization.

### 6.8 Predicate Variant Changes

The predicate variants (`BFSPred`, `SSSPPred`) are sequential, so predecessor tracking is simpler — just a plain `vid_t[]` array with no atomic operations.

---

## 7. Edge Cases & Error Handling

| Scenario | Behavior |
|----------|----------|
| `return_path` not specified | Default `false`, no path column, zero overhead |
| `return_path: true` but `path` not in YIELD | `path` column computed but not returned (user choice) |
| `path` in YIELD but `return_path` not `true` | Error: "Column 'path' requires option return_path: true" |
| Source vertex doesn't exist | Empty result (unchanged from current behavior) |
| Target vertex doesn't exist | Empty result |
| Target == Source | Single-row result: distance=0, path=[source] |
| Target unreachable | distance=-1, path=null |
| Disconnected graph | Unreachable vertices get distance=-1, path=null |
| Self-loop on path | Correctly handled by predecessor chain (won't occur in shortest path) |
| Multiple equal-cost paths | One deterministic path returned (implementation-defined: first discovered) |
| Very long paths (e.g., 1M hops) | Path reconstruction is O(path_length), acceptable |
| Large graph (100M vertices) | Predecessor array = 400MB; document as memory requirement |

---

## 8. Performance Analysis

### 8.1 Overhead When `return_path: false`

**Zero overhead** — no predecessor array allocated, no extra computation.

### 8.2 Overhead When `return_path: true`

| Cost Component | BFS | SSSP |
|---------------|-----|------|
| Predecessor array allocation | O(V), ~4B per vertex | O(V), ~4B per vertex |
| Predecessor writes during compute | 1 atomic store per vertex (amortized) | 1 atomic store per successful relaxation |
| Path reconstruction in sink | O(V × avg_path_length) | O(V × avg_path_length) |
| PathColumn building | O(V) Path objects | O(V) Path objects |

**Expected impact on typical workloads**:
- BFS: ~10-20% slowdown (predecessor CAS is piggy-backed on distance CAS)
- SSSP: ~15-30% slowdown (extra atomic store per relaxation, but relaxation count is bounded)
- Path reconstruction: O(V × L) where L is average path length; dominates sink time for large graphs

### 8.3 Memory

| Graph Size | Predecessor Array | Path Objects (worst case) |
|------------|-------------------|--------------------------|
| 1M vertices | 4 MB | ~O(V × L) |
| 10M vertices | 40 MB | ~O(V × L) |
| 100M vertices | 400 MB | ~O(V × L) |

---

## 9. Compatibility

### 9.1 Backward Compatibility

| Aspect | Impact |
|--------|--------|
| Existing Cypher queries | ✅ No change — `return_path` defaults to `false` |
| Existing output schema | ✅ Unchanged when `return_path` not set |
| Existing tests | ✅ All pass without modification |
| Python binding | ✅ `path` column accessible via existing Path type support |

### 9.2 Graphalytics Compliance

The LDBC Graphalytics benchmark validates BFS/SSSP **distance** outputs only. Path return does not affect these benchmarks.

---

## 10. Testing Strategy

### 10.1 Unit Tests

| Test | Description |
|------|-------------|
| `test_bfs_with_path_basic` | Simple graph, verify path vertex sequence |
| `test_sssp_with_path_basic` | Weighted graph, verify path and total cost |
| `test_bfs_path_unreachable` | Disconnected graph, unreachable nodes → null path |
| `test_sssp_path_unreachable` | Same for SSSP |
| `test_bfs_path_source_equals_target` | Self-path, length 0 |
| `test_bfs_path_with_target` | Single-target, early termination, one row |
| `test_sssp_path_with_target` | Single-target SSSP |
| `test_bfs_path_with_predicates` | Predicate-filtered graph, verify path respects filters |
| `test_sssp_path_with_predicates` | Same for SSSP |
| `test_bfs_path_directed_vs_undirected` | Verify directed paths differ from undirected |
| `test_path_backward_compat` | Existing queries without return_path still work |
| `test_path_yield_without_option` | Error when YIELD path without return_path: true |
| `test_path_multiple_equal_paths` | Deterministic behavior with multiple shortest paths |

### 10.2 Integration Tests

| Test | Description |
|------|-------------|
| `test_graphalytics_bfs_with_path` | Run Graphalytics datasets with path return, verify distance still correct |
| `test_graphalytics_sssp_with_path` | Same for SSSP |
| `test_large_graph_path` | 100K+ vertices, verify memory and correctness |

### 10.3 Python Binding Tests

```python
def test_bfs_returns_path():
    result = conn.execute(
        "CALL bfs('g', {source: '0', return_path: true}) "
        "YIELD node, distance, path "
        "RETURN node.id, distance, path"
    )
    rows = result.fetch_all()
    for row in rows:
        if row['distance'] > 0:
            path = row['path']
            assert path is not None
            assert len(path.nodes) == row['distance'] + 1
            assert path.nodes[0].id == '0'  # starts at source
            assert path.nodes[-1].id == row['node_id']  # ends at target
```

---

## 11. API Reference

### 11.1 BFS Options (Extended)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | (required) | Source vertex primary key |
| `directed` | bool | `false` | Whether to follow edge direction |
| `concurrency` | int | hardware_concurrency | Thread count |
| `return_path` | bool | `false` | **NEW**: Return shortest path |
| `target` | string | (none) | **NEW**: Stop at specific target vertex |

### 11.2 SSSP Options (Extended)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | (required) | Source vertex primary key |
| `directed` | bool | `false` | Whether to follow edge direction |
| `weight` | string | `""` (unit weight) | Edge property for weights |
| `concurrency` | int | hardware_concurrency | Thread count |
| `return_path` | bool | `false` | **NEW**: Return shortest path |
| `target` | string | (none) | **NEW**: Stop at specific target vertex |

### 11.3 Path Encoding Modes

When `return_path` is enabled, the `path_properties` option controls how much information is encoded in the path output:

#### Lightweight Mode (default: `path_properties: "lightweight"`)

Optimized for performance. Only encodes essential structural information:

**Vertices in path:**
- `_ID`: Unique vertex identifier
- `_LABEL`: Vertex label name
- Primary key property

**Edges in path:**
- `_ID`: Unique edge identifier
- `_LABEL`: Edge label name
- `_SRC_ID`: Source vertex ID
- `_DST_ID`: Destination vertex ID

**Performance:** ~0.5s on LDBC SNB SF10 (65K nodes, 1.9M edges, 62K paths)

**Use case:** When you only need the path structure (which nodes/edges are involved) and can retrieve full properties separately if needed.

#### Full Mode (`path_properties: "full"`)

Encodes all vertex and edge properties:

**Vertices in path:**
- All fields from lightweight mode
- All non-PK properties with their values

**Edges in path:**
- All fields from lightweight mode
- All edge properties with their values

**Performance:** ~1.3s on LDBC SNB SF10 (2.6x slower than lightweight)

**Use case:** When you need complete property information in a single query result.

**Example:**

```cypher
-- Lightweight mode (default)
CALL bfs('graph', {source: '0', path_properties: 'lightweight'})
YIELD path
RETURN path;

-- Full mode
CALL bfs('graph', {source: '0', path_properties: 'full'})
YIELD path
RETURN path;
```

### 11.4 Output Columns

When `return_path: false` (default):

| Column | Type |
|--------|------|
| `node` | `kVertex` |
| `distance` | `kInt64` (BFS) / `kDouble` (SSSP) |

When `return_path: true`:

| Column | Type |
|--------|------|
| `node` | `kVertex` |
| `distance` | `kInt64` (BFS) / `kDouble` (SSSP) |
| `path` | `kPath` |

When `target` is specified: output is at most **one row** (the target vertex).

---

## 12. Implementation Plan

### Phase 1: Core Path Return (MVP)

| Step | File | Change |
|------|------|--------|
| 1.1 | `extension/gds/include/impl/bfs_impl.h` | 添加 `predecessors_` 和 `pred_dir_` 成员；`sink()` 接受 `path_alias` |
| 1.2 | `extension/gds/src/impl/bfs_impl.cc` | CAS 成功分支中写 predecessor + direction；sink 中重建路径 |
| 1.3 | `extension/gds/include/impl/sssp_impl.h` | 同上，`predecessors_` 为 `vid_t[]` |
| 1.4 | `extension/gds/src/impl/sssp_impl.cc` | `relax_distance` 成功后写 predecessor；sink 中重建路径 + set_weight |
| 1.5 | `extension/gds/include/impl/bfs_pred_impl.h` + `.cc` | 顺序版本添加 predecessor 追踪 |
| 1.6 | `extension/gds/include/impl/sssp_pred_impl.h` + `.cc` | Dijkstra 中添加 predecessor 追踪 |
| 1.7 | `extension/gds/src/bfs.cc` | `outputColumns` 添加 `{path, kPath}`；`bind()` 检测 path_alias |
| 1.8 | `extension/gds/src/sssp.cc` | 同上 |
| 1.9 | `extension/gds/include/utils/path_utils.h` (新文件) | `reconstruct_path()` + `build_path_with_edges()` 工具函数 |
| 1.10 | `src/compiler/function/gds/gds_algo_function.cpp` | no-YIELD 时只绑定前 2 列 |
| 1.11 | `tools/python_bind/tests/test_gds.py` | 添加路径返回相关测试 |

### Phase 2: Single-Target Optimization

1. Add `target` option parsing in `bind()`
2. Implement early termination in BFS (check after each level)
3. Implement early termination in SSSPPred (Dijkstra — check on PQ pop)
4. Add target-specific tests

### Phase 3: Documentation & Polish

1. Update GDS documentation and examples
2. Add Jupyter notebook examples
3. Performance benchmarks (especially large graph path reconstruction)

---

## 13. Open Questions

| # | Question | Recommendation |
|---|----------|----------------|
| 1 | Should we return ALL shortest paths when multiple exist? | No — return one. All-paths is exponential in worst case. |
| 2 | Should path include edge properties? | Yes — `Path` already supports this via `EdgeRecord.prop`. |
| 3 | What about `kHop` return for BFS (path as hop count only)? | Already covered by `distance` column. |
| 4 | Should `target` be a separate algorithm (e.g., `shortest_path`)? | No — keep as option for simplicity. Could add alias later. |
| 5 | How to handle `return_path` with `concurrency > 1` in SSSP? | The CAS-based predecessor update is correct (see §6.2). |

---

## 14. Output Mode Discussion (from Technical Analysis)

### Stream Mode vs Aggregate Mode

The current BFS/SSSP output is "stream mode" — one row per vertex. There is an alternative "aggregate mode" (Neo4j GDS style) that returns one row per target with accumulated node/cost arrays.

| Aspect | Stream Mode (推荐) | Aggregate Mode (Neo4j) |
|--------|-------------------|----------------------|
| Output | 1 row per vertex: (node, distance, path) | 1 row per target: (index, nodeIds[], costs[], path) |
| Composability | ✅ 可与 WHERE/FILTER/ORDER BY 组合 | ❌ 数组不易于 Cypher 过滤 |
| 与当前 API 一致 | ✅ 完全一致 | ❌ 引入新输出范式 |
| 大数据集友好 | ✅ 可逐行处理 | ❌ 需要一次性物化所有数组 |
| `target` 单目标 | 返回 1 行 | 返回 1 行（无差别） |

**结论**: 保持 Stream Mode（当前设计），因为它与 NeuG 现有架构一致，且 Cypher 的 WHERE/RETURN 组合更灵活。Aggregate Mode 可作为未来增强（`RETURN AS ARRAY` 选项）。

### SSSP Path 中的 Weight 语义

对于 SSSP，返回的 `Path` 对象应包含正确的总权重：
- `path.get_weight()` 返回路径总权重（所有边权重之和）
- 这与 `distance` 列的值一致
- 在 `build_path_with_edges()` 中，需要调用 `path.set_weight(distance)` 来设置

```cpp
Path path = build_path_with_edges(vids, vertex_label, edge_label, graph);
if (is_sssp) {
  path.set_weight(distances_[v]);  // 确保 path.get_weight() == distance
}
```

### Lazy Path Construction (大图优化)

对于非常大的图（>10M 顶点），在 sink 中为所有顶点构建 Path 对象可能导致内存峰值。优化方案：

1. **仅在 `path` 列被 YIELD 时才构建**：如果用户 `YIELD node, distance` 而 `return_path: true`，跳过路径构建
2. **target 模式天然 lazy**：只构建一条路径
3. **Future**: 考虑 streaming sink — 逐批构建和输出路径，而非一次性构建所有

当前 Phase 1 不需要 lazy construction — 先实现简单版本，通过 benchmark 验证是否需要优化。

---

## 15. Alternatives Considered

### A. Separate Algorithm Names (`bfs_path`, `sssp_path`)

```cypher
CALL bfs_path('g', {source: '0'}) YIELD node, distance, path
```

**Rejected**: API explosion; each algorithm × option = new function name. Hard to maintain and discover.

### B. Always Return Path

```cypher
CALL bfs('g', {source: '0'}) YIELD node, distance, path  -- path always available
```

**Rejected**: Memory and performance overhead for users who only need distance. Breaks backward compatibility of output schema.

### C. Post-Processing Function

```cypher
CALL bfs('g', {source: '0'}) YIELD node, distance
WITH node, distance
CALL get_path('g', node, '0') YIELD path  -- separate lookup
RETURN node, distance, path;
```

**Rejected**: Requires O(V) additional graph traversals. Incorrect — `get_path` may find a different path than BFS did.

### D. List of Vertex IDs Instead of Path Object

```cypher
YIELD node, distance, path_node_ids  -- List<Int64>
```

**Rejected**: Loses edge information. NeuG already has `kPath` type with full support — use it.

### E. Pure YIELD-Based (No Option)

```cypher
-- Path implicitly computed when YIELDed
CALL bfs('g', {source: '0'}) YIELD node, distance, path RETURN path;
```

**Partially adopted**: YIELD 作为主控机制，但保留 `return_path` 选项作为显式声明的辅助手段。纯 YIELD 方案的问题是 no-YIELD 场景的向后兼容需要特殊处理（已在 §6.5 中解决）。

---

## Appendix A: Parallel Correctness Deep Dive (from Technical Analysis)

### BFS 并行正确性

**Sparse Push 阶段**:
- 多个线程可能对同一目标 `dst` 发起 CAS
- CAS 保证只有一个线程成功设置 `distances_[dst] = level`
- 成功线程随后写入 `predecessors_[dst] = src`
- **关键**: 因为 CAS 保证排他性，predecessor 写入可以用 plain store（不需要额外原子操作）
- 每个 level 中每个 vertex 只被 CAS 一次，所以 predecessor 是唯一确定的

**Dense Pull 阶段**:
- `parallel_for` 遍历所有未访问的顶点（每个顶点只被一个线程处理）
- 线程扫描该顶点的邻居，找到 frontier 中的一个
- **注意**: 当前代码只检查 `distances_[*it] == level - 1` 然后 `break`
- 必须在此处记录 `predecessors_[dst] = *it`（哪个邻居触发了发现）
- 因为每个目标顶点只被一个线程处理（parallel_for 的分块保证），无需原子操作

**结论**: BFS 的 predecessor 写入在两个阶段都是安全的。

### SSSP 并行正确性

**多次 Relaxation 问题**:
- 与 BFS 不同，SSSP 中一个顶点可能被多次 relaxation（找到更短路径）
- 每次成功的 CAS 都会更新 `distances_[dst]` 和 `predecessors_[dst]`
- 最终 `predecessors_[dst]` 对应于最终最短距离的前驱

**瞬态不一致窗口**:
- 线程 T1 CAS 成功写入 `distances_[dst] = 5.0`
- T1 还没来得及写 `predecessors_[dst]`
- 线程 T2 读取 `distances_[dst] = 5.0`，基于此做进一步 relaxation
- T1 随后写入 `predecessors_[dst] = A`

这种瞬态不一致 **不影响最终正确性**:
1. T2 基于 `distances_[dst] = 5.0` 做的 relaxation 使用的是正确的距离值
2. 如果后来发现更短路径（CAS by T3），`distances_[dst]` 和 `predecessors_[dst]` 都会被覆盖
3. Bellman-Ford 保证收敛，最终 predecessor chain 指向正确的最短路径

**CAS-Predecessor 原子性方案**:
```cpp
// 将 predecessor 写入放在 CAS 成功分支中
if (relax_distance(&distances_[dst], cand)) {
    predecessors_[dst] = src;  // CAS 成功后立即写入
    local_next[tid].push_back(dst);
}
```

这缩小了不一致窗口，但不能完全消除（另一个线程可能在 `relax_distance` 返回 true 和 `predecessors_[dst] = src` 之间发起新的 CAS）。对于最终一致性来说这不是问题。

### Dijkstra (SSSPPred) 正确性

- 单线程，无并发问题
- 当顶点从 priority queue 中 pop 出来时，其最短距离和前驱都已确定
- `predecessors_[w] = u` 在 relaxation 时写入，与 `distances_[w]` 同步更新
- 支持 early termination：当 target 被 pop 时即可终止
