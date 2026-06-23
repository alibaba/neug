/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "neug/execution/common/columns/container_types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

// K-core decomposition on a subgraph constrained by a vertex and/or edge
// predicate. Vertices excluded by the vertex predicate are dropped from the
// result and ignored when computing degrees; only edges satisfying the edge
// predicate count. Use the plain KCore when no predicate is supplied.
// Performance is not a concern on the predicate path, so this is a simple
// sequential peeling.
class KCorePred {
 public:
  KCorePred(const StorageReadInterface& graph, label_t vertex_label,
            label_t edge_label, int32_t k, int32_t concurrency,
            execution::ExprBase* vertex_pred, execution::ExprBase* edge_pred);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int core_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  int32_t k_;
  int32_t concurrency_;
  execution::ExprBase* vertex_pred_;
  execution::ExprBase* edge_pred_;

  vector_t<vid_t> vertices_;
  std::vector<int32_t> degree_;
  std::vector<char> removed_;
};

}  // namespace gds
}  // namespace neug
