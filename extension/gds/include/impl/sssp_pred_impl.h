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
#include <memory>
#include <string>

#include "neug/execution/common/columns/container_types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

// Single-source shortest paths on a subgraph constrained by a vertex and/or
// edge predicate. Vertices excluded by the vertex predicate are dropped from
// the result and cannot be traversed; only edges satisfying the edge predicate
// are followed. Use the plain SSSP when no predicate is supplied.
class SSSPPred {
 public:
  SSSPPred(const StorageReadInterface& graph, label_t vertex_label,
           label_t edge_label, vid_t source, bool directed,
           const std::string& edge_weight_prop, int concurrency,
           execution::ExprBase* vertex_pred, execution::ExprBase* edge_pred);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int distance_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  vid_t source_;
  bool directed_;
  std::string edge_weight_prop_;
  int concurrency_;
  execution::ExprBase* vertex_pred_;
  execution::ExprBase* edge_pred_;

  std::unique_ptr<double[]> distances_;
  vector_t<vid_t> vertices_;
};

}  // namespace gds
}  // namespace neug
