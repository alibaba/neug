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

#include "neug/execution/common/columns/container_types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

// PageRank on a subgraph constrained by a vertex and/or edge predicate.
// Vertices excluded by the vertex predicate are dropped from the result and do
// not participate; only edges satisfying the edge predicate contribute. Use
// the plain PageRank when no predicate is supplied. Performance is not a
// concern on the predicate path, so this is a simple sequential power
// iteration mirroring the plain PageRank.
class PageRankPred {
 public:
  PageRankPred(const StorageReadInterface& graph, label_t vertex_label,
               label_t edge_label, double damping_factor, int max_iterations,
               int concurrency, bool directed,
               execution::ExprBase* vertex_pred,
               execution::ExprBase* edge_pred);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int pr_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  double damping_factor_;
  int max_iterations_;
  int concurrency_;
  bool directed_;
  execution::ExprBase* vertex_pred_;
  execution::ExprBase* edge_pred_;

  std::unique_ptr<double[]> pr_;
  vector_t<vid_t> vertices_;
};

}  // namespace gds
}  // namespace neug
