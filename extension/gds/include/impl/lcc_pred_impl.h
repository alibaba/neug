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

// Local clustering coefficient on a subgraph constrained by a vertex and/or
// edge predicate. Vertices excluded by the vertex predicate are dropped from
// the result; only edges satisfying the edge predicate are considered. Use the
// plain LCC when no predicate is supplied. Performance is not a concern on the
// predicate path, so this is a simple sequential implementation that evaluates
// the LCC definition directly over each vertex's neighborhood.
class LCCPred {
 public:
  LCCPred(const StorageReadInterface& graph, label_t vertex_label,
          label_t edge_label, bool directed, int concurrency,
          execution::ExprBase* vertex_pred, execution::ExprBase* edge_pred);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int lcc_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  bool directed_;
  int concurrency_;
  execution::ExprBase* vertex_pred_;
  execution::ExprBase* edge_pred_;

  std::unique_ptr<double[]> lcc_;
  vector_t<vid_t> vertices_;
};

}  // namespace gds
}  // namespace neug
