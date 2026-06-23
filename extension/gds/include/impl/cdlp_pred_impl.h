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

// Community Detection using Label Propagation on a subgraph constrained by an
// edge predicate (and an optional vertex predicate). Only edges that satisfy
// the predicate propagate labels; vertices excluded by the vertex predicate
// are dropped from the result. Use the plain CDLP when no edge predicate is
// supplied. Performance is not a concern on the predicate path, so this is a
// simple sequential implementation.
class CDLPPred {
 public:
  CDLPPred(const StorageReadInterface& graph, label_t vertex_label,
           const execution::LabelTriplet& edge_triplet, int max_iterations,
           int concurrency, execution::ExprBase* vertex_pred,
           execution::ExprBase* edge_pred);

  void compute();
  void sink(execution::Context& ctx, int32_t node_alias, int32_t label_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  execution::LabelTriplet edge_triplet_;
  int max_iterations_;
  int concurrency_;
  execution::ExprBase* vertex_pred_;
  execution::ExprBase* edge_pred_;

  std::unique_ptr<int64_t[]> community_;
  vector_t<vid_t> vertices_;
};

}  // namespace gds
}  // namespace neug
