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
#include "neug/execution/expression/predicates.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

class CDLP {
 public:
  CDLP(const StorageReadInterface& graph, label_t vertex_label,
       const execution::LabelTriplet& edge_triplet, int max_iterations,
       int concurrency);

  void compute();
  void sink(execution::Context& ctx, int32_t node_alias, int32_t label_alias);

 private:
  void init_communities();

  bool run_single_iteration(int64_t* buffer, const size_t* offsets,
                            int iteration, const CsrView& ie_view,
                            const CsrView& oe_view);

  int64_t get_majority_community(const CsrView& ie_view, const CsrView& oe_view,
                                 vid_t dst, int64_t* communities) const;

  const StorageReadInterface& graph_;
  label_t vertex_label_;
  execution::LabelTriplet edge_triplet_;
  int max_iterations_;
  int concurrency_;

  std::unique_ptr<int64_t[]> community_;
  std::unique_ptr<int64_t[]> next_community_;
  vector_t<vid_t> vertices_;
};

}  // namespace gds
}  // namespace neug
