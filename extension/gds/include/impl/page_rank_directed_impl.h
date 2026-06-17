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

#include <memory>
#include <vector>

#include "neug/execution/common/columns/container_types.h"
#include "neug/execution/expression/expr.h"

namespace neug {
namespace gds {

class DirectedPageRank {
 public:
  DirectedPageRank(const StorageReadInterface& graph, label_t vertex_label,
                   label_t edge_label, double damping_factor, int concurrency);

  void compute(int max_iterations);
  void sink(execution::Context& ctx, int node_alias, int pr_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  std::unique_ptr<double[]> pr_;
  std::unique_ptr<uint32_t[]> out_degree_;
  vector_t<vid_t> valid_vertices_;
  double damping_factor_;
  int concurrency_;
  uint32_t dangling_count_;
  double dangling_sum_;
  uint32_t vertex_count_;
};

}  // namespace gds
}  // namespace neug
