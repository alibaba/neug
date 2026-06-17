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
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

class LCCDirected {
 public:
  LCCDirected(const StorageReadInterface& graph, label_t vertex_label,
              label_t edge_label, int degree_threshold, int concurrency);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int lcc_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  int degree_threshold_;
  int concurrency_;

  vector_t<vid_t> vertices_;
  std::vector<int32_t> degrees_;
  std::vector<int32_t> unique_degrees_;
  std::vector<std::vector<vid_t>> neighbors_;
  std::vector<std::vector<int32_t>> neighbor_weights_;
  std::vector<int64_t> triangles_;
  std::vector<double> lcc_;
};

}  // namespace gds
}  // namespace neug
