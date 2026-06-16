/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
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
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
namespace community {

struct LeidenResult {
  std::vector<int64_t> community;
  double modularity = 0.0;
  int64_t num_communities = 0;
};

/// Runs Leiden community detection directly on StorageReadInterface.
/// No graph copy — traverses edges via StorageReadInterface.
/// Uses vid_t as direct array indices (same pattern as PageRank/WCC).
class Leiden {
 public:
  Leiden(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, double resolution, double threshold,
         int concurrency);

  void compute();

  void sink(execution::Context& ctx, int node_alias, int community_alias);

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  double resolution_;
  double threshold_;
  int concurrency_;

  // Vertex info
  std::vector<vid_t> valid_vertices_;
  size_t vertex_count_ = 0;

  // Arrays indexed by vid_t
  size_t array_size_ = 0;
  std::unique_ptr<uint32_t[]> community_;
  std::unique_ptr<double[]> degree_;
  std::unique_ptr<double[]> stot_;

  // Per-thread scratch arrays for parallel moving phase and refine
  // Flat arrays indexed as [tid * array_size_ + community_id]
  std::unique_ptr<double[]> thread_comm_weight_;
  std::unique_ptr<uint32_t[]> thread_gen_;
  int num_threads_ = 1;
  // For refine(): sub-community assignment and membership check
  static constexpr uint32_t kInvalidSubCom = UINT32_MAX;
  std::unique_ptr<uint32_t[]> sub_com_flat_;  // sub-community ID per vid_t

  double m_ = 0.0;
  double modularity_ = 0.0;

  // Internal methods
  bool local_moving_phase();
  void refine();
};

/// @brief Run Leiden community detection.
LeidenResult RunLeiden(const StorageReadInterface& graph,
                       label_t vertex_label, label_t edge_label,
                       bool directed, double resolution, double threshold,
                       int concurrency);

}  // namespace community
}  // namespace gds
}  // namespace neug
