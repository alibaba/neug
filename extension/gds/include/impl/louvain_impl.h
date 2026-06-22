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

struct LouvainResult {
  std::vector<int64_t> community;  // indexed by vid_t
  double modularity = 0.0;
  int64_t num_communities = 0;
};

/// Runs Louvain community detection directly on StorageReadInterface.
/// No graph copy — traverses edges via StorageReadInterface.
/// Uses vid_t as direct array indices (same pattern as PageRank/WCC).
class Louvain {
 public:
  Louvain(const StorageReadInterface& graph, label_t vertex_label,
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

  // Arrays indexed by vid_t (vid_t values are contiguous for loaded data)
  size_t array_size_ = 0;
  std::unique_ptr<uint32_t[]> community_;  // community[vid]
  std::unique_ptr<double[]> degree_;       // degree[vid]
  std::unique_ptr<double[]> stot_;         // community total degree

  // Per-thread scratch arrays for parallel one_level()
  // Flat arrays indexed as [tid * array_size_ + community_id]
  std::unique_ptr<double[]> thread_comm_weight_;
  std::unique_ptr<uint32_t[]> thread_gen_;
  int num_threads_ = 1;

  double m_ = 0.0;       // total edge weight (undirected: count each edge once)
  double modularity_ = 0.0;

  // Internal methods
  bool one_level();
};

/// @brief Run Louvain community detection.
LouvainResult RunLouvain(const StorageReadInterface& graph,
                         label_t vertex_label, label_t edge_label,
                         bool directed, double resolution, double threshold,
                         int concurrency);

}  // namespace community
}  // namespace gds
}  // namespace neug
