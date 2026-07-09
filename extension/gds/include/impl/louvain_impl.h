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
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/common/types/graph_types.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"
namespace neug {
namespace gds {
namespace community {
class Louvain {
 public:
  Louvain(const StorageReadInterface& graph, std::vector<label_t> vertex_labels,
          std::vector<LabelTriplet> edge_triplets, double resolution,
          double threshold, int concurrency,
          const std::string& initial_community_property = "");
  void compute();
  void sink(execution::Context& ctx, int node_alias, int community_alias);

 private:
  const StorageReadInterface& graph_;
  std::vector<label_t> vertex_labels_;
  std::vector<LabelTriplet> edge_triplets_;
  double resolution_;
  double threshold_;
  int concurrency_;
  std::string initial_community_property_;
  std::vector<size_t> label_base_offsets_;
  std::vector<size_t> label_local_sizes_;
  std::vector<std::vector<size_t>> label_out_triplets_;
  std::vector<std::vector<size_t>> label_in_triplets_;
  std::vector<size_t> triplet_src_base_;
  std::vector<size_t> triplet_dst_base_;
  std::vector<label_t> global_to_label_;
  std::vector<vid_t> global_to_vid_;
  std::vector<size_t> global_to_label_idx_;
  std::unordered_map<label_t, size_t> label_to_index_;
  std::vector<uint32_t> valid_vertices_;
  size_t vertex_count_ = 0;
  size_t array_size_ = 0;
  std::unique_ptr<uint32_t[]> community_;
  std::unique_ptr<uint32_t[]> initial_community_;  // for stable ID inheritance
  std::unique_ptr<double[]> degree_;
  std::unique_ptr<double[]> stot_;
  std::unique_ptr<double[]> thread_comm_weight_;
  std::unique_ptr<uint32_t[]> thread_gen_;
  int num_threads_ = 1;
  double m_ = 0.0;
  double modularity_ = 0.0;
  // Simple-graph fast path
  bool is_simple_graph_ = false;
  label_t simple_vertex_label_{};
  label_t simple_edge_label_{};
  bool one_level();
};
}  // namespace community
}  // namespace gds
}  // namespace neug
