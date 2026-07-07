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

#include "neug/execution/common/context.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
namespace community {

/// Multi-label Leiden community detection.
/// Supports multiple vertex labels and edge triplets (heterogeneous subgraphs).
/// Optionally accepts initial community assignments from a vertex property
/// for incremental/warm-start community detection.
class MultiLabelLeiden {
 public:
  MultiLabelLeiden(const StorageReadInterface& graph,
                   std::vector<label_t> vertex_labels,
                   std::vector<execution::LabelTriplet> edge_triplets,
                   double resolution, double threshold, int concurrency,
                   const std::string& initial_community_property = "");

  void compute();

  void sink(execution::Context& ctx, int node_alias, int community_alias);

 private:
  const StorageReadInterface& graph_;
  std::vector<label_t> vertex_labels_;
  std::vector<execution::LabelTriplet> edge_triplets_;
  double resolution_;
  double threshold_;
  int concurrency_;
  std::string initial_community_property_;

  // Global index mapping: different labels may have overlapping vid_t values,
  // so we map (label_idx, local_vid) -> global_id via offsets.
  std::vector<size_t> label_base_offsets_;  // base offset for each label
  std::vector<size_t> label_local_sizes_;   // max_vid + 1 for each label

  // Per-label triplet acceleration: which triplets are relevant for each label
  // label_out_triplets_[label_idx] = indices into edge_triplets_ where
  // src_label == vertex_labels_[label_idx]
  std::vector<std::vector<size_t>> label_out_triplets_;
  // label_in_triplets_[label_idx] = indices into edge_triplets_ where
  // dst_label == vertex_labels_[label_idx]
  std::vector<std::vector<size_t>> label_in_triplets_;

  // Reverse mapping: global_id -> (label, local vid) for sink()
  std::vector<label_t> global_to_label_;
  std::vector<vid_t> global_to_vid_;

  // Label index lookup
  std::unordered_map<label_t, size_t> label_to_index_;

  // Algorithm state arrays (indexed by global_id)
  std::vector<uint32_t> valid_vertices_;  // global IDs of all valid vertices
  size_t vertex_count_ = 0;
  size_t array_size_ = 0;
  std::unique_ptr<uint32_t[]> community_;
  std::unique_ptr<double[]> degree_;
  std::unique_ptr<double[]> stot_;

  // Per-thread scratch arrays for parallel moving phase and refine
  std::unique_ptr<double[]> thread_comm_weight_;
  std::unique_ptr<uint32_t[]> thread_gen_;
  int num_threads_ = 1;

  // For refine(): sub-community assignment
  static constexpr uint32_t kInvalidSubCom = UINT32_MAX;
  std::unique_ptr<uint32_t[]> sub_com_flat_;

  double m_ = 0.0;
  double modularity_ = 0.0;

  // Simple-graph fast path: when vertex_labels_.size()==1 && edge_triplets_.size()==1,
  // fall back to parallel preprocessing and cached views (zero overhead vs standard leiden).
  bool is_simple_graph_ = false;
  label_t simple_vertex_label_{};
  label_t simple_edge_label_{};

  // Internal methods
  bool local_moving_phase();
  void refine();
};

}  // namespace community
}  // namespace gds
}  // namespace neug
