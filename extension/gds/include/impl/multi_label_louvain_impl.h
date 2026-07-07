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
namespace neug { namespace gds { namespace community {
class MultiLabelLouvain {
 public:
  MultiLabelLouvain(const StorageReadInterface& graph,
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
  std::vector<size_t> label_base_offsets_;
  std::vector<size_t> label_local_sizes_;
  std::vector<std::vector<size_t>> label_out_triplets_;
  std::vector<std::vector<size_t>> label_in_triplets_;
  std::vector<label_t> global_to_label_;
  std::vector<vid_t> global_to_vid_;
  std::unordered_map<label_t, size_t> label_to_index_;
  std::vector<uint32_t> valid_vertices_;
  size_t vertex_count_ = 0;
  size_t array_size_ = 0;
  std::unique_ptr<uint32_t[]> community_;
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
}}}  // namespace neug::gds::community
