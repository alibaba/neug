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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "neug/storages/csr/dual_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

#include "neug/utils/property/types.h"

#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/vertex_table.h"

namespace gs {
class CsrBase;
template <typename EDATA_T>
class TypedMutableCsrBase;

enum class LoadingStatus {
  kLoading = 0,
  kLoaded = 1,
  kCommited = 2,
  kUnknown = 3,
};

// define << and >> for LoadingStatus
std::ostream& operator<<(std::ostream& os, const LoadingStatus& status);

std::istream& operator>>(std::istream& is, LoadingStatus& status);

// FragmentLoader should use this BasicFragmentLoader to construct
// mutable_csr_fragment.
class BasicFragmentLoader {
 public:
  BasicFragmentLoader(const Schema& schema, const std::string& prefix);

  ~BasicFragmentLoader();

  void LoadFragment();

  template <typename KEY_T>
  void FinishAddingVertex(label_t v_label,
                          const IdIndexer<KEY_T, vid_t>& indexer) {
    CHECK(v_label < vertex_label_num_);
    std::string filename =
        vertex_map_prefix(schema_.get_vertex_label_name(v_label));
    auto primary_keys = schema_.get_vertex_primary_key(v_label);
    auto type = std::get<0>(primary_keys[0]);

    build_lf_indexer<KEY_T, vid_t>(
        indexer, LFIndexer<vid_t>::prefix() + "_" + filename,
        vertex_tables_[v_label].get_indexer(), snapshot_dir(work_dir_, 0),
        tmp_dir(work_dir_), type);
    append_vertex_loading_progress(schema_.get_vertex_label_name(v_label),
                                   LoadingStatus::kLoaded);
    auto label_name = schema_.get_vertex_label_name(v_label);
    auto& v_data = vertex_tables_[v_label].get_properties_table();
    v_data.resize(vertex_tables_[v_label].get_indexer().size());
    v_data.dump(vertex_table_prefix(label_name), snapshot_dir(work_dir_, 0));
    append_vertex_loading_progress(label_name, LoadingStatus::kCommited);
  }

  template <typename EDATA_T>
  void AddNoPropEdgeBatch(label_t src_label_id, label_t dst_label_id,
                          label_t edge_label_id) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;

    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(edge_label_id);
    edge_tables_.at(index).BatchInit(tmp_dir(work_dir_), {}, {}, false);
  }

  template <typename EDATA_T>
  static decltype(auto) get_casted_dual_csr(DualCsrBase* dual_csr) {
    if constexpr (std::is_same_v<EDATA_T, RecordView>) {
      auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr);
      CHECK(casted_dual_csr != NULL);
      return casted_dual_csr;
    } else {
      auto casted_dual_csr = dynamic_cast<DualCsr<EDATA_T>*>(dual_csr);
      CHECK(casted_dual_csr != NULL);
      return casted_dual_csr;
    }
  }
  template <typename EDATA_T, typename VECTOR_T>
  void PutEdges(label_t src_label_id, label_t dst_label_id,
                label_t edge_label_id, const std::vector<VECTOR_T>& edges_vec,
                const std::vector<int32_t>& ie_degree,
                const std::vector<int32_t>& oe_degree, bool build_csr_in_mem) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;
    auto dual_csr = edge_tables_.at(index).GetDualCsr();
    auto casted_dual_csr = get_casted_dual_csr<EDATA_T>(dual_csr);
    CHECK(casted_dual_csr != NULL);
    auto& src_indexer = vertex_tables_[src_label_id].get_indexer();
    auto& dst_indexer = vertex_tables_[dst_label_id].get_indexer();
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(edge_label_id);

    auto INVALID_VID = std::numeric_limits<vid_t>::max();
    std::atomic<size_t> edge_count(0);
    CHECK(ie_degree.size() == dst_indexer.size());
    CHECK(oe_degree.size() == src_indexer.size());

    edge_tables_.at(index).BatchInit(tmp_dir(work_dir_), oe_degree, ie_degree,
                                     build_csr_in_mem);

    std::vector<std::thread> work_threads;
    for (size_t i = 0; i < edges_vec.size(); ++i) {
      work_threads.emplace_back(
          [&](int idx) {
            edge_count.fetch_add(edges_vec[idx].size());
            for (auto& edge : edges_vec[idx]) {
              if (std::get<1>(edge) == INVALID_VID ||
                  std::get<0>(edge) == INVALID_VID) {
                VLOG(10) << "Skip invalid edge:" << std::get<0>(edge) << "->"
                         << std::get<1>(edge);
                continue;
              }
              casted_dual_csr->BatchPutEdge(
                  std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
            }
          },
          i);
    }
    for (auto& t : work_threads) {
      t.join();
    }
    append_edge_loading_progress(src_label_name, dst_label_name,
                                 edge_label_name, LoadingStatus::kLoaded);
    if (schema_.get_sort_on_compaction(src_label_name, dst_label_name,
                                       edge_label_name)) {
      edge_tables_.at(index).SortByEdgeData(1);
    }

    edge_tables_.at(index).Dump(snapshot_dir(work_dir_, 0));
    append_edge_loading_progress(src_label_name, dst_label_name,
                                 edge_label_name, LoadingStatus::kCommited);
    VLOG(10) << "Finish adding edge batch of size: " << edge_count.load();
  }

  Table& GetVertexTable(size_t ind) {
    CHECK(ind < vertex_tables_.size());
    return vertex_tables_[ind].get_properties_table();
  }

  Table& GetEdgePropertiesTable(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;
    return edge_tables_.at(index).get_properties_table();
  }
  // get lf_indexer
  const IndexerType& GetLFIndexer(label_t v_label) const;
  IndexerType& GetLFIndexer(label_t v_label);

  const std::string& work_dir() const { return work_dir_; }

  DualCsrBase* get_csr(label_t src_label_id, label_t dst_label_id,
                       label_t edge_label_id);

 private:
  // create status files for each vertex label and edge triplet pair.
  void append_vertex_loading_progress(const std::string& label_name,
                                      LoadingStatus status);

  void append_edge_loading_progress(const std::string& src_label_name,
                                    const std::string& dst_label_name,
                                    const std::string& edge_label_name,
                                    LoadingStatus status);
  void init_loading_status_file();

  const Schema& schema_;
  std::string work_dir_;
  size_t vertex_label_num_, edge_label_num_;
  std::unordered_map<uint32_t, EdgeTable> edge_tables_;
  std::vector<VertexTable> vertex_tables_;

  // loading progress related
  std::mutex loading_progress_mutex_;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_