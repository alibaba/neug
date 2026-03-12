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

#include "neug/storages/loader/abstract_property_graph_loader.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"

namespace neug {

void AbstractPropertyGraphLoader::addVerticesToVertexTable(
    label_t v_label_id, const std::string& label_name, DataType pk_type,
    const std::string& pk_name, int pk_ind,
    const std::vector<std::string>& v_files) {
  for (auto& v_file : v_files) {
    LOG(INFO) << "Start to load vertex label: " << label_name
              << " from file: " << v_file;
    auto suppliers =
        createVertexRecordBatchSupplier(v_label_id, label_name, v_file, pk_type,
                                        pk_name, pk_ind, loading_config_, 0);
    for (auto& supplier : suppliers) {
      graph_.BatchAddVertices(v_label_id, supplier);
    }
  }
}

void AbstractPropertyGraphLoader::addVertices(
    label_t v_label_id, const std::vector<std::string>& v_files) {
  auto pks = schema_.get_vertex_primary_key(v_label_id);
  if (pks.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex label: "
               << schema_.get_vertex_label_name(v_label_id);
    return;
  }
  auto primary_key = pks[0];
  auto pk_type = std::get<0>(primary_key);
  auto pk_name = std::get<1>(primary_key);
  auto pk_ind = std::get<2>(primary_key);
  auto pk_type_id = pk_type.id();
  if (pk_type_id != DataTypeId::kInt64 && pk_type_id != DataTypeId::kVarchar &&
      pk_type_id != DataTypeId::kInt32 && pk_type_id != DataTypeId::kUInt32 &&
      pk_type_id != DataTypeId::kUInt64) {
    LOG(FATAL)
        << "Only support int64_t, uint64_t, int32_t, uint32_t and string "
           "primary key for vertex.";
    return;
  }

  return addVerticesToVertexTable(v_label_id,
                                  schema_.get_vertex_label_name(v_label_id),
                                  pk_type, pk_name, pk_ind, v_files);
}

void AbstractPropertyGraphLoader::loadVertices() {
  auto vertex_sources = loading_config_.GetVertexLoadingMeta();
  if (vertex_sources.empty()) {
    LOG(INFO) << "Skip loading vertices since no vertex source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading vertices with single thread...";
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      auto v_label_id = iter->first;
      auto v_files = iter->second;
      addVertices(v_label_id, v_files);
    }
  } else {
    // copy vertex_sources and edge sources to vector, since we need to
    // use multi-thread loading.
    std::vector<std::pair<label_t, std::vector<std::string>>> vertex_files;
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      vertex_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << " " << vertex_files.size() << " vertex files, ";
    std::atomic<size_t> v_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = v_ind.fetch_add(1);
          if (cur >= vertex_files.size()) {
            break;
          }
          auto v_label_id = vertex_files[cur].first;
          addVertices(v_label_id, vertex_files[cur].second);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }

    LOG(INFO) << "Finished loading vertices";
  }
}

void AbstractPropertyGraphLoader::addEdgesToEdgeTable(
    label_t src_label_id, label_t dst_label_id, label_t e_label_id,
    const std::vector<std::string>& e_files) {
  for (auto& e_file : e_files) {
    LOG(INFO) << "Start to load edge label: "
              << schema_.get_edge_label_name(e_label_id)
              << " from file: " << e_file;
    auto suppliers = createEdgeRecordBatchSupplier(
        src_label_id, dst_label_id, e_label_id, e_file, loading_config_, 0);
    for (auto& supplier : suppliers) {
      graph_.BatchAddEdges(src_label_id, dst_label_id, e_label_id, supplier);
    }
  }
}

void AbstractPropertyGraphLoader::addEdges(
    label_t src_label_id, label_t dst_label_id, label_t e_label_id,
    const std::vector<std::string>& e_files) {
  return addEdgesToEdgeTable(src_label_id, dst_label_id, e_label_id, e_files);
}

void AbstractPropertyGraphLoader::loadEdges() {
  auto& edge_sources = loading_config_.GetEdgeLoadingMeta();

  if (edge_sources.empty()) {
    LOG(INFO) << "Skip loading edges since no edge source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading edges with single thread...";
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      auto src_label_id = std::get<0>(iter->first);
      auto dst_label_id = std::get<1>(iter->first);
      auto e_label_id = std::get<2>(iter->first);
      auto& e_files = iter->second;

      addEdges(src_label_id, dst_label_id, e_label_id, e_files);
    }
  } else {
    std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                          std::vector<std::string>>>
        edge_files;
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      edge_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << edge_files.size() << " edge files.";
    std::atomic<size_t> e_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = e_ind.fetch_add(1);
          if (cur >= edge_files.size()) {
            break;
          }
          auto& edge_file = edge_files[cur];
          auto src_label_id = std::get<0>(edge_file.first);
          auto dst_label_id = std::get<1>(edge_file.first);
          auto e_label_id = std::get<2>(edge_file.first);
          auto& file_names = edge_file.second;
          addEdges(src_label_id, dst_label_id, e_label_id, file_names);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Finished loading edges";
  }
}

result<bool> AbstractPropertyGraphLoader::LoadFragment() {
  try {
    loadVertices();
    loadEdges();
    graph_.Compact(false, 0.0, 1);
    graph_.Dump(false);

  } catch (const std::exception& e) {
    printDiskRemaining(work_dir_);
    LOG(ERROR) << "Load fragment failed: " << e.what();
    return result<bool>(false);
  }
  return result<bool>(true);
}

}  // namespace neug