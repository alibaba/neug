
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

#include "neug/storages/rt_mutable_graph/loader/basic_fragment_loader.h"

#include <filesystem>

#include "libgrape-lite/grape/io/local_io_adaptor.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/utils/indexers.h"

namespace gs {
class CsrBase;

std::ostream& operator<<(std::ostream& os, const LoadingStatus& status) {
  if (status == LoadingStatus::kLoading) {
    os << "Loading";
  } else if (status == LoadingStatus::kLoaded) {
    os << "Loaded";
  } else if (status == LoadingStatus::kCommited) {
    os << "Commited";
  } else {
    os << "Unknown";
  }
  return os;
}

std::istream& operator>>(std::istream& is, LoadingStatus& status) {
  std::string tmp;
  is >> tmp;
  if (tmp == "Loading") {
    status = LoadingStatus::kLoading;
  } else if (tmp == "Loaded") {
    status = LoadingStatus::kLoaded;
  } else if (tmp == "Commited") {
    status = LoadingStatus::kCommited;
  } else {
    status = LoadingStatus::kUnknown;
  }
  return is;
}

BasicFragmentLoader::BasicFragmentLoader(const Schema& schema,
                                         const std::string& prefix)
    : schema_(schema),
      work_dir_(prefix),
      vertex_label_num_(schema_.vertex_label_num()),
      edge_label_num_(schema_.edge_label_num()) {
  std::filesystem::create_directories(runtime_dir(prefix));
  std::filesystem::create_directories(snapshot_dir(prefix, 0));
  std::filesystem::create_directories(wal_dir(prefix));
  std::filesystem::create_directories(tmp_dir(prefix));
  for (label_t v_label = 0; v_label < vertex_label_num_; v_label++) {
    const auto& v_label_name = schema_.get_vertex_label_name(v_label);
    vertex_tables_.emplace_back(
        schema_.get_vertex_label_name(v_label),
        std::get<0>(schema_.get_vertex_primary_key(v_label)[0]),
        schema_.get_vertex_property_names(v_label),
        schema_.get_vertex_properties(v_label),
        schema_.get_vertex_storage_strategies(v_label_name));
    vertex_tables_.back().Open(snapshot_dir(work_dir_, 0), tmp_dir(work_dir_),
                               0, true);
  }
  for (label_t src_label = 0; src_label < vertex_label_num_; src_label++) {
    for (label_t dst_label = 0; dst_label < vertex_label_num_; dst_label++) {
      for (label_t edge_label = 0; edge_label < edge_label_num_; edge_label++) {
        if (schema_.exist(src_label, dst_label, edge_label)) {
          std::string src_label_name = schema_.get_vertex_label_name(src_label);
          std::string dst_label_name = schema_.get_vertex_label_name(dst_label);
          std::string edge_label_name = schema_.get_edge_label_name(edge_label);
          EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
              src_label, dst_label, edge_label);
          EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
              src_label, dst_label, edge_label);
          auto properties =
              schema_.get_edge_properties(src_label, dst_label, edge_label);
          auto prop_names =
              schema_.get_edge_property_names(src_label, dst_label, edge_label);
          bool oe_mutable = schema_.outgoing_edge_mutable(
              src_label_name, dst_label_name, edge_label_name);
          bool ie_mutable = schema_.incoming_edge_mutable(
              src_label_name, dst_label_name, edge_label_name);
          EdgeTable edge_table(src_label_name, dst_label_name, edge_label_name,
                               oe_strategy, ie_strategy, prop_names, properties,
                               oe_mutable, ie_mutable);
          size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                         dst_label * edge_label_num_ + edge_label;
          edge_tables_.emplace(index, std::move(edge_table));
          edge_tables_.at(index).Open(snapshot_dir(work_dir_, 0),
                                      tmp_dir(work_dir_), 0);
        }
      }
    }
  }

  //  initially create all status files for vertices and edges.
  init_loading_status_file();
}

BasicFragmentLoader::~BasicFragmentLoader() {}

void BasicFragmentLoader::append_vertex_loading_progress(
    const std::string& label_name, LoadingStatus status) {
  auto status_file_path = bulk_load_progress_file(work_dir_);
  std::lock_guard<std::mutex> lock(loading_progress_mutex_);
  std::ofstream status_file(status_file_path, std::ios::app);
  std::stringstream ss;
  ss << "[VertexLabel]:" << label_name << ", [Status]:" << status << "\n";
  status_file << ss.str();
  status_file.close();
  return;
}

void BasicFragmentLoader::append_edge_loading_progress(
    const std::string& src_label_name, const std::string& dst_label_name,
    const std::string& edge_label_name, LoadingStatus status) {
  auto status_file_path = bulk_load_progress_file(work_dir_);
  std::lock_guard<std::mutex> lock(loading_progress_mutex_);
  std::ofstream status_file(status_file_path, std::ios::app);
  std::stringstream ss;
  ss << "[SrcVertexLabel]:" << src_label_name
     << " -> [DstVertexLabel]:" << dst_label_name << " : [EdgeLabel]"
     << edge_label_name << ", [Status]:" << status << "\n";
  status_file << ss.str();
  status_file.close();
  return;
}

void BasicFragmentLoader::init_loading_status_file() {
  for (label_t v_label = 0; v_label < vertex_label_num_; v_label++) {
    auto label_name = schema_.get_vertex_label_name(v_label);
    append_vertex_loading_progress(label_name, LoadingStatus::kLoading);
  }
  VLOG(1) << "Finish init vertex status files";
  for (size_t src_label = 0; src_label < vertex_label_num_; src_label++) {
    std::string src_label_name = schema_.get_vertex_label_name(src_label);
    for (size_t dst_label = 0; dst_label < vertex_label_num_; dst_label++) {
      std::string dst_label_name = schema_.get_vertex_label_name(dst_label);
      for (size_t edge_label = 0; edge_label < edge_label_num_; edge_label++) {
        std::string edge_label_name = schema_.get_edge_label_name(edge_label);
        if (schema_.exist(src_label_name, dst_label_name, edge_label_name)) {
          append_edge_loading_progress(src_label_name, dst_label_name,
                                       edge_label_name,
                                       LoadingStatus::kLoading);
        }
      }
    }
  }
}

void BasicFragmentLoader::LoadFragment() {
  std::string schema_filename = schema_path(work_dir_);
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_filename));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();

  set_snapshot_version(work_dir_, 0);
  clear_tmp(work_dir_);
}

const IndexerType& BasicFragmentLoader::GetLFIndexer(label_t v_label) const {
  CHECK(v_label < vertex_label_num_);
  return vertex_tables_[v_label].get_indexer();
}

IndexerType& BasicFragmentLoader::GetLFIndexer(label_t v_label) {
  CHECK(v_label < vertex_label_num_);
  return vertex_tables_[v_label].get_indexer();
}

DualCsrBase* BasicFragmentLoader::get_csr(label_t src_label_id,
                                          label_t dst_label_id,
                                          label_t edge_label_id) {
  size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                 dst_label_id * edge_label_num_ + edge_label_id;
  return edge_tables_.at(index).GetDualCsr();
}

}  // namespace gs
