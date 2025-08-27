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

#include "neug/storages/rt_mutable_graph/vertex_table.h"

namespace gs {

void VertexTable::Open(const std::string& snapshot_dir,
                       const std::string& tmp_dir_path, int memory_level,
                       bool build_empty_graph) {
  memory_level_ = memory_level;
  work_dir_ = tmp_dir_path;
  vertex_tomb_ = std::make_shared<Bitset>();
  if (memory_level_ == 0) {
    indexer_.open(
        IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name_),
        snapshot_dir, tmp_dir_path);
    table_->open(vertex_table_prefix(v_label_name_), snapshot_dir, tmp_dir_path,
                 property_names, property_types, storage_strategies);
    if (!build_empty_graph) {
      table_->copy_to_tmp(vertex_table_prefix(v_label_name_), snapshot_dir,
                          tmp_dir_path);
    }
  } else if (memory_level_ == 1) {
    indexer_.open_in_memory(snapshot_dir + "/" + IndexerType::prefix() + "_" +
                            vertex_map_prefix(v_label_name_));
    table_->open_in_memory(vertex_table_prefix(v_label_name_), snapshot_dir,
                           property_names, property_types, storage_strategies);
  } else if (memory_level_ >= 2) {
    indexer_.open_with_hugepages(snapshot_dir + "/" + IndexerType::prefix() +
                                     "_" + vertex_map_prefix(v_label_name_),
                                 (memory_level_ > 2));
    table_->open_with_hugepages(vertex_table_prefix(v_label_name_),
                                snapshot_dir, property_names, property_types,
                                storage_strategies, (memory_level_ > 2));
  }
  vertex_tomb_->resize(indexer_.capacity());
}
size_t VertexTable::vertex_num() const {
  return indexer_.size() - vertex_tomb_->count();
}

size_t VertexTable::lid_num() const { return indexer_.size(); }

vid_t VertexTable::add_vertex(const Any& id) { return indexer_.insert(id); }

vid_t VertexTable::add_vertex_safe(const Any& id) {
  return indexer_.insert_safe(id);
}

bool VertexTable::get_index(const Any& oid, vid_t& lid) const {
  return indexer_.get_index(oid, lid);
}

Any VertexTable::get_oid(vid_t lid) const { return indexer_.get_key(lid); }

bool VertexTable::is_valid_lid(vid_t lid) const {
  return !vertex_tomb_->get(lid);
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  for (auto v : vids) {
    if (v < indexer_.size() && !vertex_tomb_->get(v)) {
      auto oid = indexer_.get_key(v);
      indexer_.remove(oid);
      vertex_tomb_->set(v);
    }
  }
}

void VertexTable::DeleteProperties(const std::vector<std::string>& properties) {
  for (size_t i = property_names.size(); i-- > 0;) {
    if (std::find(properties.begin(), properties.end(), property_names[i]) !=
        properties.end()) {
      table_->delete_column(property_names[i]);
      property_names.erase(property_names.begin() + i);
      property_types.erase(property_types.begin() + i);
      storage_strategies.erase(storage_strategies.begin() + i);
    }
  }
}

void VertexTable::AddProperties(
    const std::vector<std::string>& properties,
    const std::vector<PropertyType>& types,
    const std::vector<StorageStrategy>& strategies) {
  table_->add_columns(properties, types);
  for (size_t i = 0; i < properties.size(); ++i) {
    property_names.push_back(properties[i]);
    property_types.push_back(types[i]);
    if (i >= strategies.size()) {
      storage_strategies.push_back(StorageStrategy::kMem);
    } else {
      storage_strategies.push_back(strategies[i]);
    }
  }
}

void VertexTable::Drop() {
  indexer_.drop();
  table_->drop();
  vertex_tomb_->clear();
  vertex_capacity_ = 0;
  dumped_num_ = 0;
  table_.reset();
  // TODO(zhanglei): reset the indexer.
  // indexer_ = IndexerType();
  property_names.clear();
  property_types.clear();
  storage_strategies.clear();
}

void VertexTable::RenameProperties(const std::vector<std::string>& old_names,
                                   const std::vector<std::string>& new_names) {
  for (size_t i = 0; i < old_names.size(); ++i) {
    auto it =
        std::find(property_names.begin(), property_names.end(), old_names[i]);
    if (it == property_names.end()) {
      LOG(ERROR) << "Column " << old_names[i] << " does not exist.";
      return;
    }
    if (std::find(property_names.begin(), property_names.end(), new_names[i]) !=
        property_names.end()) {
      LOG(ERROR) << "Column " << new_names[i] << " already exists.";
      return;
    }
    size_t index = std::distance(property_names.begin(), it);
    property_names[index] = new_names[i];
    table_->rename_column(old_names[i], new_names[i]);
  }
}
}  // namespace gs