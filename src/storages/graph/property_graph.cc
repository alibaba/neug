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

#include "neug/storages/graph/property_graph.h"

#include <assert.h>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "libgrape-lite/grape/io/local_io_adaptor.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "libgrape-lite/grape/types.h"
#include "libgrape-lite/grape/util.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/file_names.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/yaml_utils.h"

namespace gs {

PropertyGraph::PropertyGraph()
    : vertex_label_num_(0), edge_label_num_(0), memory_level_(1) {}

PropertyGraph::~PropertyGraph() {
  std::vector<size_t> degree_list(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    degree_list[i] = vertex_tables_[i].lid_num();
    vertex_tables_[i].Reserve(degree_list[i]);
  }
  for (size_t src_label = 0; src_label != vertex_label_num_; ++src_label) {
    for (size_t dst_label = 0; dst_label != vertex_label_num_; ++dst_label) {
      for (size_t e_label = 0; e_label != edge_label_num_; ++e_label) {
        size_t index =
            schema_.generate_edge_label(src_label, dst_label, e_label);
        auto edge_table = edge_tables_.find(index);
        if (edge_table != edge_tables_.end()) {
          edge_table->second.Reserve(degree_list[src_label],
                                     degree_list[dst_label]);
        }
      }
    }
  }
}

void PropertyGraph::loadSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);
}

void PropertyGraph::Clear() {
  vertex_tables_.clear();
  edge_tables_.clear();
  vertex_label_num_ = 0;
  edge_label_num_ = 0;
  schema_.Clear();
}

Status PropertyGraph::create_vertex_type(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  if (schema_.contains_vertex_label(vertex_type_name)) {
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Vertex label already exists.");
    } else {
      return Status(StatusCode::OK, "Vertex label already exists.");
    }
  }
  std::vector<std::string> property_names;
  std::vector<PropertyType> property_types;
  // TODO(zhanglei): Not used
  std::vector<Any> default_property_values;
  std::vector<std::tuple<PropertyType, std::string, size_t>> primary_keys;
  std::vector<int> primary_key_inds(primary_key_names.size(), -1);
  if (primary_key_inds.size() > 1) {
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Multi primary keys are not supported.");
  } else if (primary_key_inds.size() == 0) {
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "At least one primary key is required.");
  }
  for (size_t i = 0; i < properties.size(); i++) {
    auto [type, name, default_value] = properties[i];
    property_names.emplace_back(name);
    property_types.emplace_back(type);
    default_property_values.emplace_back(default_value);
  }
  for (size_t i = 0; i < primary_key_names.size(); i++) {
    std::string primary_key_name = primary_key_names.at(i);
    for (size_t j = 0; j < property_names.size(); j++) {
      if (property_names[j] == primary_key_name) {
        primary_key_inds[i] = j;
        break;
      }
    }
    if (primary_key_inds[i] == -1) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " is not found in properties";
      return Status(
          StatusCode::ERR_INVALID_SCHEMA,
          "Primary key " + primary_key_name + " is not found in properties");
    }
    if (property_types[primary_key_inds[i]] != PropertyType::kInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kStringView &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kInt32 &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt32 &&
        !property_types[primary_key_inds[i]].IsVarchar()) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " should be int64/int32/uint64/uint32 or string/varchar";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Primary key " + primary_key_name +
                        " should be int64/int32/uint64/"
                        "uint32 or string/varchar");
    }
    primary_keys.emplace_back(property_types[primary_key_inds[i]],
                              property_names[primary_key_inds[i]],
                              primary_key_inds[i]);
    property_names.erase(property_names.begin() + primary_key_inds[i]);
    property_types.erase(property_types.begin() + primary_key_inds[i]);
  }
  std::vector<StorageStrategy> strategies(property_types.size(),
                                          StorageStrategy::kMem);
  std::string description;
  size_t max_num = ((size_t) 1) << 8;
  schema_.add_vertex_label(vertex_type_name,

                           property_types, property_names, primary_keys,
                           strategies, max_num, description);
  label_t vertex_label_id = schema_.get_vertex_label_id(vertex_type_name);
  vertex_tables_.emplace_back(
      vertex_type_name,
      std::get<0>(schema_.get_vertex_primary_key(vertex_label_id)[0]),
      property_names, property_types, strategies);

  // TODO: use memory level.
  vertex_tables_.back().Open(snapshot_dir(work_dir_, 0), tmp_dir(work_dir_),
                             memory_level_, true);
  vertex_tables_.back().Reserve(4096);
  vertex_label_num_ = schema_.vertex_label_num();
  while (v_mutex_.size() < vertex_label_num_) {
    v_mutex_.emplace_back(std::make_shared<std::mutex>());
  }

  LOG(INFO) << "create_vertex_type: vertex_type_name: " << vertex_type_name
            << ", vertex_label_id: " << static_cast<int32_t>(vertex_label_id)
            << ",properties " << property_names.size()
            << ", primary_key_names: " << primary_key_names[0];
  return gs::Status::OK();
}

Status PropertyGraph::batch_add_vertices(label_t label_id,
                                         std::vector<Any>&& vertices,
                                         std::unique_ptr<Table>&& table,
                                         timestamp_t ts) {
  if (vertices.size() == 0) {
    return gs::Status::OK();
  }
  vertex_tables_[label_id].BatchAddVertices(std::move(vertices),
                                            std::move(table), ts);
  return gs::Status::OK();
}

Status PropertyGraph::batch_add_edges(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    std::vector<std::tuple<vid_t, vid_t, size_t>>&& edges_vec,
    std::unique_ptr<Table>&& table) {
  if (edges_vec.size() == 0) {
    return gs::Status::OK();
  }
  int index =
      schema_.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  edge_tables_.at(index).BatchAddEdges(std::move(edges_vec), std::move(table));
  return gs::Status::OK();
}

Status PropertyGraph::create_edge_type(
    const std::string& src_vertex_type, const std::string& dst_vertex_type,
    const std::string& edge_type_name,
    const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
    bool error_on_conflict,  // Not used
    EdgeStrategy oe_edge_strategy, EdgeStrategy ie_edge_strategy) {
  LOG(INFO) << "create_edge_type: src_vertex_type: " << src_vertex_type
            << ", dst_vertex_type: " << dst_vertex_type
            << ", edge_type_name: " << edge_type_name;
  if (!schema_.contains_vertex_label(src_vertex_type)) {
    LOG(ERROR) << "Source_vertex [" << src_vertex_type
               << "] does not exist in the graph.";
    return Status(
        StatusCode::ERR_INVALID_SCHEMA,
        "Source_vertex [" + src_vertex_type + "] does not exist in the graph.");
  }
  if (!schema_.contains_vertex_label(dst_vertex_type)) {
    LOG(ERROR) << "Destination_vertex [" << dst_vertex_type
               << "] does not exist in the graph.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Destination_vertex [" + dst_vertex_type +
                      "] does not exist in the graph.");
  }
  if (schema_.has_edge_label(src_vertex_type, dst_vertex_type,
                             edge_type_name)) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_vertex_type
               << "] to [" << dst_vertex_type << "] already exists";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Edge [" + edge_type_name + "] from [" + src_vertex_type +
                        "] to [" + dst_vertex_type + "] already exists");
    } else {
      return Status(StatusCode::OK, "Edge triplet already exists.");
    }
  }
  std::vector<std::string> property_names;
  std::vector<PropertyType> property_types;
  // TODO(zhanglei): Not used
  std::vector<Any> default_property_values;
  for (size_t i = 0; i < properties.size(); i++) {
    auto [type, name, default_value] = properties[i];
    property_names.emplace_back(name);
    property_types.emplace_back(type);
    default_property_values.emplace_back(default_value);
  }
  EdgeStrategy cur_ie = EdgeStrategy::kMultiple;
  EdgeStrategy cur_oe = EdgeStrategy::kMultiple;
  bool oe_mutable = true, ie_mutable = true;
  bool cur_sort_on_compaction = false;
  std::string description;
  schema_.add_edge_label(src_vertex_type, dst_vertex_type, edge_type_name,
                         property_types, property_names, cur_oe, cur_ie,
                         oe_mutable, ie_mutable, cur_sort_on_compaction,
                         description);
  edge_label_num_ = schema_.edge_label_num();

  label_t src_label_i = schema_.get_vertex_label_id(src_vertex_type);
  label_t dst_label_i = schema_.get_vertex_label_id(dst_vertex_type);
  label_t e_label_i = schema_.get_edge_label_id(edge_type_name);
  size_t index =
      schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
  EdgeStrategy oe_strategy = EdgeStrategy::kMultiple;
  EdgeStrategy ie_strategy = EdgeStrategy::kMultiple;
  EdgeTable edge_table(src_vertex_type, dst_vertex_type, edge_type_name,
                       oe_strategy, ie_strategy, property_names, property_types,
                       oe_mutable, ie_mutable);
  edge_tables_.emplace(index, std::move(edge_table));
  auto src_v_capacity = std::max(
      vertex_tables_[src_label_i].get_indexer().capacity(), (size_t) 4096);
  auto dst_v_capacity = std::max(
      vertex_tables_[dst_label_i].get_indexer().capacity(), (size_t) 4096);
  edge_tables_.at(index).Open(snapshot_dir(work_dir_, 0), tmp_dir(work_dir_),
                              memory_level_, src_v_capacity, dst_v_capacity);

  edge_tables_.at(index).Reserve(src_v_capacity, dst_v_capacity);

  return gs::Status::OK();
}

Status PropertyGraph::add_vertex_properties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<PropertyType, std::string, Any>>&
        add_properties,
    bool error_on_conflict) {
  if (!schema_.contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex label[" << vertex_type_name << "] does not exists.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Vertex label[" + vertex_type_name + "] does not exists.");
    } else {
      return Status(StatusCode::OK,
                    "Vertex label " + vertex_type_name + "] does not exists.");
    }
  }
  std::vector<std::string> add_property_names;
  std::vector<PropertyType> add_property_types;
  std::vector<Any> add_default_property_values;
  for (size_t i = 0; i < add_properties.size(); i++) {
    auto [property_type, property_name, default_value] = add_properties[i];
    if (schema_.vertex_has_property(vertex_type_name, property_name)) {
      LOG(ERROR) << "Property [" << property_name
                 << "] already exists in vertex [" << vertex_type_name << "].";
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA,
                      "Property [" + property_name +
                          "] already exists in vertex [" + vertex_type_name +
                          "].");
      } else {
        return Status(StatusCode::OK, "Property [" + property_name +
                                          "] already exists in vertex [" +
                                          vertex_type_name + "].");
      }
    }
    add_property_names.emplace_back(property_name);
    add_property_types.emplace_back(property_type);
    add_default_property_values.emplace_back(default_value);
  }
  schema_.add_vertex_properties(vertex_type_name, add_property_names,
                                add_property_types,
                                add_default_property_values);
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);
  vertex_tables_[v_label].AddProperties(add_property_names, add_property_types);
  return gs::Status::OK();
}

Status PropertyGraph::add_edge_properties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::tuple<PropertyType, std::string, Any>>&
        add_properties,
    bool error_on_conflict) {
  if (!schema_.has_edge_label(src_type_name, dst_type_name, edge_type_name)) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name << "] does not exist";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Edge [" + edge_type_name + "] from [" + src_type_name +
                        "] to [" + dst_type_name + "] does not exist");
    } else {
      return Status(StatusCode::OK, "Edge Triplet doesn't exists");
    }
  }
  std::vector<std::string> add_property_names;
  std::vector<PropertyType> add_property_types;
  std::vector<Any> add_default_property_values;
  for (size_t i = 0; i < add_properties.size(); i++) {
    auto [property_type, property_name, default_value] = add_properties[i];
    if (schema_.edge_has_property(src_type_name, dst_type_name, edge_type_name,
                                  property_name)) {
      LOG(ERROR) << "Property [" << property_name
                 << "] already exists in edge [" << edge_type_name << "] from ["
                 << src_type_name << "] to [" << dst_type_name << "].";
      std::string msg = "Property [" + property_name +
                        "] already exists in edge [" + edge_type_name +
                        "] from [" + src_type_name + "] to [" + dst_type_name +
                        "].";
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    add_property_names.emplace_back(property_name);
    add_property_types.emplace_back(property_type);
    add_default_property_values.emplace_back(default_value);
  }
  label_t src_label = schema_.get_vertex_label_id(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id(dst_type_name);
  label_t e_label = schema_.get_edge_label_id(edge_type_name);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);
  // Before adding properties, we need to check whether the csr data type
  // needs to be changed.

  schema_.add_edge_properties(src_type_name, dst_type_name, edge_type_name,
                              add_property_names, add_property_types,
                              add_default_property_values);
  if (edge_tables_.find(index) == edge_tables_.end()) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot add properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot add properties.");
  }

  auto& edge_table = edge_tables_.at(index);
  edge_table.AddProperties(add_property_names, add_property_types);

  return gs::Status::OK();
}

Status PropertyGraph::rename_vertex_properties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<std::string, std::string>>& update_properties,
    bool error_on_conflict) {
  if (!schema_.contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex label[" << vertex_type_name << "] does not exists.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Vertex label[" + vertex_type_name + "] does not exists.");
    } else {
      return Status(StatusCode::OK,
                    "Vertex label[" + vertex_type_name + "] does not exists.");
    }
  }
  std::vector<std::string> update_property_names;
  std::vector<std::string> update_property_renames;
  for (size_t i = 0; i < update_properties.size(); i++) {
    auto [property_name, property_rename] = update_properties[i];
    if (!schema_.vertex_has_property(vertex_type_name, property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in vertex [" + vertex_type_name +
                        "].";
      LOG(ERROR) << msg;
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    update_property_names.emplace_back(property_name);
    update_property_renames.emplace_back(property_rename);
  }
  schema_.update_vertex_properties(vertex_type_name, update_property_names,
                                   update_property_renames);
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);
  vertex_tables_.at(v_label).RenameProperties(update_property_names,
                                              update_property_renames);

  return gs::Status::OK();
}

Status PropertyGraph::rename_edge_properties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::tuple<std::string, std::string>>& update_properties,
    bool error_on_conflict) {
  if (!schema_.has_edge_label(src_type_name, dst_type_name, edge_type_name)) {
    std::string msg = "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name + "] does not exist";
    LOG(ERROR) << msg;
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
    } else {
      return Status(StatusCode::OK, msg);
    }
  }
  std::vector<std::string> update_property_names;
  std::vector<std::string> update_property_renames;
  for (size_t i = 0; i < update_properties.size(); i++) {
    auto [property_name, property_rename] = update_properties[i];
    if (!schema_.edge_has_property(src_type_name, dst_type_name, edge_type_name,
                                   property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in edge [" + edge_type_name +
                        "] from [" + src_type_name + "] to [" + dst_type_name +
                        "].";
      LOG(ERROR) << msg;
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    update_property_names.emplace_back(property_name);
    update_property_renames.emplace_back(property_rename);
  }
  schema_.update_edge_properties(src_type_name, dst_type_name, edge_type_name,
                                 update_property_names,
                                 update_property_renames);
  label_t src_label = schema_.get_vertex_label_id(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id(dst_type_name);
  label_t e_label = schema_.get_edge_label_id(edge_type_name);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);
  if (edge_tables_.find(index) == edge_tables_.end()) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot rename properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot rename properties.");
  }
  auto& edge_table = edge_tables_.at(index);

  edge_table.RenameProperties(update_property_names, update_property_renames);
  return gs::Status::OK();
}

Status PropertyGraph::delete_vertex_properties(
    const std::string& vertex_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  if (!schema_.contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex label[" << vertex_type_name << "] does not exists.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Vertex label[" + vertex_type_name + "] does not exists.");
    } else {
      return gs::Status(StatusCode::OK, "Vertex label[" + vertex_type_name +
                                            "] does not exists.");
    }
  }
  std::vector<std::string> delete_property_names;
  for (size_t i = 0; i < delete_properties.size(); i++) {
    auto property_name = delete_properties[i];
    if (!schema_.vertex_has_property(vertex_type_name, property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in vertex [" + vertex_type_name +
                        "].";
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA,
                      "Property [" + property_name +
                          "] does not exist in vertex [" + vertex_type_name +
                          "].");
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    delete_property_names.emplace_back(property_name);
  }
  schema_.delete_vertex_properties(vertex_type_name, delete_property_names);
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);

  vertex_tables_[v_label].DeleteProperties(delete_property_names);
  return gs::Status::OK();
}

Status PropertyGraph::delete_edge_properties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  if (!schema_.has_edge_label(src_type_name, dst_type_name, edge_type_name)) {
    std::string msg = "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name + "] does not exist";
    LOG(ERROR) << msg;
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
    } else {
      return Status(StatusCode::OK, msg);
    }
  }
  std::vector<std::string> delete_property_names;
  for (size_t i = 0; i < delete_properties.size(); i++) {
    auto property_name = delete_properties[i];
    if (!schema_.edge_has_property(src_type_name, dst_type_name, edge_type_name,
                                   property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in edge [" + edge_type_name +
                        "] from [" + src_type_name + "] to [" + dst_type_name +
                        "].";
      LOG(ERROR) << msg;
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_SCHEMA, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    delete_property_names.emplace_back(property_name);
  }
  schema_.delete_edge_properties(src_type_name, dst_type_name, edge_type_name,
                                 delete_property_names);
  label_t src_label = schema_.get_vertex_label_id(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id(dst_type_name);
  label_t e_label = schema_.get_edge_label_id(edge_type_name);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);

  if (edge_tables_.find(index) == edge_tables_.end()) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot delete properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot delete properties.");
  }
  edge_tables_.at(index).DeleteProperties(delete_property_names);
  return gs::Status::OK();
}

Status PropertyGraph::delete_vertex_type(const std::string& vertex_type_name,
                                         bool is_detach,
                                         bool error_on_conflict) {
  if (!schema_.contains_vertex_label(vertex_type_name)) {
    if (error_on_conflict) {
      LOG(ERROR) << "Vertex label[" << vertex_type_name << "] does not exists.";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Vertex label[" + vertex_type_name + "] does not exists.");
    } else {
      LOG(INFO) << "Vertex label[" << vertex_type_name
                << "] does not exist, skip deletion.";
      return gs::Status::OK();
    }
    return gs::Status::OK();
  }
  label_t v_label_id = schema_.get_vertex_label_id(vertex_type_name);
  schema_.delete_vertex_label(vertex_type_name);
  vertex_tables_[v_label_id].Drop();

  if (is_detach) {
    for (label_t i = 0; i < vertex_label_num_; i++) {
      for (label_t j = 0; j < edge_label_num_; j++) {
        if (schema_.exist(v_label_id, i, j)) {
          schema_.delete_edge_label(v_label_id, i, j);
          size_t index = schema_.generate_edge_label(v_label_id, i, j);

          auto edge_table = edge_tables_.find(index);
          if (edge_table != edge_tables_.end()) {
            edge_table->second.Drop();
            edge_tables_.erase(index);
          }
        }
        if (schema_.exist(i, v_label_id, j)) {
          schema_.delete_edge_label(i, v_label_id, j);
          size_t index = schema_.generate_edge_label(i, v_label_id, j);
          auto edge_table = edge_tables_.find(index);
          if (edge_table != edge_tables_.end()) {
            edge_table->second.Drop();
            edge_tables_.erase(index);
          }
        }
      }
    }
  }
  return gs::Status::OK();
}

Status PropertyGraph::delete_edge_type(const std::string& src_vertex_type,
                                       const std::string& dst_vertex_type,
                                       const std::string& edge_type,
                                       bool error_on_conflict) {
  if (!schema_.has_edge_label(src_vertex_type, dst_vertex_type, edge_type)) {
    if (error_on_conflict) {
      LOG(ERROR) << "Edge [" << edge_type << "] from [" << src_vertex_type
                 << "] to [" << dst_vertex_type << "] does not exist";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Edge [" + edge_type + "] from [" + src_vertex_type +
                        "] to [" + dst_vertex_type + "] does not exist");
    } else {
      LOG(INFO) << "Edge [" << edge_type << "] from [" << src_vertex_type
                << "] to [" << dst_vertex_type
                << "] does not exist, skip deletion.";
      return gs::Status::OK();
    }
    return gs::Status::OK();
  }
  label_t src_v_label = schema_.get_vertex_label_id(src_vertex_type);
  label_t dst_v_label = schema_.get_vertex_label_id(dst_vertex_type);
  label_t edge_label = schema_.get_edge_label_id(edge_type);
  schema_.delete_edge_label(src_v_label, dst_v_label, edge_label);
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  auto edge_table = edge_tables_.find(index);
  if (edge_table != edge_tables_.end()) {
    edge_table->second.Drop();
    edge_tables_.erase(index);
  }
  return gs::Status::OK();
}

Status PropertyGraph::batch_delete_vertices(const label_t& v_label_id,
                                            const std::vector<vid_t>& vids) {
  vertex_tables_.at(v_label_id).BatchDeleteVertices(vids);

  for (label_t i = 0; i < vertex_label_num_; i++) {
    for (label_t j = 0; j < edge_label_num_; j++) {
      if (schema_.has_edge_label(i, v_label_id, j)) {
        size_t index = schema_.generate_edge_label(i, v_label_id, j);
        edge_tables_.at(index).BatchDeleteVertices(false, vids);
      }
      if (schema_.has_edge_label(v_label_id, i, j)) {
        size_t index = schema_.generate_edge_label(v_label_id, i, j);
        edge_tables_.at(index).BatchDeleteVertices(true, vids);
      }
    }
  }

  return Status::OK();
}

Status PropertyGraph::batch_delete_edges(
    const label_t& src_v_label, const label_t& dst_v_label,
    const label_t& edge_label,
    std::vector<std::tuple<vid_t, vid_t>>& edges_vec) {
  std::string src_vertex_type = schema_.get_vertex_label_name(src_v_label);
  std::string dst_vertex_type = schema_.get_vertex_label_name(dst_v_label);
  std::string edge_type_name = schema_.get_edge_label_name(edge_label);
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  edge_tables_.at(index).BatchDeleteEdge(edges_vec);
  return Status::OK();
}

void PropertyGraph::DumpSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();

  LOG(INFO) << "Dump schema to file: " << get_schema_yaml_path();
  std::string filename = get_schema_yaml_path();
  auto schema_res = schema_.to_yaml();
  if (!schema_res.ok()) {
    LOG(ERROR) << "Failed to dump schema to yaml: "
               << schema_res.status().error_message();
    return;
  }
  write_yaml_file(schema_res.value(), filename);
}

void PropertyGraph::Open(const std::string& work_dir, int memory_level) {
  // copy work_dir to work_dir_
  memory_level_ = memory_level;
  work_dir_.assign(work_dir);
  std::string schema_file = schema_path(work_dir_);
  std::string snap_shot_dir{};
  bool build_empty_graph = false;
  if (std::filesystem::exists(schema_file)) {
    loadSchema(schema_file);
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    for (size_t i = 0; i < vertex_label_num_; i++) {
      std::string v_label_name = schema_.get_vertex_label_name(i);
      const auto& properties = schema_.get_vertex_properties(i);
      const auto& property_names = schema_.get_vertex_property_names(i);
      const auto& property_strategies =
          schema_.get_vertex_storage_strategies(v_label_name);
      vertex_tables_.emplace_back(
          v_label_name, std::get<0>(schema_.get_vertex_primary_key(i)[0]),
          property_names, properties, property_strategies);
    }
    snap_shot_dir = get_latest_snapshot(work_dir_);
  } else {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    for (size_t i = 0; i < vertex_label_num_; i++) {
      std::string v_label_name = schema_.get_vertex_label_name(i);
      const auto& properties = schema_.get_vertex_properties(i);
      const auto& property_names = schema_.get_vertex_property_names(i);
      const auto& property_strategies =
          schema_.get_vertex_storage_strategies(v_label_name);
      vertex_tables_.emplace_back(
          v_label_name, std::get<0>(schema_.get_vertex_primary_key(i)[0]),
          property_names, properties, property_strategies);
    }
    build_empty_graph = true;
    LOG(INFO) << "Schema file not found, build empty graph";

    // create snapshot dir
    std::string snap_shot_dir = snapshot_dir(work_dir_, 0);
    std::filesystem::create_directories(snap_shot_dir);
    set_snapshot_version(work_dir_, 0);
  }

  std::string tmp_dir_path = tmp_dir(work_dir_);

  if (std::filesystem::exists(tmp_dir_path)) {
    std::filesystem::remove_all(tmp_dir_path);
  }

  std::filesystem::create_directories(tmp_dir_path);

  std::vector<size_t> vertex_capacities(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    std::string v_label_name = schema_.get_vertex_label_name(i);

    vertex_tables_[i].Open(snap_shot_dir, tmp_dir_path, memory_level,
                           build_empty_graph);

    // We will reserve the at least 4096 slots for each vertex label
    size_t vertex_capacity =
        std::max(vertex_tables_[i].get_indexer().capacity(), (size_t) 4096);
    vertex_tables_[i].Reserve(vertex_capacity);

    vertex_capacities[i] = vertex_capacity;
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index =
            schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
        auto& properties =
            schema_.get_edge_properties(src_label, dst_label, edge_label);
        EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
            src_label, dst_label, edge_label);
        EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
            src_label, dst_label, edge_label);
        bool oe_mutable =
            schema_.outgoing_edge_mutable(src_label, dst_label, edge_label);
        bool ie_mutable =
            schema_.incoming_edge_mutable(src_label, dst_label, edge_label);

        auto& prop_names =
            schema_.get_edge_property_names(src_label, dst_label, edge_label);

        EdgeTable edge_table(src_label, dst_label, edge_label, oe_strategy,
                             ie_strategy, prop_names, properties, oe_mutable,
                             ie_mutable);

        edge_table.Open(snap_shot_dir, tmp_dir_path, memory_level,
                        vertex_capacities[src_label_i],
                        vertex_capacities[dst_label_i]);
        edge_table.Reserve(vertex_capacities[src_label_i],
                           vertex_capacities[dst_label_i]);
        edge_tables_.emplace(index, std::move(edge_table));
      }
    }
  }
  v_mutex_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    v_mutex_[i] = std::make_shared<std::mutex>();
  }
}

void PropertyGraph::Compact(uint32_t version) {
  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index =
            schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
        auto edge_table = edge_tables_.find(index);
        if (edge_table != edge_tables_.end()) {
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            edge_table->second.SortByEdgeData(version);
          }
        }
      }
    }
  }
}

void PropertyGraph::Dump(const std::string& work_dir, uint32_t version) {
  DumpSchema(schema_path(work_dir_));
  std::string snapshot_dir_path = snapshot_dir(work_dir, version);
  std::error_code errorCode;
  std::filesystem::create_directories(snapshot_dir_path, errorCode);
  if (errorCode) {
    std::stringstream ss;
    ss << "Failed to create snapshot directory: " << snapshot_dir_path << ", "
       << errorCode.message();
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  std::vector<size_t> vertex_num(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    if (!vertex_tables_[i].is_dropped()) {
      vertex_num[i] = vertex_tables_[i].lid_num();
      vertex_tables_[i].Dump(snapshot_dir_path);
    }
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    if (!schema_.vertex_label_valid(src_label_i)) {
      continue;
    }
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      if (!schema_.vertex_label_valid(dst_label_i)) {
        continue;
      }
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        if (!schema_.edge_label_valid(e_label_i)) {
          continue;
        }
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label) ||
            !schema_.edge_triplet_valid(src_label_i, dst_label_i, e_label_i)) {
          continue;
        }
        size_t index =
            schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
        auto edge_table = edge_tables_.find(index);
        if (edge_table != edge_tables_.end()) {
          edge_table->second.Reserve(vertex_num[src_label_i],
                                     vertex_num[dst_label_i]);
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            edge_table->second.SortByEdgeData(version + 1);
          }
          edge_table->second.Dump(snapshot_dir_path);
        }
      }
    }
  }
  set_snapshot_version(work_dir, version);
}

void PropertyGraph::IngestEdge(label_t src_label, vid_t src_lid,
                               label_t dst_label, vid_t dst_lid,
                               label_t edge_label, timestamp_t ts,
                               grape::OutArchive& arc, Allocator& alloc) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  edge_tables_.at(index).IngestEdge(src_lid, dst_lid, arc, ts, alloc);
}

void PropertyGraph::UpdateEdge(label_t src_label, vid_t src_lid,
                               label_t dst_label, vid_t dst_lid,
                               label_t edge_label, timestamp_t ts,
                               const Any& arc, Allocator& alloc) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  edge_tables_.at(index).UpdateEdge(src_lid, dst_lid, arc, ts, alloc);
}
const Schema& PropertyGraph::schema() const { return schema_; }

Schema& PropertyGraph::mutable_schema() { return schema_; }

vid_t PropertyGraph::lid_num(label_t vertex_label) const {
  return vertex_tables_[vertex_label].lid_num();
}

vid_t PropertyGraph::vertex_num(label_t vertex_label, timestamp_t ts) const {
  return vertex_tables_[vertex_label].vertex_num(ts);
}

bool PropertyGraph::is_valid_lid(label_t vertex_label, vid_t lid,
                                 timestamp_t ts) const {
  return vertex_tables_[vertex_label].is_valid_lid(lid, ts);
}

size_t PropertyGraph::edge_num(label_t src_label, label_t edge_label,
                               label_t dst_label) const {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  auto edge_table = edge_tables_.find(index);
  if (edge_table != edge_tables_.end()) {
    return edge_table->second.EdgeNum();
  } else {
    return 0;
  }
}

bool PropertyGraph::get_lid(label_t label, const Any& oid, vid_t& lid,
                            timestamp_t ts) const {
  return vertex_tables_[label].get_index(oid, lid, ts);
}

Any PropertyGraph::get_oid(label_t label, vid_t lid, timestamp_t ts) const {
  return vertex_tables_[label].get_oid(lid, ts);
}

vid_t PropertyGraph::add_vertex(label_t label, const Any& id, timestamp_t ts) {
  return vertex_tables_[label].add_vertex(id, ts);
}

vid_t PropertyGraph::add_vertex_safe(label_t label, const Any& id,
                                     timestamp_t ts) {
  return vertex_tables_[label].add_vertex_safe(id, ts);
}

std::shared_ptr<CsrConstEdgeIterBase> PropertyGraph::get_outgoing_edges(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

std::shared_ptr<CsrConstEdgeIterBase> PropertyGraph::get_incoming_edges(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

std::shared_ptr<CsrEdgeIterBase> PropertyGraph::get_outgoing_edges_mut(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

std::shared_ptr<CsrEdgeIterBase> PropertyGraph::get_incoming_edges_mut(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

std::string PropertyGraph::get_statistics_json() const {
  size_t vertex_count = 0;
  std::string ss = "\"vertex_type_statistics\": [\n";
  size_t vertex_label_num = schema_.vertex_label_num();
  for (size_t idx = 0; idx < vertex_label_num; ++idx) {
    ss += "{\n\"type_id\": " + std::to_string(idx) + ", \n";
    ss += "\"type_name\": \"" + schema_.get_vertex_label_name(idx) + "\", \n";
    size_t count = vertex_num(idx, MAX_TIMESTAMP);
    ss += "\"count\": " + std::to_string(count) + "\n}";
    vertex_count += count;
    if (idx != vertex_label_num - 1) {
      ss += ", \n";
    } else {
      ss += "\n";
    }
  }
  ss += "]\n";
  size_t edge_count = 0;

  size_t edge_label_num = schema_.edge_label_num();
  std::vector<std::thread> count_threads;
  std::unordered_map<uint32_t, size_t> edge_count_map;
  for (const auto& edge_table : edge_tables_) {
    edge_count_map.emplace(edge_table.first, edge_table.second.EdgeNum());
  }
  for (auto& t : count_threads) {
    t.join();
  }
  ss += ",\n";
  ss += "\"edge_type_statistics\": [";

  for (size_t edge_label = 0; edge_label < edge_label_num; ++edge_label) {
    const auto& edge_label_name = schema_.get_edge_label_name(edge_label);

    ss += "{\n\"type_id\": " + std::to_string(edge_label) + ", \n";
    ss += "\"type_name\": \"" + edge_label_name + "\", \n";
    ss += "\"vertex_type_pair_statistics\": [\n";
    bool first = true;
    std::string props_content{};
    for (size_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      const auto& src_label_name = schema_.get_vertex_label_name(src_label);
      for (size_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        const auto& dst_label_name = schema_.get_vertex_label_name(dst_label);
        size_t index =
            schema_.generate_edge_label(src_label, dst_label, edge_label);
        if (schema_.exist(src_label_name, dst_label_name, edge_label_name)) {
          if (!first) {
            ss += ",\n";
          }
          first = false;
          ss += "{\n\"source_vertex\" : \"" + src_label_name + "\", \n";
          ss += "\"destination_vertex\" : \"" + dst_label_name + "\", \n";
          ss +=
              "\"count\" : " + std::to_string(edge_count_map.at(index)) + "\n";
          edge_count += edge_count_map.at(index);
          ss += "}";
        }
      }
    }

    ss += "\n]\n}";
    if (edge_label != edge_label_num - 1) {
      ss += ", \n";
    } else {
      ss += "\n";
    }
  }
  ss += "]\n";
  LOG(INFO) << "In generateStatistics, vertex count: " << vertex_count
            << ", edge count: " << edge_count;
  std::string final_ss =
      "{\n\"total_vertex_count\": " + std::to_string(vertex_count) + ",\n";
  final_ss += "\"total_edge_count\": " + std::to_string(edge_count) + ",\n";
  final_ss += ss;
  final_ss += "}\n";
  return final_ss;
}

void PropertyGraph::generateStatistics() const {
  std::string filename = statisticsFilePath();

  {
    std::ofstream out(filename);
    if (!out.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }
    out << get_statistics_json();
    out.close();
  }
}

}  // namespace gs
