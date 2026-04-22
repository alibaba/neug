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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <algorithm>
#include <filesystem>
#include <set>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <tl/expected.hpp>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/types.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

PropertyGraph::PropertyGraph()
    : vertex_label_total_count_(0),
      edge_label_total_count_(0),
      memory_level_(MemoryLevel::kInMemory) {}

PropertyGraph::~PropertyGraph() { Clear(); }

void PropertyGraph::loadSchema(const std::string& schema_path) {
  std::ifstream in(schema_path);
  schema_.Deserialize(in);
}

void PropertyGraph::Clear() {
  vertex_tables_.clear();
  edge_tables_.clear();
  vertex_label_total_count_ = 0;
  edge_label_total_count_ = 0;
  schema_.Clear();
}

Status PropertyGraph::EnsureCapacity(label_t v_label, size_t capacity) {
  if (schema_.vertex_label_valid(v_label)) {
    auto old_cap = vertex_tables_[v_label].Capacity();
    if (capacity <= old_cap) {
      return neug::Status::OK();
    }
    auto v_new_cap = vertex_tables_[v_label].EnsureCapacity(capacity);
    for (label_t dst_label = 0; dst_label < vertex_label_total_count_;
         ++dst_label) {
      if (!schema_.vertex_label_valid(dst_label)) {
        continue;
      }
      for (label_t e_label = 0; e_label < edge_label_total_count_; ++e_label) {
        size_t index = schema_.generate_edge_label(v_label, dst_label, e_label);
        if (edge_tables_.count(index) > 0) {
          edge_tables_.at(index).EnsureCapacity(
              v_new_cap, vertex_tables_[dst_label].Capacity());
        }
        if (v_label != dst_label) {
          index = schema_.generate_edge_label(dst_label, v_label, e_label);
          if (edge_tables_.count(index) > 0) {
            edge_tables_.at(index).EnsureCapacity(
                vertex_tables_[dst_label].Capacity(), v_new_cap);
          }
        }
      }
    }
    return neug::Status::OK();
  } else {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex label does not exist.");
  }
}

Status PropertyGraph::EnsureCapacity(label_t src_label, label_t dst_label,
                                     label_t edge_label, size_t capacity) {
  if (!schema_.exist(src_label, dst_label, edge_label)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge label does not exist for the given source and "
                  "destination vertex labels.");
  }
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  if (edge_tables_.count(index) == 0) {
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Edge table for the given edge label triplet does not exist.");
  }
  size_t old_cap = edge_tables_.at(index).Capacity();
  if (capacity <= old_cap) {
    return neug::Status::OK();
  }
  edge_tables_.at(index).EnsureCapacity(capacity);
  return neug::Status::OK();
}

Status PropertyGraph::EnsureCapacity(label_t src_label, label_t dst_label,
                                     label_t edge_label, size_t src_v_cap,
                                     size_t dst_v_cap, size_t capacity) {
  if (!schema_.exist(src_label, dst_label, edge_label)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge label does not exist for the given source and "
                  "destination vertex labels.");
  }
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  if (edge_tables_.count(index) == 0) {
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Edge table for the given edge label triplet does not exist.");
  }
  size_t old_cap = edge_tables_.at(index).Capacity();
  if (capacity <= old_cap) {
    return neug::Status::OK();
  }
  edge_tables_.at(index).EnsureCapacity(src_v_cap, dst_v_cap, capacity);
  return neug::Status::OK();
}

Status PropertyGraph::BatchAddVertices(
    label_t v_label, std::shared_ptr<IRecordBatchSupplier> supplier) {
  assert(v_label < vertex_tables_.size());
  vertex_tables_[v_label].insert_vertices(supplier);
  return neug::Status::OK();
}

Status PropertyGraph::BatchAddEdges(
    label_t src_v_label, label_t dst_v_label, label_t e_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  schema_.ensure_vertex_label_valid(src_v_label);
  schema_.ensure_vertex_label_valid(dst_v_label);
  schema_.ensure_edge_triplet_valid(src_v_label, dst_v_label, e_label);
  size_t index = schema_.generate_edge_label(src_v_label, dst_v_label, e_label);
  assert(edge_tables_.count(index) > 0);
  edge_tables_.at(index).BatchAddEdges(
      vertex_tables_.at(src_v_label).get_indexer(),
      vertex_tables_.at(dst_v_label).get_indexer(), supplier);
  return neug::Status::OK();
}

Status PropertyGraph::CreateVertexType(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  if (schema_.contains_vertex_label(vertex_type_name)) {
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex label already exists.");
    } else {
      return Status(StatusCode::OK, "Vertex label already exists.");
    }
  }
  std::vector<std::string> property_names;
  std::vector<DataType> property_types;
  std::vector<Property> default_property_values;
  std::vector<std::tuple<DataType, std::string, size_t>> primary_keys;
  std::vector<int> primary_key_inds(primary_key_names.size(), -1);
  if (primary_key_inds.size() > 1) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Multi primary keys are not supported.");
  } else if (primary_key_inds.size() == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
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
          StatusCode::ERR_INVALID_ARGUMENT,
          "Primary key " + primary_key_name + " is not found in properties");
    }
    auto type_id = property_types[primary_key_inds[i]].id();
    if (type_id != DataTypeId::kInt64 && type_id != DataTypeId::kVarchar &&
        type_id != DataTypeId::kUInt64 && type_id != DataTypeId::kInt32 &&
        type_id != DataTypeId::kUInt32) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " should be int64/int32/uint64/uint32 or string/varchar";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Primary key " + primary_key_name +
                        " should be int64/int32/uint64/"
                        "uint32 or string/varchar");
    }
    primary_keys.emplace_back(property_types[primary_key_inds[i]],
                              property_names[primary_key_inds[i]],
                              primary_key_inds[i]);
    property_names.erase(property_names.begin() + primary_key_inds[i]);
    property_types.erase(property_types.begin() + primary_key_inds[i]);
    default_property_values.erase(default_property_values.begin() +
                                  primary_key_inds[i]);
  }

  std::string description;
  schema_.AddVertexLabel(vertex_type_name, property_types, property_names,
                         primary_keys, Schema::MAX_VNUM, description,
                         default_property_values);
  label_t vertex_label_id = schema_.get_vertex_label_id(vertex_type_name);
  if (vertex_label_id < vertex_tables_.size()) {
    auto& vtable = vertex_tables_[vertex_label_id];
    if (vtable.is_dropped()) {
      // Reuse a dropped vertex table
      auto new_v_table =
          VertexTable(schema_.get_vertex_schema(vertex_label_id));

      vtable.Swap(new_v_table);
    } else {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex label id conflict.");
    }
  } else {
    vertex_tables_.emplace_back(schema_.get_vertex_schema(vertex_label_id));
  }

  auto& vtable = vertex_tables_[vertex_label_id];
  vtable.Open(work_dir_, memory_level_);
  vtable.EnsureCapacity(4096);
  vertex_label_total_count_ = schema_.vertex_label_frontier();
  assert(vertex_tables_.size() == vertex_label_total_count_);

  while (v_mutex_.size() < vertex_label_total_count_) {
    v_mutex_.emplace_back(std::make_shared<std::mutex>());
  }

  LOG(INFO) << "CreateVertexType: vertex_type_name: " << vertex_type_name
            << ", vertex_label_id: " << static_cast<int32_t>(vertex_label_id)
            << ",properties " << property_names.size()
            << ", primary_key_names: " << primary_key_names[0];

  return neug::Status::OK();
}

Status PropertyGraph::CreateEdgeType(
    const std::string& src_vertex_type, const std::string& dst_vertex_type,
    const std::string& edge_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    bool error_on_conflict, EdgeStrategy oe_edge_strategy,
    EdgeStrategy ie_edge_strategy) {
  LOG(INFO) << "CreateEdgeType: src_vertex_type: " << src_vertex_type
            << ", dst_vertex_type: " << dst_vertex_type
            << ", edge_type_name: " << edge_type_name;
  if (!schema_.contains_vertex_label(src_vertex_type)) {
    LOG(ERROR) << "Source_vertex [" << src_vertex_type
               << "] does not exist in the graph.";
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Source_vertex [" + src_vertex_type + "] does not exist in the graph.");
  }
  if (!schema_.contains_vertex_label(dst_vertex_type)) {
    LOG(ERROR) << "Destination_vertex [" << dst_vertex_type
               << "] does not exist in the graph.";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Destination_vertex [" + dst_vertex_type +
                      "] does not exist in the graph.");
  }
  if (schema_.has_edge_label(src_vertex_type, dst_vertex_type,
                             edge_type_name)) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_vertex_type
               << "] to [" << dst_vertex_type << "] already exists";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge [" + edge_type_name + "] from [" + src_vertex_type +
                        "] to [" + dst_vertex_type + "] already exists");
    } else {
      return Status(StatusCode::OK, "Edge triplet already exists.");
    }
  }
  std::vector<std::string> property_names;
  std::vector<DataType> property_types;
  std::vector<Property> default_property_values;
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
  schema_.AddEdgeLabel(src_vertex_type, dst_vertex_type, edge_type_name,
                       property_types, property_names, cur_oe, cur_ie,
                       oe_mutable, ie_mutable, cur_sort_on_compaction,
                       description, default_property_values);
  edge_label_total_count_ = schema_.edge_label_frontier();

  label_t src_label_i = schema_.get_vertex_label_id(src_vertex_type);
  label_t dst_label_i = schema_.get_vertex_label_id(dst_vertex_type);
  label_t e_label_i = schema_.get_edge_label_id(edge_type_name);
  size_t index =
      schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);

  if (edge_tables_.count(index) > 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Edge label id conflict.");
  }
  EdgeTable edge_table(
      schema_.get_edge_schema(src_label_i, dst_label_i, e_label_i));
  edge_tables_.emplace(index, std::move(edge_table));
  auto src_v_capacity = std::max(
      vertex_tables_[src_label_i].get_indexer().capacity(), (size_t) 4096);
  auto dst_v_capacity = std::max(
      vertex_tables_[dst_label_i].get_indexer().capacity(), (size_t) 4096);
  edge_tables_.at(index).Open(work_dir_, memory_level_);
  edge_tables_.at(index).EnsureCapacity(src_v_capacity, dst_v_capacity, 4096);

  return neug::Status::OK();
}

Status PropertyGraph::AddVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  RETURN_IF_NOT_OK_CONFLICT(vertex_label_check(vertex_type_name),
                            error_on_conflict);
  std::vector<std::string> add_property_names;
  std::vector<DataType> add_property_types;
  std::vector<Property> add_default_property_values;
  for (size_t i = 0; i < add_properties.size(); i++) {
    auto [property_type, property_name, default_value] = add_properties[i];
    if (schema_.vertex_has_property(vertex_type_name, property_name)) {
      LOG(ERROR) << "Property [" << property_name
                 << "] already exists in vertex [" << vertex_type_name << "].";
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_ARGUMENT,
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
  schema_.AddVertexProperties(vertex_type_name, add_property_names,
                              add_property_types, add_default_property_values);
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);
  vertex_tables_[v_label].AddProperties(add_property_names, add_property_types,
                                        add_default_property_values);
  return neug::Status::OK();
}

Status PropertyGraph::AddEdgeProperties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  RETURN_IF_NOT_OK_CONFLICT(
      edge_triplet_check(src_type_name, dst_type_name, edge_type_name),
      error_on_conflict);
  std::vector<std::string> add_property_names;
  std::vector<DataType> add_property_types;
  std::vector<Property> add_default_property_values;
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
        return Status(StatusCode::ERR_INVALID_ARGUMENT, msg);
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

  schema_.AddEdgeProperties(src_type_name, dst_type_name, edge_type_name,
                            add_property_names, add_property_types,
                            add_default_property_values);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);
  if (!edge_tables_.count(index)) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot add properties.";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot add properties.");
  }

  auto& edge_table = edge_tables_.at(index);
  edge_table.AddProperties(add_property_names, add_property_types,
                           add_default_property_values);

  return neug::Status::OK();
}

Status PropertyGraph::RenameVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::pair<std::string, std::string>>& update_properties,
    bool error_on_conflict) {
  RETURN_IF_NOT_OK_CONFLICT(vertex_label_check(vertex_type_name),
                            error_on_conflict);
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
        return Status(StatusCode::ERR_INVALID_ARGUMENT, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    update_property_names.emplace_back(property_name);
    update_property_renames.emplace_back(property_rename);
  }
  schema_.RenameVertexProperties(vertex_type_name, update_property_names,
                                 update_property_renames);
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);
  vertex_tables_[v_label].RenameProperties(update_property_names,
                                           update_property_renames);
  return neug::Status::OK();
}

Status PropertyGraph::RenameEdgeProperties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::pair<std::string, std::string>>& update_properties,
    bool error_on_conflict) {
  RETURN_IF_NOT_OK_CONFLICT(
      edge_triplet_check(src_type_name, dst_type_name, edge_type_name),
      error_on_conflict);
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
        return Status(StatusCode::ERR_INVALID_ARGUMENT, msg);
      } else {
        return Status(StatusCode::OK, msg);
      }
    }
    update_property_names.emplace_back(property_name);
    update_property_renames.emplace_back(property_rename);
  }
  schema_.RenameEdgeProperties(src_type_name, dst_type_name, edge_type_name,
                               update_property_names, update_property_renames);
  label_t src_label = schema_.get_vertex_label_id(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id(dst_type_name);
  label_t e_label = schema_.get_edge_label_id(edge_type_name);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);
  if (edge_tables_.count(index) == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot rename properties.");
  }
  auto& edge_table = edge_tables_.at(index);

  edge_table.RenameProperties(update_property_names, update_property_renames);
  return neug::Status::OK();
}

Status PropertyGraph::delete_vertex_properties_check(
    const std::string& vertex_type_name, const std::vector<std::string>& props,
    bool error_on_conflict, std::vector<std::string>& valid_props) {
  RETURN_IF_NOT_OK_CONFLICT(vertex_label_check(vertex_type_name),
                            error_on_conflict);
  auto label_id = schema_.get_vertex_label_id(vertex_type_name);
  for (size_t i = 0; i < props.size(); i++) {
    auto property_name = props[i];
    if (!schema_.vertex_has_property_internal(label_id, property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in vertex [" + vertex_type_name +
                        "].";
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_ARGUMENT,
                      "Property [" + property_name +
                          "] does not exist in vertex [" + vertex_type_name +
                          "].");
      }
    }
    valid_props.emplace_back(property_name);
  }
  return neug::Status::OK();
}

Status PropertyGraph::DeleteVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  std::vector<std::string> delete_property_names;
  auto status =
      delete_vertex_properties_check(vertex_type_name, delete_properties,
                                     error_on_conflict, delete_property_names);
  if (!status.ok()) {
    return status;
  }
  label_t v_label = schema_.get_vertex_label_id(vertex_type_name);

  schema_.DeleteVertexProperties(vertex_type_name, delete_property_names);
  vertex_tables_[v_label].DeleteProperties(delete_property_names);
  return neug::Status::OK();
}

Status PropertyGraph::delete_edge_properties_check(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name, const std::vector<std::string>& props,
    bool error_on_conflict, std::vector<std::string>& valid_props) {
  RETURN_IF_NOT_OK_CONFLICT(
      edge_triplet_check(src_type_name, dst_type_name, edge_type_name),
      error_on_conflict);
  label_t src_label = schema_.get_vertex_label_id_internal(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id_internal(dst_type_name);
  label_t e_label = schema_.get_edge_label_id_internal(edge_type_name);

  for (size_t i = 0; i < props.size(); i++) {
    auto property_name = props[i];
    if (!schema_.edge_has_property_internal(src_label, dst_label, e_label,
                                            property_name)) {
      std::string msg = "Property [" + property_name +
                        "] does not exist in edge [" + edge_type_name +
                        "] from [" + src_type_name + "] to [" + dst_type_name +
                        "].";
      LOG(ERROR) << msg;
      if (error_on_conflict) {
        return Status(StatusCode::ERR_INVALID_ARGUMENT, msg);
      }
    }
    valid_props.emplace_back(property_name);
  }
  return neug::Status::OK();
}

Status PropertyGraph::DeleteEdgeProperties(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  std::vector<std::string> delete_property_names;
  RETURN_IF_NOT_OK_CONFLICT(
      delete_edge_properties_check(src_type_name, dst_type_name, edge_type_name,
                                   delete_properties, error_on_conflict,
                                   delete_property_names),
      error_on_conflict);
  label_t src_label = schema_.get_vertex_label_id_internal(src_type_name);
  label_t dst_label = schema_.get_vertex_label_id_internal(dst_type_name);
  label_t e_label = schema_.get_edge_label_id_internal(edge_type_name);
  size_t index = schema_.generate_edge_label(src_label, dst_label, e_label);
  // NOTE: We need to delete properties in edge table before updating schema,
  // since edge_tables_ use schema_ to determine delete logic.
  if (edge_tables_.count(index) == 0) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot delete properties.";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot delete properties.");
  }
  edge_tables_.at(index).DeleteProperties(delete_property_names);
  schema_.DeleteEdgeProperties(src_type_name, dst_type_name, edge_type_name,
                               delete_property_names);
  return neug::Status::OK();
}

Status PropertyGraph::DeleteVertexType(const std::string& vertex_type_name,
                                       bool error_on_conflict) {
  label_t v_label_id = schema_.get_vertex_label_id_internal(vertex_type_name);
  return DeleteVertexType(v_label_id, error_on_conflict);
}

Status PropertyGraph::DeleteVertexType(label_t v_label_id,
                                       bool error_on_conflict) {
  schema_.DeleteVertexLabel(v_label_id, false);
  vertex_tables_[v_label_id].Drop();

  for (label_t i = 0; i < vertex_label_total_count_; i++) {
    if (!schema_.vertex_label_valid(i)) {
      continue;
    }
    for (label_t j = 0; j < edge_label_total_count_; j++) {
      if (!schema_.edge_label_valid(j)) {
        continue;
      }
      if (schema_.exist(v_label_id, i, j)) {
        schema_.DeleteEdgeLabel(v_label_id, i, j);
        size_t index = schema_.generate_edge_label(v_label_id, i, j);
        if (edge_tables_.count(index) > 0) {
          edge_tables_.erase(index);
        }
      }
      if (schema_.exist(i, v_label_id, j)) {
        schema_.DeleteEdgeLabel(i, v_label_id, j);
        size_t index = schema_.generate_edge_label(i, v_label_id, j);
        if (edge_tables_.count(index) > 0) {
          edge_tables_.erase(index);
        }
      }
    }
  }

  return neug::Status::OK();
}

Status PropertyGraph::DeleteEdgeType(const std::string& src_vertex_type,
                                     const std::string& dst_vertex_type,
                                     const std::string& edge_type,
                                     bool error_on_conflict) {
  label_t src_v_label = schema_.get_vertex_label_id_internal(src_vertex_type);
  label_t dst_v_label = schema_.get_vertex_label_id_internal(dst_vertex_type);
  label_t edge_label = schema_.get_edge_label_id_internal(edge_type);
  return DeleteEdgeType(src_v_label, dst_v_label, edge_label,
                        error_on_conflict);
}
Status PropertyGraph::DeleteEdgeType(label_t src_v_label, label_t dst_v_label,
                                     label_t edge_label,
                                     bool error_on_conflict) {
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  schema_.DeleteEdgeLabel(src_v_label, dst_v_label, edge_label, false);
  if (edge_tables_.count(index) > 0) {
    edge_tables_.erase(index);
  }
  return neug::Status::OK();
}

Status PropertyGraph::BatchDeleteVertices(label_t v_label_id,
                                          const std::vector<vid_t>& vids) {
  vertex_tables_[v_label_id].BatchDeleteVertices(vids);

  std::set<vid_t> vids_set(vids.begin(), vids.end());

  for (label_t i = 0; i < vertex_label_total_count_; i++) {
    if (!schema_.vertex_label_valid(i)) {
      continue;
    }
    for (label_t j = 0; j < edge_label_total_count_; j++) {
      if (schema_.has_edge_label(i, v_label_id, j)) {
        size_t index = schema_.generate_edge_label(i, v_label_id, j);
        edge_tables_.at(index).BatchDeleteVertices({}, vids_set);
      }
      if (schema_.has_edge_label(v_label_id, i, j)) {
        size_t index = schema_.generate_edge_label(v_label_id, i, j);
        edge_tables_.at(index).BatchDeleteVertices(vids_set, {});
      }
    }
  }

  return Status::OK();
}

Status PropertyGraph::DeleteVertex(label_t label, const Property& oid,
                                   timestamp_t ts) {
  vid_t lid;
  if (!vertex_tables_.at(label).get_index(oid, lid, ts)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex oid does not exist.");
  }
  return DeleteVertex(label, lid, ts);
}

Status PropertyGraph::DeleteVertex(label_t label, vid_t lid, timestamp_t ts) {
  for (label_t i = 0; i < vertex_label_total_count_; i++) {
    if (!schema_.vertex_label_valid(i)) {
      continue;
    }
    for (label_t j = 0; j < edge_label_total_count_; j++) {
      if (schema_.has_edge_label(i, label, j)) {
        size_t index = schema_.generate_edge_label(i, label, j);
        assert(edge_tables_.count(index) > 0);
        edge_tables_.at(index).DeleteVertex(true, lid, ts);
      }
      if (schema_.has_edge_label(label, i, j)) {
        size_t index = schema_.generate_edge_label(label, i, j);
        assert(edge_tables_.count(index) > 0);
        edge_tables_.at(index).DeleteVertex(false, lid, ts);
      }
    }
  }
  vertex_tables_.at(label).DeleteVertex(lid, ts);
  return Status::OK();
}

Status PropertyGraph::DeleteEdge(label_t src_label, vid_t src_lid,
                                 label_t dst_label, vid_t dst_lid,
                                 label_t edge_label, int32_t oe_offset,
                                 int32_t ie_offset, timestamp_t ts) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  if (edge_tables_.count(index) == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge label does not exist.");
  }
  edge_tables_.at(index).DeleteEdge(src_lid, dst_lid, oe_offset, ie_offset, ts);
  return Status::OK();
}

Status PropertyGraph::BatchDeleteEdges(
    label_t src_v_label, label_t dst_v_label, label_t edge_label,
    const std::vector<std::tuple<vid_t, vid_t>>& edges_vec) {
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  std::vector<vid_t> src_vids, dst_vids;
  for (auto& edge : edges_vec) {
    src_vids.push_back(std::get<0>(edge));
    dst_vids.push_back(std::get<1>(edge));
  }
  edge_tables_.at(index).BatchDeleteEdges(src_vids, dst_vids);
  return Status::OK();
}

Status PropertyGraph::BatchDeleteEdges(
    label_t src_v_label, label_t dst_v_label, label_t edge_label,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  edge_tables_.at(index).BatchDeleteEdges(oe_edges, ie_edges);
  return Status::OK();
}

void PropertyGraph::Open(const Schema& schema, const std::string& work_dir,
                         MemoryLevel memory_level) {
  schema_ = schema;
  Open(work_dir, memory_level);
}

void PropertyGraph::Open(const std::string& work_dir,
                         MemoryLevel memory_level) {
  // copy work_dir to work_dir_
  memory_level_ = memory_level;
  work_dir_.assign(work_dir);
  std::string schema_file = schema_path(work_dir_);
  std::string checkpoint_dir_path = checkpoint_dir(work_dir_);
  if (std::filesystem::exists(schema_file)) {
    loadSchema(schema_file);
  } else {
    LOG(INFO) << "Schema file not found, build empty graph";
    std::filesystem::create_directories(checkpoint_dir_path);
  }
  vertex_label_total_count_ = schema_.vertex_label_frontier();
  edge_label_total_count_ = schema_.edge_label_frontier();
  for (size_t i = 0; i < vertex_label_total_count_; i++) {
    if (!schema_.vertex_label_valid(i)) {
      THROW_INTERNAL_EXCEPTION("Invalid vertex label id: " + std::to_string(i));
    }
    vertex_tables_.emplace_back(schema_.get_vertex_schema(i));
  }

  std::string tmp_dir_path = tmp_dir(work_dir_);

  if (std::filesystem::exists(tmp_dir_path)) {
    remove_directory(tmp_dir_path);
  }

  std::filesystem::create_directories(tmp_dir_path);

  std::vector<size_t> vertex_capacities(vertex_label_total_count_, 0);
  for (size_t i = 0; i < vertex_label_total_count_; ++i) {
    if (!schema_.vertex_label_valid(i)) {
      continue;
    }
    std::string v_label_name = schema_.get_vertex_label_name(i);

    vertex_tables_[i].Open(work_dir_, memory_level);
    // Case 1: Open from checkpoint, the capacity should be already reserved and
    // satisfied.
    // Case 2: Open from empty, Capacity should be the default minimum
    // capacity(4096)
    auto v_size = vertex_tables_[i].Size();
    vertex_tables_[i].EnsureCapacity(v_size < 4096 ? 4096
                                                   : v_size + v_size / 4);
    vertex_capacities[i] = vertex_tables_[i].Capacity();
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_total_count_;
       ++src_label_i) {
    if (!schema_.vertex_label_valid(src_label_i)) {
      continue;
    }
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_total_count_;
         ++dst_label_i) {
      if (!schema_.vertex_label_valid(dst_label_i)) {
        continue;
      }
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_total_count_;
           ++e_label_i) {
        if (!schema_.edge_label_valid(e_label_i)) {
          continue;
        }
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index =
            schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);

        EdgeTable edge_table(
            schema_.get_edge_schema(src_label_i, dst_label_i, e_label_i));
        edge_table.Open(work_dir_, memory_level_);
        auto e_size = edge_table.PropTableSize();
        size_t e_capacity = e_size < 4096 ? 4096 : e_size + (e_size + 4) / 5;
        edge_table.EnsureCapacity(vertex_capacities[src_label_i],
                                  vertex_capacities[dst_label_i], e_capacity);
        edge_tables_.emplace(index, std::move(edge_table));
      }
    }
  }
  v_mutex_.resize(vertex_label_total_count_);
  for (size_t i = 0; i < vertex_label_total_count_; ++i) {
    v_mutex_[i] = std::make_shared<std::mutex>();
  }
}

void PropertyGraph::compact_schema() {
  auto new_schema = schema_.Compact();
  std::vector<VertexTable> new_vertex_tables;
  std::unordered_map<uint32_t, EdgeTable> new_edge_tables;

  for (size_t old_v_label = 0; old_v_label != vertex_label_total_count_;
       ++old_v_label) {
    if (schema_.vertex_label_valid(old_v_label)) {
      auto src_name = schema_.get_vertex_label_name(old_v_label);
      size_t cur_new_label_id =
          new_schema.get_vertex_label_id_internal(src_name);
      new_vertex_tables.emplace_back(
          new_schema.get_vertex_schema(cur_new_label_id));
      new_vertex_tables.back().Swap(vertex_tables_[old_v_label]);
      // Update the handle to VertexSchema for the new vertex table.
      // The soft deleted properties should be removed physically in this step.
      new_vertex_tables.back().SetVertexSchema(
          new_schema.get_vertex_schema(cur_new_label_id));
    }
  }
  assert(new_vertex_tables.size() == new_schema.vertex_label_frontier());
  for (size_t old_src_label = 0; old_src_label != vertex_label_total_count_;
       ++old_src_label) {
    if (!schema_.vertex_label_valid(old_src_label)) {
      continue;
    }
    auto src_name = schema_.get_vertex_label_name(old_src_label);
    for (size_t old_dst_label = 0; old_dst_label != vertex_label_total_count_;
         ++old_dst_label) {
      if (!schema_.vertex_label_valid(old_dst_label)) {
        continue;
      }
      auto dst_name = schema_.get_vertex_label_name(old_dst_label);
      for (size_t old_e_label = 0; old_e_label != edge_label_total_count_;
           ++old_e_label) {
        if (!schema_.edge_label_valid(old_e_label) ||
            !schema_.exist(old_src_label, old_dst_label, old_e_label)) {
          continue;
        }
        auto e_name = schema_.get_edge_label_name(old_e_label);
        size_t old_index = schema_.generate_edge_label(
            old_src_label, old_dst_label, old_e_label);
        size_t new_src_label =
            new_schema.get_vertex_label_id_internal(src_name);
        size_t new_dst_label =
            new_schema.get_vertex_label_id_internal(dst_name);
        size_t new_e_label = new_schema.get_edge_label_id_internal(e_name);
        size_t new_index = new_schema.generate_edge_label(
            new_src_label, new_dst_label, new_e_label);
        new_edge_tables.emplace(
            new_index, EdgeTable(new_schema.get_edge_schema(
                           new_src_label, new_dst_label, new_e_label)));
        new_edge_tables.at(new_index).Swap(edge_tables_.at(old_index));
        new_edge_tables.at(new_index).SetEdgeSchema(new_schema.get_edge_schema(
            new_src_label, new_dst_label, new_e_label));
      }
    }
  }

  vertex_label_total_count_ = new_schema.vertex_label_frontier();
  edge_label_total_count_ = new_schema.edge_label_frontier();
  schema_ = new_schema;
  vertex_tables_.swap(new_vertex_tables);
  edge_tables_.swap(new_edge_tables);
  v_mutex_.resize(new_schema.vertex_label_frontier());
}

void PropertyGraph::Compact(bool compact_csr, float reserve_ratio,
                            timestamp_t ts) {
  /**
   * The compaction process includes two parts:
   * 1. Schema: remove the deleted properties and labels from
   *    schema.
   * 2. Data: for each vertex and edge table, remove the deleted
   *    data and compact the storage.
   *
   * Assume concurrency is controlled by the caller.
   */
  compact_schema();
  for (size_t src_label_i = 0; src_label_i != vertex_label_total_count_;
       ++src_label_i) {
    if (schema_.vertex_label_valid(src_label_i)) {
      vertex_tables_[src_label_i].Compact(ts);
    } else {
      continue;
    }
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_total_count_;
         ++dst_label_i) {
      if (!schema_.vertex_label_valid(dst_label_i)) {
        continue;
      }
      for (size_t e_label_i = 0; e_label_i != edge_label_total_count_;
           ++e_label_i) {
        if (schema_.edge_label_valid(e_label_i) &&
            schema_.exist(src_label_i, dst_label_i, e_label_i)) {
          size_t index =
              schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
          bool sort_on_compaction = schema_.get_sort_on_compaction(
              src_label_i, dst_label_i, e_label_i);
          if (edge_tables_.count(index) > 0) {
            auto& edge_table = edge_tables_.at(index);
            edge_table.Compact(compact_csr, sort_on_compaction, ts);
          }
        }
      }
    }
  }
  LOG(INFO) << "Compaction completed.";
}

void PropertyGraph::Dump(bool reopen) {
  // First dump to the  temp dir, then move to the checkpoint dir
  std::string target_dir = temp_checkpoint_dir(work_dir_);
  if (std::filesystem::exists(target_dir)) {
    remove_directory(target_dir);
  } else {
    std::filesystem::create_directories(target_dir);
  }

  std::error_code errorCode;
  std::filesystem::create_directories(target_dir, errorCode);
  if (errorCode) {
    std::stringstream ss;
    ss << "Failed to create snapshot directory: " << target_dir << ", "
       << errorCode.message();
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  std::vector<size_t> vertex_num(vertex_label_total_count_, 0);
  std::vector<size_t> vertex_capacity(vertex_label_total_count_, 0);
  for (size_t i = 0; i < vertex_label_total_count_; ++i) {
    if (!vertex_tables_[i].is_dropped()) {
      vertex_num[i] = vertex_tables_[i].LidNum();
      EnsureCapacity(
          i, vertex_num[i] < 4096 ? 4096 : vertex_num[i] + vertex_num[i] / 4);
      vertex_capacity[i] = vertex_tables_[i].Capacity();
      vertex_tables_[i].Dump(target_dir);
    }
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_total_count_;
       ++src_label_i) {
    if (!schema_.vertex_label_valid(src_label_i)) {
      continue;
    }
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_total_count_;
         ++dst_label_i) {
      if (!schema_.vertex_label_valid(dst_label_i)) {
        continue;
      }
      for (size_t e_label_i = 0; e_label_i != edge_label_total_count_;
           ++e_label_i) {
        if (!schema_.edge_label_valid(e_label_i) ||
            !schema_.exist(src_label_i, dst_label_i, e_label_i)) {
          continue;
        }
        size_t index =
            schema_.generate_edge_label(src_label_i, dst_label_i, e_label_i);
        if (edge_tables_.count(index) > 0) {
          auto& edge_table = edge_tables_.at(index);
          auto e_size = edge_table.PropTableSize();
          auto new_cap = e_size < 4096 ? 4096 : e_size + (e_size + 4) / 5;
          EnsureCapacity(src_label_i, dst_label_i, e_label_i,
                         vertex_capacity[src_label_i],
                         vertex_capacity[dst_label_i], new_cap);
          edge_table.Dump(target_dir);
        }
      }
    }
  }
  DumpSchema();
  copy_directory(target_dir, checkpoint_dir(work_dir_), true, true);
  remove_directory(target_dir);
  remove_directory(tmp_dir(work_dir_));
  remove_directory(wal_dir(work_dir_));
  LOG(INFO) << "Dump graph to " << checkpoint_dir(work_dir_);
  Clear();
  if (reopen) {
    Open(work_dir_, memory_level_);
  }
}

void PropertyGraph::DumpSchema() {
  auto _schema_path = schema_path(work_dir_);
  std::ofstream out(_schema_path);
  schema_.Serialize(out);
  out.flush();
  out.close();

  std::string filename = get_schema_yaml_path();
  auto schema_res = schema_.to_yaml();
  if (!schema_res) {
    LOG(ERROR) << "Failed to dump schema to yaml: "
               << schema_res.error().error_message();
    return;
  }
  if (!write_yaml_file(schema_res.value(), filename)) {
    THROW_IO_EXCEPTION("Failed to write schema yaml file: " + filename);
  }
  VLOG(1) << "Dump schema to yaml file: " << filename;
}

const Schema& PropertyGraph::schema() const { return schema_; }

Schema& PropertyGraph::mutable_schema() { return schema_; }

vid_t PropertyGraph::LidNum(label_t vertex_label) const {
  return vertex_tables_[vertex_label].LidNum();
}

vid_t PropertyGraph::VertexNum(label_t vertex_label, timestamp_t ts) const {
  return vertex_tables_[vertex_label].VertexNum(ts);
}

bool PropertyGraph::IsValidLid(label_t vertex_label, vid_t lid,
                               timestamp_t ts) const {
  return vertex_tables_[vertex_label].IsValidLid(lid, ts);
}

size_t PropertyGraph::EdgeNum(label_t src_label, label_t edge_label,
                              label_t dst_label) const {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  if (edge_tables_.count(index) > 0) {
    return edge_tables_.at(index).EdgeNum();
  } else {
    return 0;
  }
}

bool PropertyGraph::get_lid(label_t label, const Property& oid, vid_t& lid,
                            timestamp_t ts) const {
  return vertex_tables_[label].get_index(oid, lid, ts);
}

Property PropertyGraph::GetOid(label_t label, vid_t lid, timestamp_t ts) const {
  return vertex_tables_[label].GetOid(lid, ts);
}

Status PropertyGraph::AddVertex(label_t label, const Property& id,
                                const std::vector<Property>& props, vid_t& ret,
                                timestamp_t ts, bool insert_safe) {
  if (!vertex_tables_[label].AddVertex(id, props, ret, ts, insert_safe)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Fail to add vertex.");
  }
  return Status::OK();
}

int32_t PropertyGraph::AddEdge(label_t src_label, vid_t src_lid,
                               label_t dst_label, vid_t dst_lid,
                               label_t edge_label,
                               const std::vector<Property>& properties,
                               timestamp_t ts, Allocator& alloc,
                               bool insert_safe) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  if (edge_tables_.count(index) == 0) {
    LOG(ERROR) << "Edge table does not exist for edge label: " << edge_label;
    THROW_INVALID_ARGUMENT_EXCEPTION("Edge table does not exist for label <" +
                                     std::to_string(src_label) + ", " +
                                     std::to_string(dst_label) + ", " +
                                     std::to_string(edge_label) + ">");
  }
  return edge_tables_.at(index).AddEdge(src_lid, dst_lid, properties, ts, alloc,
                                        insert_safe);
}

Status PropertyGraph::UpdateVertexProperty(label_t v_label, vid_t vid,
                                           int32_t prop_id,
                                           const Property& value,
                                           timestamp_t ts) {
  assert(prop_id >= 0);
  assert(schema_.vertex_label_valid(v_label));
  if (!vertex_tables_[v_label].UpdateProperty(vid, prop_id, value, ts)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Fail to update vertex property.");
  }
  return neug::Status::OK();
}

Status PropertyGraph::UpdateEdgeProperty(label_t src_v_label, vid_t src_vid,
                                         label_t dst_v_label, vid_t dst_vid,
                                         label_t e_label, int32_t oe_offset,
                                         int32_t ie_offset, int32_t prop_id,
                                         const Property& value,
                                         timestamp_t ts) {
  assert(prop_id >= 0);
  assert(schema_.edge_label_valid(e_label));
  size_t index = schema_.generate_edge_label(src_v_label, dst_v_label, e_label);
  if (edge_tables_.count(index) == 0) {
    LOG(ERROR) << "Edge table does not exist for edge label: " << e_label;
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table does not exist for label <" +
                      std::to_string(src_v_label) + ", " +
                      std::to_string(dst_v_label) + ", " +
                      std::to_string(e_label) + ">");
  }
  edge_tables_.at(index).UpdateEdgeProperty(src_vid, dst_vid, oe_offset,
                                            ie_offset, prop_id, value, ts);
  return neug::Status::OK();
}

std::string PropertyGraph::get_statistics_json() const {
  // Generate json string using rapidjson document
  rapidjson::Document document;
  document.SetObject();
  rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
  size_t vertex_count = 0;
  rapidjson::Value vertex_type_statistics(rapidjson::kArrayType);
  auto label_ids = schema_.get_vertex_label_ids();
  for (const auto& label_id : label_ids) {
    auto label_name = schema_.get_vertex_label_name(label_id);
    rapidjson::Value vertex_type_stat(rapidjson::kObjectType);
    vertex_type_stat.AddMember(
        "type_id", rapidjson::Value().SetUint64(label_id), allocator);
    vertex_type_stat.AddMember(
        "type_name",
        rapidjson::Value().SetString(label_name.c_str(), allocator), allocator);
    size_t count = VertexNum(label_id, MAX_TIMESTAMP);
    vertex_type_stat.AddMember("count", rapidjson::Value().SetUint64(count),
                               allocator);
    vertex_count += count;
    vertex_type_statistics.PushBack(vertex_type_stat, allocator);
  }
  document.AddMember("vertex_type_statistics", vertex_type_statistics,
                     allocator);
  size_t edge_count = 0;
  rapidjson::Value edge_type_statistics(rapidjson::kArrayType);
  std::unordered_map<uint32_t, size_t> edge_count_map;
  for (const auto& iter : edge_tables_) {
    edge_count_map.emplace(iter.first, iter.second.EdgeNum());
  }
  for (label_t edge_label = 0; edge_label < edge_label_total_count_;
       ++edge_label) {
    if (!schema_.edge_label_valid(edge_label)) {
      continue;
    }
    auto edge_label_name = schema_.get_edge_label_name(edge_label);
    rapidjson::Value edge_type_stat(rapidjson::kObjectType);
    edge_type_stat.AddMember(
        "type_id", rapidjson::Value().SetUint64(edge_label), allocator);
    edge_type_stat.AddMember(
        "type_name",
        rapidjson::Value().SetString(edge_label_name.c_str(), allocator),
        allocator);
    rapidjson::Value vertex_type_pair_statistics(rapidjson::kArrayType);
    for (label_t src_label = 0; src_label < vertex_label_total_count_;
         ++src_label) {
      if (!schema_.vertex_label_valid(src_label)) {
        continue;
      }
      auto src_label_name = schema_.get_vertex_label_name(src_label);
      for (label_t dst_label = 0; dst_label < vertex_label_total_count_;
           ++dst_label) {
        if (!schema_.vertex_label_valid(dst_label)) {
          continue;
        }
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        auto dst_label_name = schema_.get_vertex_label_name(dst_label);
        auto edge_triplet_id =
            schema_.generate_edge_label(src_label, dst_label, edge_label);
        assert(edge_count_map.find(edge_triplet_id) != edge_count_map.end());
        size_t value = edge_count_map.at(edge_triplet_id);

        rapidjson::Value vertex_type_pair_stat(rapidjson::kObjectType);
        vertex_type_pair_stat.AddMember(
            "source_vertex",
            rapidjson::Value().SetString(src_label_name.c_str(), allocator),
            allocator);
        vertex_type_pair_stat.AddMember(
            "destination_vertex",
            rapidjson::Value().SetString(dst_label_name.c_str(), allocator),
            allocator);
        vertex_type_pair_stat.AddMember(
            "count", rapidjson::Value().SetUint64(value), allocator);
        edge_count += value;
        vertex_type_pair_statistics.PushBack(vertex_type_pair_stat, allocator);
      }
    }
    if (vertex_type_pair_statistics.Empty()) {
      continue;
    } else {
      edge_type_stat.AddMember("vertex_type_pair_statistics",
                               vertex_type_pair_statistics, allocator);
      edge_type_statistics.PushBack(edge_type_stat, allocator);
    }
  }

  document.AddMember("edge_type_statistics", edge_type_statistics, allocator);
  document.AddMember("total_vertex_count",
                     rapidjson::Value().SetUint64(vertex_count), allocator);
  document.AddMember("total_edge_count",
                     rapidjson::Value().SetUint64(edge_count), allocator);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document.Accept(writer);
  return buffer.GetString();
}

Status PropertyGraph::edge_triplet_check(const std::string& src_type_name,
                                         const std::string& dst_type_name,
                                         const std::string& edge_type_name) {
  if (!schema_.exist(src_type_name, dst_type_name, edge_type_name)) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name << "] does not exist";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name + "] does not exist");
  }
  return neug::Status::OK();
}

Status PropertyGraph::edge_triplet_exist(const std::string& src_type_name,
                                         const std::string& dst_type_name,
                                         const std::string& edge_type_name) {
  auto ret =
      schema_.has_edge_label(src_type_name, dst_type_name, edge_type_name);
  if (!ret) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name << "] does not exist";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name + "] does not exist");
  }
  return neug::Status::OK();
}

Status PropertyGraph::vertex_label_check(const std::string& vertex_type_name) {
  if (!schema_.contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex label[" << vertex_type_name << "] does not exists.";
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex label[" + vertex_type_name + "] does not exists.");
  }
  return neug::Status::OK();
}

}  // namespace neug
