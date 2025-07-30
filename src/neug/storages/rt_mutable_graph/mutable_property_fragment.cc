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

#include "neug/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <assert.h>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "libgrape-lite/grape/io/local_io_adaptor.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "libgrape-lite/grape/types.h"
#include "libgrape-lite/grape/util.h"
#include "neug/storages/rt_mutable_graph/csr/csr_base.h"
#include "neug/storages/rt_mutable_graph/csr/immutable_csr.h"
#include "neug/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "neug/storages/rt_mutable_graph/csr/nbr.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/types.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/yaml_utils.h"

namespace gs {

void delete_dual_csr(const std::string& oe_name, const std::string& ie_name,
                     const std::string& edata_name,
                     const std::string& snapshot_dir) {
  std::vector<std::string> delete_file;
}

inline DualCsrBase* create_csr(EdgeStrategy oes, EdgeStrategy ies,
                               const std::vector<PropertyType>& properties,
                               bool oe_mutable, bool ie_mutable,
                               const std::vector<std::string>& prop_names) {
  if (properties.empty()) {
    return new DualCsr<grape::EmptyType>(oes, ies, oe_mutable, ie_mutable);
  } else if (properties.size() == 1) {
    if (properties[0] == PropertyType::kBool) {
      return new DualCsr<bool>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt32) {
      return new DualCsr<int32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt32) {
      return new DualCsr<uint32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDate) {
      return new DualCsr<Date>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt64) {
      return new DualCsr<int64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt64) {
      return new DualCsr<uint64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDouble) {
      return new DualCsr<double>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kFloat) {
      return new DualCsr<float>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
      return new DualCsr<std::string_view>(
          oes, ies, properties[0].additional_type_info.max_length, oe_mutable,
          ie_mutable);
    } else if (properties[0] == PropertyType::kStringView) {
      return new DualCsr<std::string_view>(
          oes, ies, gs::PropertyType::GetStringDefaultMaxLength(), oe_mutable,
          ie_mutable);
    } else if (properties[0] == PropertyType::kTimestamp) {
      return new DualCsr<TimeStamp>(oes, ies, oe_mutable, ie_mutable);
    }
  } else {
    // TODO: fix me, storage strategy not set
    return new DualCsr<RecordView>(oes, ies, prop_names, properties, {},
                                   oe_mutable, ie_mutable);
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

MutablePropertyFragment::MutablePropertyFragment()
    : vertex_label_num_(0), edge_label_num_(0) {}

MutablePropertyFragment::~MutablePropertyFragment() {
  std::vector<size_t> degree_list(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    degree_list[i] = lf_indexers_[i].size();
    vertex_data_[i].resize(degree_list[i]);
  }
  for (size_t src_label = 0; src_label != vertex_label_num_; ++src_label) {
    for (size_t dst_label = 0; dst_label != vertex_label_num_; ++dst_label) {
      for (size_t e_label = 0; e_label != edge_label_num_; ++e_label) {
        size_t index =
            schema_.generate_edge_label(src_label, dst_label, e_label);
        auto dual_csr = dual_csr_map_.find(index);
        if (dual_csr != dual_csr_map_.end() && dual_csr->second != NULL) {
          dual_csr->second->Resize(degree_list[src_label],
                                   degree_list[dst_label]);
          delete dual_csr->second;
        }
      }
    }
  }
}

void MutablePropertyFragment::loadSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);
}

void MutablePropertyFragment::Clear() {
  for (auto pair : dual_csr_map_) {
    if (pair.second != NULL) {
      delete pair.second;
    }
  }
  lf_indexers_.clear();
  vertex_data_.clear();
  ie_map_.clear();
  oe_map_.clear();
  dual_csr_map_.clear();
  vertex_label_num_ = 0;
  edge_label_num_ = 0;
  schema_.Clear();
}

Status MutablePropertyFragment::create_vertex_type(
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
  std::vector<StorageStrategy> strategies;
  std::string description;
  size_t max_num = ((size_t) 1) << 8;
  schema_.add_vertex_label(vertex_type_name, property_types, property_names,
                           primary_keys, strategies, max_num, description);
  label_t vertex_label_id = schema_.get_vertex_label_id(vertex_type_name);
  lf_indexers_.resize(schema_.vertex_label_num());
  vertex_tomb_.emplace_back(std::make_shared<Bitset>());
  lf_indexers_[vertex_label_id].init(
      std::get<0>(schema_.get_vertex_primary_key(vertex_label_id)[0]));
  // TODO: use memory level.
  lf_indexers_[vertex_label_id].open_in_memory(
      snapshot_dir(work_dir_, 0) + "/" + IndexerType::prefix() +
      vertex_map_prefix(vertex_type_name));
  size_t vertex_capacity =
      std::max(lf_indexers_[vertex_label_id].capacity(), (size_t) 4096);
  if (vertex_capacity > lf_indexers_[vertex_label_id].capacity()) {
    lf_indexers_[vertex_label_id].reserve(vertex_capacity);
  }
  vertex_data_.resize(schema_.vertex_label_num());
  vertex_data_[vertex_label_id].open_in_memory(
      // TODO: FIXME, assume version 0
      vertex_table_prefix(vertex_type_name), snapshot_dir(work_dir_, 0),
      schema_.get_vertex_property_names(vertex_label_id),
      schema_.get_vertex_properties(vertex_label_id),
      schema_.get_vertex_storage_strategies(vertex_type_name));

  // Dump schema
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  vertex_data_[vertex_label_id].dump_without_close(
      vertex_table_prefix(vertex_type_name), snapshot_dir(work_dir_, 0));
  lf_indexers_[vertex_label_id].dump_without_close(
      LFIndexer<vid_t>::prefix() + "_" + vertex_map_prefix(vertex_type_name),
      snapshot_dir(work_dir_, 0));
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

Status MutablePropertyFragment::create_edge_type(
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
  DualCsrBase* dual_csr = create_csr(oe_strategy, ie_strategy, property_types,
                                     oe_mutable, ie_mutable, property_names);
  dual_csr_map_.emplace(index, dual_csr);
  ie_map_.emplace(index, dual_csr->GetInCsr());
  oe_map_.emplace(index, dual_csr->GetOutCsr());
  auto src_v_capacity =
      std::max(lf_indexers_[src_label_i].capacity(), (size_t) 4096);
  auto dst_v_capacity =
      std::max(lf_indexers_[dst_label_i].capacity(), (size_t) 4096);
  dual_csr->OpenInMemory(
      oe_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
      ie_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
      edata_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
      snapshot_dir(work_dir_, 0), src_v_capacity, dst_v_capacity);

  dual_csr->Resize(src_v_capacity, dst_v_capacity);
  init_state_.emplace(index, false);

  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::add_vertex_properties(
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
  auto& vertex_data = vertex_data_[v_label];
  vertex_data.add_columns(add_property_names, add_property_types);
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::add_edge_properties(
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
  if (!dynamic_cast<DualCsr<RecordView>*>(dual_csr_map_.at(index))) {
    change_csr_data_type_to_record_view(src_type_name, dst_type_name,
                                        edge_type_name);
  }
  schema_.add_edge_properties(src_type_name, dst_type_name, edge_type_name,
                              add_property_names, add_property_types,
                              add_default_property_values);
  auto dual_csr = dual_csr_map_.at(index);
  if (dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot add properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot add properties.");
  }

  auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr);
  if (casted_dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not support adding properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not support adding properties.");
  }
  casted_dual_csr->add_properties(add_property_names, add_property_types);
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::rename_vertex_properties(
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
  auto& vertex_data = vertex_data_[v_label];
  for (size_t i = 0; i < update_property_names.size(); i++) {
    vertex_data.rename_column(update_property_names[i],
                              update_property_renames[i]);
  }
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::rename_edge_properties(
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
  auto dual_csr = dual_csr_map_.at(index);
  if (dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot rename properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot rename properties.");
  }
  auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr);
  if (casted_dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not support renaming properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not support renaming properties.");
  }
  casted_dual_csr->rename_properties(update_property_names,
                                     update_property_renames);
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::delete_vertex_properties(
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
  auto& vertex_data = vertex_data_[v_label];
  for (const auto& property_name : delete_property_names) {
    vertex_data.delete_column(property_name);
  }
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::delete_edge_properties(
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
  auto dual_csr = dual_csr_map_.at(index);
  if (dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot delete properties.";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Edge [" + edge_type_name + "] from [" + src_type_name +
                      "] to [" + dst_type_name +
                      "] does not exist, cannot delete properties.");
  }
  auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr);
  if (casted_dual_csr == NULL) {
    std::vector<vid_t> src_vids, dst_vids;
    size_t edge_num = dual_csr->EdgeNum();
    auto oe_csr = oe_map_.at(index);
    src_vids.reserve(edge_num);
    dst_vids.reserve(edge_num);
    for (vid_t i = 0; i < lid_num(src_label); i++) {
      if (!vertex_tomb_[src_label]->get(i)) {
        auto e_iter = oe_csr->edge_iter(i);
        while (e_iter->is_valid()) {
          src_vids.emplace_back(i);
          dst_vids.emplace_back(e_iter->get_neighbor());
          e_iter->next();
        }
      }
    }
    std::vector<std::atomic<int32_t>> ie_degree(lid_num(dst_label)),
        oe_degree(lid_num(src_label));
    for (size_t idx = 0; idx < ie_degree.size(); ++idx) {
      ie_degree[idx].store(0);
    }
    for (size_t idx = 0; idx < oe_degree.size(); ++idx) {
      oe_degree[idx].store(0);
    }
    std::vector<std::vector<std::pair<vid_t, vid_t>>> edges(
        std::thread::hardware_concurrency());
    std::atomic<vid_t> vid(0);
    vid_t src_lid_num = lid_num(src_label);
    std::vector<std::thread> work_threads;
    for (size_t idx = 0; idx < std::thread::hardware_concurrency(); ++idx) {
      work_threads.emplace_back(
          [&](int idx) {
            while (true) {
              vid_t vid_i = vid.fetch_add(1);
              if (vid_i >= src_lid_num) {
                break;
              }
              if (vertex_tomb_[src_label]->get(vid_i)) {
                continue;
              }
              auto e_iter = oe_csr->edge_iter(vid_i);
              while (e_iter->is_valid()) {
                ie_degree[e_iter->get_neighbor()]++;
                oe_degree[vid_i]++;
                edges[idx].emplace_back(
                    std::make_pair(vid_i, e_iter->get_neighbor()));
                e_iter->next();
              }
            }
          },
          idx);
    }
    for (auto& t : work_threads) {
      t.join();
    }
    dual_csr->Close();
    // delete_dual_csr(oe_prefix(src_type_name, dst_type_name, edge_type_name),
    //                 ie_prefix(src_type_name, dst_type_name, edge_type_name),
    //                 edata_prefix(src_type_name, dst_type_name,
    //                 edge_type_name), snapshot_dir(work_dir_, 0));
    bool oe_mutable = true, ie_mutable = true;
    EdgeStrategy oe_strategy = EdgeStrategy::kMultiple;
    EdgeStrategy ie_strategy = EdgeStrategy::kMultiple;
    delete dual_csr;
    dual_csr = new DualCsr<grape::EmptyType>(oe_strategy, ie_strategy,
                                             oe_mutable, ie_mutable);
    auto src_v_capacity =
        std::max(lf_indexers_[src_label].capacity(), (size_t) 4096);
    auto dst_v_capacity =
        std::max(lf_indexers_[dst_label].capacity(), (size_t) 4096);
    dual_csr->OpenInMemory(
        oe_prefix(src_type_name, dst_type_name, edge_type_name),
        ie_prefix(src_type_name, dst_type_name, edge_type_name),
        edata_prefix(src_type_name, dst_type_name, edge_type_name),
        snapshot_dir(work_dir_, 0), src_v_capacity, dst_v_capacity);

    std::vector<int32_t> ie_deg(ie_degree.size());
    std::vector<int32_t> oe_deg(oe_degree.size());
    for (size_t idx = 0; idx < ie_deg.size(); ++idx) {
      ie_deg[idx] = ie_degree[idx];
    }
    for (size_t idx = 0; idx < oe_deg.size(); ++idx) {
      oe_deg[idx] = oe_degree[idx];
    }

    dual_csr->BatchInit(
        oe_prefix(src_type_name, dst_type_name, edge_type_name),
        ie_prefix(src_type_name, dst_type_name, edge_type_name),
        edata_prefix(src_type_name, dst_type_name, edge_type_name),
        tmp_dir(work_dir_), oe_deg, ie_deg);
    auto new_csr = dynamic_cast<DualCsr<grape::EmptyType>*>(dual_csr);
    std::vector<std::thread> edge_threads;
    for (size_t i = 0; i < edges.size(); ++i) {
      edge_threads.emplace_back(
          [&](int idx) {
            for (auto& edge : edges[idx]) {
              new_csr->BatchPutEdge(edge.first, edge.second,
                                    grape::EmptyType());
            }
          },
          i);
    }
    for (auto& t : edge_threads) {
      t.join();
    }
    dual_csr_map_.erase(index);
    ie_map_.erase(index);
    oe_map_.erase(index);
    dual_csr_map_.insert({index, dual_csr});
    ie_map_.insert({index, dual_csr->GetInCsr()});
    oe_map_.insert({index, dual_csr->GetOutCsr()});

    dual_csr->Dump(oe_prefix(src_type_name, dst_type_name, edge_type_name),
                   ie_prefix(src_type_name, dst_type_name, edge_type_name),
                   edata_prefix(src_type_name, dst_type_name, edge_type_name),
                   snapshot_dir(work_dir_, 0));

  } else {
    casted_dual_csr->delete_properties(delete_property_names);
  }
  DumpSchema(schema_path(work_dir_));
  dumpSchema();
  return gs::Status::OK();
}

Status MutablePropertyFragment::delete_vertex_type(
    const std::string& vertex_type_name, bool is_detach,
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
  lf_indexers_[v_label_id].close();
  vertex_data_[v_label_id].close();

  if (is_detach) {
    for (label_t i = 0; i < vertex_label_num_; i++) {
      for (label_t j = 0; j < edge_label_num_; j++) {
        if (schema_.exist(v_label_id, i, j)) {
          schema_.delete_edge_label(v_label_id, i, j);
          size_t index = schema_.generate_edge_label(v_label_id, i, j);
          ie_map_.erase(index);
          oe_map_.erase(index);
          auto dual_csr = dual_csr_map_.find(index);
          if (dual_csr != dual_csr_map_.end()) {
            delete dual_csr->second;
            dual_csr_map_.erase(index);
          }
        }
        if (schema_.exist(i, v_label_id, j)) {
          schema_.delete_edge_label(i, v_label_id, j);
          size_t index = schema_.generate_edge_label(i, v_label_id, j);
          ie_map_.erase(index);
          oe_map_.erase(index);
          auto dual_csr = dual_csr_map_.find(index);
          if (dual_csr != dual_csr_map_.end()) {
            delete dual_csr->second;
            dual_csr_map_.erase(index);
          }
        }
      }
    }
  }
  return gs::Status::OK();
}

Status MutablePropertyFragment::delete_edge_type(
    const std::string& src_vertex_type, const std::string& dst_vertex_type,
    const std::string& edge_type, bool error_on_conflict) {
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
  ie_map_.erase(index);
  oe_map_.erase(index);
  auto dual_csr = dual_csr_map_.find(index);
  if (dual_csr != dual_csr_map_.end()) {
    delete dual_csr->second;
    dual_csr_map_.erase(index);
  }
  return gs::Status::OK();
}

Status MutablePropertyFragment::batch_delete_vertices(
    const label_t& v_label_id, const std::vector<vid_t>& vids) {
  for (auto v : vids) {
    if (v < lf_indexers_[v_label_id].size() &&
        !vertex_tomb_[v_label_id]->get(v)) {
      auto oid = lf_indexers_[v_label_id].get_key(v);
      lf_indexers_[v_label_id].remove(oid);
      vertex_tomb_[v_label_id]->set(v);
    }
  }

  for (label_t i = 0; i < vertex_label_num_; i++) {
    for (label_t j = 0; j < edge_label_num_; j++) {
      if (schema_.has_edge_label(i, v_label_id, j)) {
        size_t index = schema_.generate_edge_label(i, v_label_id, j);
        dual_csr_map_.at(index)->BatchDeleteVertices(false, vids);
      }
      if (schema_.has_edge_label(v_label_id, i, j)) {
        size_t index = schema_.generate_edge_label(v_label_id, i, j);
        dual_csr_map_.at(index)->BatchDeleteVertices(true, vids);
      }
    }
  }

  return Status::OK();
}

Status MutablePropertyFragment::batch_delete_edges(
    const label_t& src_v_label, const label_t& dst_v_label,
    const label_t& edge_label,
    std::vector<std::tuple<vid_t, vid_t>>& edges_vec) {
  std::string src_vertex_type = schema_.get_vertex_label_name(src_v_label);
  std::string dst_vertex_type = schema_.get_vertex_label_name(dst_v_label);
  std::string edge_type_name = schema_.get_edge_label_name(edge_label);
  size_t index =
      schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);
  dual_csr_map_.at(index)->BatchDeleteEdge(edges_vec);
  return Status::OK();
}

void MutablePropertyFragment::DumpSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();
}

void MutablePropertyFragment::Open(const std::string& work_dir,
                                   int memory_level) {
  // copy work_dir to work_dir_
  work_dir_.assign(work_dir);
  std::string schema_file = schema_path(work_dir_);
  std::string snap_shot_dir{};
  bool build_empty_graph = false;
  if (std::filesystem::exists(schema_file)) {
    loadSchema(schema_file);
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    lf_indexers_.resize(vertex_label_num_);
    for (size_t i = 0; i < vertex_label_num_; i++) {
      vertex_tomb_.emplace_back(std::make_shared<Bitset>());
    }
    snap_shot_dir = get_latest_snapshot(work_dir_);
  } else {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    lf_indexers_.resize(vertex_label_num_);
    for (size_t i = 0; i < vertex_label_num_; i++) {
      vertex_tomb_.emplace_back(std::make_shared<Bitset>());
    }
    build_empty_graph = true;
    LOG(INFO) << "Schema file not found, build empty graph";
    for (size_t i = 0; i < vertex_label_num_; ++i) {
      lf_indexers_[i].init(std::get<0>(schema_.get_vertex_primary_key(i)[0]));
    }
    // create snapshot dir
    std::string snap_shot_dir = snapshot_dir(work_dir_, 0);
    std::filesystem::create_directories(snap_shot_dir);
    set_snapshot_version(work_dir_, 0);
  }

  vertex_data_.resize(vertex_label_num_);
  std::string tmp_dir_path = tmp_dir(work_dir_);

  if (std::filesystem::exists(tmp_dir_path)) {
    std::filesystem::remove_all(tmp_dir_path);
  }

  std::filesystem::create_directories(tmp_dir_path);

  std::vector<size_t> vertex_capacities(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    std::string v_label_name = schema_.get_vertex_label_name(i);

    if (memory_level == 0) {
      lf_indexers_[i].open(
          IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name),
          snap_shot_dir, tmp_dir_path);
      vertex_data_[i].open(vertex_table_prefix(v_label_name), snap_shot_dir,
                           tmp_dir_path, schema_.get_vertex_property_names(i),
                           schema_.get_vertex_properties(i),
                           schema_.get_vertex_storage_strategies(v_label_name));
      if (!build_empty_graph) {
        vertex_data_[i].copy_to_tmp(vertex_table_prefix(v_label_name),
                                    snap_shot_dir, tmp_dir_path);
      }
    } else if (memory_level == 1) {
      lf_indexers_[i].open_in_memory(snap_shot_dir + "/" +
                                     IndexerType::prefix() + "_" +
                                     vertex_map_prefix(v_label_name));
      vertex_data_[i].open_in_memory(
          vertex_table_prefix(v_label_name), snap_shot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name));
    } else if (memory_level == 2) {
      lf_indexers_[i].open_with_hugepages(snap_shot_dir + "/" +
                                              IndexerType::prefix() + "_" +
                                              vertex_map_prefix(v_label_name),
                                          false);
      vertex_data_[i].open_with_hugepages(
          vertex_table_prefix(v_label_name), snap_shot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name), false);
    } else {
      assert(memory_level == 3);
      lf_indexers_[i].open_with_hugepages(snap_shot_dir + "/" +
                                              IndexerType::prefix() + "_" +
                                              vertex_map_prefix(v_label_name),
                                          true);
      vertex_data_[i].open_with_hugepages(
          vertex_table_prefix(v_label_name), snap_shot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name), true);
    }
    vertex_tomb_[i]->resize(lf_indexers_[i].size());

    // We will reserve the at least 4096 slots for each vertex label
    size_t vertex_capacity =
        std::max(lf_indexers_[i].capacity(), (size_t) 4096);
    if (vertex_capacity > lf_indexers_[i].capacity()) {
      lf_indexers_[i].reserve(vertex_capacity);
    }
    vertex_data_[i].resize(vertex_capacity);
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

        auto dual_csr = create_csr(oe_strategy, ie_strategy, properties,
                                   oe_mutable, ie_mutable, prop_names);
        dual_csr_map_.emplace(index, dual_csr);
        ie_map_.emplace(index, dual_csr->GetInCsr());
        oe_map_.emplace(index, dual_csr->GetOutCsr());
        if (memory_level == 0) {
          dual_csr->Open(oe_prefix(src_label, dst_label, edge_label),
                         ie_prefix(src_label, dst_label, edge_label),
                         edata_prefix(src_label, dst_label, edge_label),
                         snap_shot_dir, tmp_dir_path);
        } else if (memory_level >= 2) {
          dual_csr->OpenWithHugepages(
              oe_prefix(src_label, dst_label, edge_label),
              ie_prefix(src_label, dst_label, edge_label),
              edata_prefix(src_label, dst_label, edge_label), snap_shot_dir,
              vertex_capacities[src_label_i], vertex_capacities[dst_label_i]);
        } else {
          dual_csr->OpenInMemory(oe_prefix(src_label, dst_label, edge_label),
                                 ie_prefix(src_label, dst_label, edge_label),
                                 edata_prefix(src_label, dst_label, edge_label),
                                 snap_shot_dir, vertex_capacities[src_label_i],
                                 vertex_capacities[dst_label_i]);
        }
        dual_csr->Resize(vertex_capacities[src_label_i],
                         vertex_capacities[dst_label_i]);
      }
    }
  }
  v_mutex_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    v_mutex_[i] = std::make_shared<std::mutex>();
  }
}

void MutablePropertyFragment::Compact(uint32_t version) {
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
        auto dual_csr = dual_csr_map_.find(index);
        if (dual_csr != dual_csr_map_.end() && dual_csr->second != NULL) {
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            dual_csr->second->SortByEdgeData(version);
          }
        }
      }
    }
  }
}

void MutablePropertyFragment::Dump(const std::string& work_dir,
                                   uint32_t version) {
  std::string snapshot_dir_path = snapshot_dir(work_dir, version);
  std::error_code errorCode;
  std::filesystem::create_directories(snapshot_dir_path, errorCode);
  if (errorCode) {
    std::stringstream ss;
    ss << "Failed to create snapshot directory: " << snapshot_dir_path << ", "
       << errorCode.message();
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  std::vector<size_t> vertex_num(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    vertex_num[i] = lf_indexers_[i].size();
    lf_indexers_[i].dump(
        IndexerType::prefix() + "_" +
            vertex_map_prefix(schema_.get_vertex_label_name(i)),
        snapshot_dir_path);
    vertex_data_[i].resize(vertex_num[i]);
    vertex_data_[i].dump(vertex_table_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
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
        auto dual_csr = dual_csr_map_.find(index);
        if (dual_csr != dual_csr_map_.end() && dual_csr->second != NULL) {
          dual_csr->second->Resize(vertex_num[src_label_i],
                                   vertex_num[dst_label_i]);
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            dual_csr->second->SortByEdgeData(version + 1);
          }
          dual_csr->second->Dump(oe_prefix(src_label, dst_label, edge_label),
                                 ie_prefix(src_label, dst_label, edge_label),
                                 edata_prefix(src_label, dst_label, edge_label),
                                 snapshot_dir_path);
        }
      }
    }
  }
  set_snapshot_version(work_dir, version);
}

void MutablePropertyFragment::Warmup(int thread_num) {
  double t = -grape::GetCurrentTime();
  for (auto ptr : dual_csr_map_) {
    if (ptr.second != NULL) {
      ptr.second->Warmup(thread_num);
    }
  }
  for (auto& indexer : lf_indexers_) {
    indexer.warmup(thread_num);
  }
  t += grape::GetCurrentTime();
  LOG(INFO) << "Warmup takes: " << t << " s";
}
void MutablePropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         grape::OutArchive& arc,
                                         Allocator& alloc) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  dual_csr_map_.at(index)->IngestEdge(src_lid, dst_lid, arc, ts, alloc);
}

void MutablePropertyFragment::UpdateEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         const Any& arc, Allocator& alloc) {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  dual_csr_map_.at(index)->UpdateEdge(src_lid, dst_lid, arc, ts, alloc);
}
const Schema& MutablePropertyFragment::schema() const { return schema_; }

Schema& MutablePropertyFragment::mutable_schema() { return schema_; }

vid_t MutablePropertyFragment::lid_num(label_t vertex_label) const {
  return static_cast<vid_t>(lf_indexers_[vertex_label].size());
}

vid_t MutablePropertyFragment::vertex_num(label_t vertex_label) const {
  return static_cast<vid_t>(lf_indexers_[vertex_label].size()) -
         vertex_tomb_[vertex_label]->count();
}

bool MutablePropertyFragment::is_valid_lid(label_t vertex_label,
                                           vid_t lid) const {
  return !vertex_tomb_[vertex_label]->get(lid);
}

size_t MutablePropertyFragment::edge_num(label_t src_label, label_t edge_label,
                                         label_t dst_label) const {
  size_t index = schema_.generate_edge_label(src_label, dst_label, edge_label);
  auto dual_csr = dual_csr_map_.find(index);
  if (dual_csr != dual_csr_map_.end()) {
    return dual_csr->second->EdgeNum();
  } else {
    return 0;
  }
}

bool MutablePropertyFragment::get_lid(label_t label, const Any& oid,
                                      vid_t& lid) const {
  return lf_indexers_[label].get_index(oid, lid);
}

Any MutablePropertyFragment::get_oid(label_t label, vid_t lid) const {
  return lf_indexers_[label].get_key(lid);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, const Any& id) {
  return lf_indexers_[label].insert(id);
}

vid_t MutablePropertyFragment::add_vertex_safe(label_t label, const Any& id) {
  return lf_indexers_[label].insert_safe(id);
}

std::shared_ptr<CsrConstEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

std::shared_ptr<CsrConstEdgeIterBase>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

CsrConstEdgeIterBase* MutablePropertyFragment::get_outgoing_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter_raw(u);
}

CsrConstEdgeIterBase* MutablePropertyFragment::get_incoming_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter_raw(u);
}

std::shared_ptr<CsrEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

std::shared_ptr<CsrEdgeIterBase>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

std::string MutablePropertyFragment::get_statistics_json() const {
  size_t vertex_count = 0;
  std::string ss = "\"vertex_type_statistics\": [\n";
  size_t vertex_label_num = schema_.vertex_label_num();
  for (size_t idx = 0; idx < vertex_label_num; ++idx) {
    ss += "{\n\"type_id\": " + std::to_string(idx) + ", \n";
    ss += "\"type_name\": \"" + schema_.get_vertex_label_name(idx) + "\", \n";
    size_t count = vertex_num(idx);
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
  for (auto dual_csr : dual_csr_map_) {
    if (dual_csr.second != NULL) {
      edge_count_map.emplace(dual_csr.first, dual_csr.second->EdgeNum());
    }
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

void MutablePropertyFragment::generateStatistics() const {
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

void MutablePropertyFragment::dumpSchema() const {
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

void MutablePropertyFragment::change_csr_data_type_to_record_view(
    const std::string& src_type_name, const std::string& dst_type_name,
    const std::string& edge_type_name) {
  LOG(INFO) << "Changing CSR data type to RecordView for edge ["
            << edge_type_name << "] from [" << src_type_name << "] to ["
            << dst_type_name << "]";
  auto src_label_id = schema_.get_vertex_label_id(src_type_name);
  auto dst_label_id = schema_.get_vertex_label_id(dst_type_name);
  auto edge_label_id = schema_.get_edge_label_id(edge_type_name);
  auto index =
      schema_.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  CHECK(dual_csr_map_.find(index) != dual_csr_map_.end())
      << "Edge [" << edge_type_name << "] from [" << src_type_name << "] to ["
      << dst_type_name << "] does not exist, cannot change data type.";
  auto dual_csr = dual_csr_map_.at(index);
  if (dual_csr == NULL) {
    LOG(ERROR) << "Edge [" << edge_type_name << "] from [" << src_type_name
               << "] to [" << dst_type_name
               << "] does not exist, cannot change data type.";
  }
  auto prev_prop_names = schema_.get_edge_property_names(
      src_label_id, dst_label_id, edge_label_id);
  auto prev_prop_types =
      schema_.get_edge_properties(src_label_id, dst_label_id, edge_label_id);
  std::vector<std::string> empty_prop_name;
  std::vector<PropertyType> empty_prop_type;
  auto new_csr = new DualCsr<RecordView>(
      schema_.get_outgoing_edge_strategy(src_label_id, dst_label_id,
                                         edge_label_id),
      schema_.get_incoming_edge_strategy(src_label_id, dst_label_id,
                                         edge_label_id),
      empty_prop_name, empty_prop_type, {},
      schema_.outgoing_edge_mutable(src_type_name, dst_type_name,
                                    edge_type_name),
      schema_.incoming_edge_mutable(src_type_name, dst_type_name,
                                    edge_type_name));
  if (prev_prop_types.size() == 1) {
    if (prev_prop_types[0].type_enum == impl::PropertyTypeImpl::kVarChar ||
        prev_prop_types[0].type_enum == impl::PropertyTypeImpl::kStringView) {
      auto prev_csr = dynamic_cast<DualCsr<std::string_view>*>(dual_csr);
      auto in_csr = prev_csr->take_in_csr();
      auto out_csr = prev_csr->take_out_csr();
      auto string_column =
          std::make_shared<StringColumn>(prev_csr->take_string_column());
      new_csr->add_property(prev_prop_names[0], prev_prop_types[0],
                            string_column);
      auto in_index_csr = in_csr->take_index_csr();
      auto out_index_csr = out_csr->take_index_csr();
      delete in_csr;
      delete out_csr;
      new_csr->SetInCsr(std::move(in_index_csr));
      new_csr->SetOutCsr(std::move(out_index_csr));
    } else {
      std::vector<int32_t> ie_deg, oe_deg;
      oe_deg.resize(lid_num(src_label_id), 0);
      ie_deg.resize(lid_num(dst_label_id), 0);
      if (prev_prop_types[0] == PropertyType::Bool()) {
        auto out_csr = dynamic_cast<MutableCsr<bool>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<BoolColumn> column =
            std::make_shared<BoolColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::UInt8()) {
        auto out_csr =
            dynamic_cast<MutableCsr<uint8_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<UInt8Column> column =
            std::make_shared<UInt8Column>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::UInt16()) {
        auto out_csr =
            dynamic_cast<MutableCsr<uint16_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<UInt16Column> column =
            std::make_shared<UInt16Column>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Int32()) {
        auto out_csr =
            dynamic_cast<MutableCsr<int32_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<IntColumn> column =
            std::make_shared<IntColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::UInt32()) {
        auto out_csr =
            dynamic_cast<MutableCsr<uint32_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<UIntColumn> column =
            std::make_shared<UIntColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Int64()) {
        auto out_csr =
            dynamic_cast<MutableCsr<int64_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<LongColumn> column =
            std::make_shared<LongColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::UInt64()) {
        auto out_csr =
            dynamic_cast<MutableCsr<uint64_t>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<ULongColumn> column =
            std::make_shared<ULongColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Double()) {
        auto out_csr = dynamic_cast<MutableCsr<double>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<DoubleColumn> column =
            std::make_shared<DoubleColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Float()) {
        auto out_csr = dynamic_cast<MutableCsr<float>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<FloatColumn> column =
            std::make_shared<FloatColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Date()) {
        auto out_csr = dynamic_cast<MutableCsr<Date>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<DateColumn> column =
            std::make_shared<DateColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::DateTime()) {
        auto out_csr =
            dynamic_cast<MutableCsr<DateTime>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<DateTimeColumn> column =
            std::make_shared<DateTimeColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      } else if (prev_prop_types[0] == PropertyType::Timestamp()) {
        auto out_csr =
            dynamic_cast<MutableCsr<TimeStamp>*>(dual_csr->GetOutCsr());
        std::atomic<size_t> offset(0);
        std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
        std::shared_ptr<TimeStampColumn> column =
            std::make_shared<TimeStampColumn>(StorageStrategy::kMem);
        column->resize(edge_num(src_label_id, edge_label_id, dst_label_id));
        for (vid_t i = 0; i < lid_num(src_label_id); i++) {
          for (auto e : out_csr->get_edges(i)) {
            auto local_offset = offset.fetch_add(1);
            edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
            oe_deg[i]++;
            ie_deg[e.neighbor]++;
            column->set_value(local_offset, e.data);
          }
        }
        new_csr->BatchInit(
            oe_prefix(src_type_name, dst_type_name, edge_type_name),
            ie_prefix(src_type_name, dst_type_name, edge_type_name),
            edata_prefix(src_type_name, dst_type_name, edge_type_name),
            tmp_dir(work_dir_), oe_deg, ie_deg);
        for (auto& edge : edges) {
          new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                                std::get<2>(edge));
        }
        new_csr->add_property(prev_prop_names[0], prev_prop_types[0], column);
      }
    }
  } else if (prev_prop_types.size() == 0) {
    std::vector<int32_t> ie_deg, oe_deg;
    oe_deg.resize(lid_num(src_label_id), 0);
    ie_deg.resize(lid_num(dst_label_id), 0);
    auto out_csr =
        dynamic_cast<MutableCsr<grape::EmptyType>*>(dual_csr->GetOutCsr());
    std::atomic<size_t> offset(0);
    std::vector<std::tuple<vid_t, vid_t, size_t>> edges;
    for (vid_t i = 0; i < lid_num(src_label_id); i++) {
      for (auto e : out_csr->get_edges(i)) {
        auto local_offset = offset.fetch_add(1);
        edges.emplace_back(std::make_tuple(i, e.neighbor, local_offset));
        oe_deg[i]++;
        ie_deg[e.neighbor]++;
      }
    }
    new_csr->BatchInit(
        oe_prefix(src_type_name, dst_type_name, edge_type_name),
        ie_prefix(src_type_name, dst_type_name, edge_type_name),
        edata_prefix(src_type_name, dst_type_name, edge_type_name),
        tmp_dir(work_dir_), oe_deg, ie_deg);
    for (auto& edge : edges) {
      new_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                            std::get<2>(edge));
    }
  }
  // TODO: open new_csr, and copy data from dual_csr to new_csr
  LOG(INFO) << "Opening new CSR for edge [" << edge_type_name << "] from ["
            << src_type_name << "] to [" << dst_type_name << "]";
  {
    // Delete the old CSR and dump the new one
    ie_map_.erase(index);
    oe_map_.erase(index);
    delete dual_csr;
    dual_csr_map_.erase(index);
    dual_csr_map_.emplace(index, new_csr);
    ie_map_.emplace(index, new_csr->GetInCsr());
    oe_map_.emplace(index, new_csr->GetOutCsr());
  }
}

}  // namespace gs
