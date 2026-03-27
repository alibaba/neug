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

#include "neug/storages/graph/schema.h"
#include <ctype.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <filesystem>
#include <ostream>
#include <stdexcept>
#include <type_traits>
#include "neug/utils/exception/exception.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

std::shared_ptr<ExtraTypeInfo> parse_extra_type_info(YAML::Node node) {
  try {
    return node.as<std::shared_ptr<ExtraTypeInfo>>();
  } catch (const YAML::BadConversion& e) {
    LOG(ERROR) << "Failed to parse extra type info: " << e.what();
    return nullptr;
  }
}

template <typename T>
std::vector<T> extract_with_invalid_flags(
    const std::vector<T>& in, const std::vector<bool>& invalid_flags) {
  assert(in.size() == invalid_flags.size());
  auto cnt = std::count(invalid_flags.begin(), invalid_flags.end(), false);
  if ((size_t) cnt == in.size()) {
    return in;
  }
  std::vector<T> out;
  out.reserve(cnt);
  for (size_t i = 0; i < in.size(); i++) {
    if (!invalid_flags[i]) {
      out.emplace_back(in[i]);
    }
  }
  return out;
}

void VertexSchema::clear() {
  property_types.clear();
  property_names.clear();
  primary_keys.clear();
  default_property_values.clear();
  default_property_strings.clear();
  vprop_soft_deleted.clear();
}

void VertexSchema::add_properties(const std::vector<std::string>& names,
                                  const std::vector<DataType>& types,
                                  const std::vector<Property>& default_values) {
  for (size_t i = 0; i < names.size(); i++) {
    property_names.emplace_back(names[i]);
    property_types.emplace_back(types[i]);
    vprop_soft_deleted.emplace_back(false);
    if (default_values.size() > i)
      default_property_values.emplace_back(default_values[i]);
    else {
      default_property_values.emplace_back(get_default_value(types[i].id()));
    }
  }
  process_default_values(default_property_values, default_property_strings);
}

void VertexSchema::set_properties(const std::vector<DataType>& types,
                                  const std::vector<Property>& default_values) {
  property_types = types;
  default_property_values = default_values;
  process_default_values(default_property_values, default_property_strings);
  vprop_soft_deleted.resize(property_types.size(), false);
}

void VertexSchema::rename_properties(const std::vector<std::string>& names,
                                     const std::vector<std::string>& renames) {
  assert(names.size() == renames.size());
  for (size_t i = 0; i < names.size(); i++) {
    for (size_t j = 0; j < property_names.size(); j++) {
      if (vprop_soft_deleted[j]) {
        LOG(ERROR) << "Cannot rename logically deleted property: "
                   << property_names[j];
        continue;
      }
      if (property_names[j] == names[i]) {
        property_names[j] = renames[i];
        break;
      }
    }
  }
}

void VertexSchema::delete_properties(const std::vector<std::string>& names,
                                     bool is_soft) {
  assert(primary_keys.size() > 0);
  auto pk_iter =
      std::find(names.begin(), names.end(), std::get<1>(primary_keys[0]));
  if (pk_iter != names.end()) {
    THROW_RUNTIME_ERROR("Cannot physically delete primary key property: " +
                        *pk_iter);
  }
  for (size_t i = 0; i < names.size(); i++) {
    auto it = std::find(property_names.begin(), property_names.end(), names[i]);
    if (it != property_names.end()) {
      size_t j = static_cast<size_t>(std::distance(property_names.begin(), it));
      if (!is_soft) {
        property_names.erase(property_names.begin() + j);
        property_types.erase(property_types.begin() + j);
        default_property_values.erase(default_property_values.begin() + j);
        vprop_soft_deleted.erase(vprop_soft_deleted.begin() + j);
      } else {
        vprop_soft_deleted[j] = true;
      }
    } else {
      LOG(WARNING) << "Property name " << names[i]
                   << " does not exist in vertex schema.";
    }
  }
}

bool VertexSchema::is_property_soft_deleted(const std::string& prop) const {
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < vprop_soft_deleted.size());
    return vprop_soft_deleted[idx];
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Property not found: " + prop);
  }
}

void VertexSchema::revert_delete_properties(
    const std::vector<std::string>& names) {
  for (size_t i = 0; i < names.size(); i++) {
    auto it = std::find(property_names.begin(), property_names.end(), names[i]);
    if (it != property_names.end()) {
      size_t j = static_cast<size_t>(std::distance(property_names.begin(), it));
      vprop_soft_deleted[j] = false;
    }
  }
}

bool VertexSchema::has_property(const std::string& prop) const {
  assert(primary_keys.size() == 1);
  if (std::get<1>(primary_keys[0]) == prop) {
    return true;
  }
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < vprop_soft_deleted.size());
    if (vprop_soft_deleted[idx]) {
      return false;
    }
    return true;
  }
  return false;
}

int32_t VertexSchema::get_property_index(const std::string& prop) const {
  assert(primary_keys.size() == 1);
  if (std::get<1>(primary_keys[0]) == prop) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Primary key property does not have an index: " + prop);
  }
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < vprop_soft_deleted.size());
    if (vprop_soft_deleted[idx]) {
      return -1;
    }
    return static_cast<int32_t>(idx);
  }
  return -1;
}

std::string VertexSchema::get_property_name(size_t index) const {
  if (index >= property_names.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Property index out of range: " +
                                     std::to_string(index));
  }
  return property_names[index];
}

bool VertexSchema::is_pk_same(const VertexSchema& lhs,
                              const VertexSchema& rhs) {
  if (lhs.primary_keys.size() != rhs.primary_keys.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.primary_keys.size(); i++) {
    if (std::get<0>(lhs.primary_keys[i]) != std::get<0>(rhs.primary_keys[i]) ||
        std::get<2>(lhs.primary_keys[i]) != std::get<2>(rhs.primary_keys[i])) {
      return false;
    }
  }
  return true;
}

bool VertexSchema::has_property_internal(const std::string& prop) const {
  assert(primary_keys.size() == 1);
  if (std::get<1>(primary_keys[0]) == prop) {
    return true;
  }
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    return true;
  }
  return false;
}

bool EdgeSchema::is_bundled() const {
  if (properties.empty()) {
    return true;
  } else if (properties.size() == 1 &&
             properties[0].id() == DataTypeId::kVarchar) {
    return false;
  } else if (properties.size() > 1) {
    return false;
  } else {
    return true;
  }
}

bool EdgeSchema::has_property(const std::string& prop) const {
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < eprop_soft_deleted.size());
    if (eprop_soft_deleted[idx]) {
      return false;
    }
    return true;
  }
  return false;
}

void EdgeSchema::add_properties(const std::vector<std::string>& names,
                                const std::vector<DataType>& types,
                                const std::vector<Property>& default_values) {
  for (size_t i = 0; i < names.size(); i++) {
    if (std::find(property_names.begin(), property_names.end(), names[i]) !=
        property_names.end()) {
      THROW_RUNTIME_ERROR("Property name " + names[i] +
                          " already exists for edge " + edge_label_name +
                          " from " + src_label_name + " to " + dst_label_name);
    }
    property_names.emplace_back(names[i]);
    properties.emplace_back(types[i]);
    if (default_values.size() > i)
      default_property_values.emplace_back(default_values[i]);
    else {
      default_property_values.emplace_back(get_default_value(types[i].id()));
    }
    eprop_soft_deleted.emplace_back(false);
  }
  process_default_values(default_property_values, default_property_strings);
}

void EdgeSchema::rename_properties(const std::vector<std::string>& names,
                                   const std::vector<std::string>& renames) {
  for (size_t i = 0; i < names.size(); i++) {
    auto it = std::find(property_names.begin(), property_names.end(), names[i]);
    if (it != property_names.end()) {
      size_t j = static_cast<size_t>(std::distance(property_names.begin(), it));
      if (eprop_soft_deleted[j]) {
        THROW_RUNTIME_ERROR("Property name " + names[i] +
                            " has been deleted for edge " + edge_label_name +
                            " from " + src_label_name + " to " +
                            dst_label_name);
      }
      property_names[j] = renames[i];
    } else {
      THROW_RUNTIME_ERROR("Property name " + names[i] +
                          " does not exist for edge " + edge_label_name +
                          " from " + src_label_name + " to " + dst_label_name);
    }
  }
}

void EdgeSchema::delete_properties(const std::vector<std::string>& names,
                                   bool is_soft) {
  for (size_t i = 0; i < names.size(); i++) {
    auto it = std::find(property_names.begin(), property_names.end(), names[i]);
    if (it != property_names.end()) {
      size_t j = static_cast<size_t>(std::distance(property_names.begin(), it));
      if (!is_soft) {
        property_names.erase(property_names.begin() + j);
        properties.erase(properties.begin() + j);
        default_property_values.erase(default_property_values.begin() + j);
        eprop_soft_deleted.erase(eprop_soft_deleted.begin() + j);
      } else {
        eprop_soft_deleted[j] = true;
      }
    } else {
      LOG(WARNING) << "Property name " << names[i]
                   << " does not exist for edge " << edge_label_name << " from "
                   << src_label_name << " to " << dst_label_name;
    }
  }
}

bool EdgeSchema::is_property_soft_deleted(const std::string& prop) const {
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < eprop_soft_deleted.size());
    return eprop_soft_deleted[idx];
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Property not found: " + prop);
  }
}

void EdgeSchema::revert_delete_properties(
    const std::vector<std::string>& names) {
  for (size_t i = 0; i < names.size(); i++) {
    auto it = std::find(property_names.begin(), property_names.end(), names[i]);
    if (it != property_names.end()) {
      size_t j = static_cast<size_t>(std::distance(property_names.begin(), it));
      eprop_soft_deleted[j] = false;
    } else {
      THROW_RUNTIME_ERROR("Property name " + names[i] +
                          " does not exist for edge " + edge_label_name +
                          " from " + src_label_name + " to " + dst_label_name);
    }
  }
}

int32_t EdgeSchema::get_property_index(const std::string& prop) const {
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    size_t idx = static_cast<size_t>(std::distance(property_names.begin(), it));
    assert(idx < eprop_soft_deleted.size());
    if (eprop_soft_deleted[idx]) {
      return -1;
    }
    return static_cast<int32_t>(idx);
  }
  return -1;
}

std::string EdgeSchema::get_property_name(size_t index) const {
  if (index < property_names.size()) {
    return property_names[index];
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid property index: " +
                                     std::to_string(index));
  }
}

bool EdgeSchema::has_property_internal(const std::string& prop) const {
  auto it = std::find(property_names.begin(), property_names.end(), prop);
  if (it != property_names.end()) {
    return true;
  }
  return false;
}

Schema::Schema() = default;
Schema::~Schema() = default;

void Schema::Clear() {
  vlabel_indexer_.Clear();
  elabel_indexer_.Clear();
  v_schemas_.clear();
  e_schemas_.clear();
  vlabel_tomb_.clear();
  elabel_tomb_.clear();
  elabel_triplet_tomb_.clear();
}

// TODO(zhanglei): support extra_type_info for primary key
void Schema::AddVertexLabel(
    const std::string& label, const std::vector<DataType>& property_types,
    const std::vector<std::string>& property_names,
    const std::vector<std::tuple<DataType, std::string, size_t>>& primary_key,
    size_t max_vnum, const std::string& description,
    const std::vector<Property>& default_property_values) {
  label_t v_label_id = vertex_label_to_index(label);
  if (vlabel_tomb_.get(v_label_id)) {  // Add back a deleted label
    vlabel_tomb_.reset(v_label_id);
  }
  // Only grow, never shrink: a lower label_id being re-added must not
  // truncate entries for higher (possibly tombstoned) label_ids.
  if (v_schemas_.size() <= v_label_id) {
    v_schemas_.resize(v_label_id + 1);
  }
  v_schemas_[v_label_id] = std::make_shared<VertexSchema>(
      label, property_types, property_names, primary_key,
      default_property_values, description, max_vnum);
  VLOG(10) << "Add vertex label: " << label << ", id: " << (int) v_label_id
           << ", prop size: " << v_schemas_[v_label_id]->property_names.size();
}

void Schema::AddEdgeLabel(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label, const std::vector<DataType>& properties,
    const std::vector<std::string>& prop_names, EdgeStrategy oe,
    EdgeStrategy ie, bool oe_mutable, bool ie_mutable, bool sort_on_compaction,
    const std::string& description,
    const std::vector<Property>& default_property_values) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);
  if (elabel_tomb_.get(edge_label_id)) {  // Add back a deleted label
    elabel_tomb_.reset(edge_label_id);
  }

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  e_schemas_.emplace(
      label_id, std::make_shared<EdgeSchema>(
                    src_label, dst_label, edge_label, sort_on_compaction,
                    description, ie_mutable, oe_mutable, oe, ie, properties,
                    prop_names, default_property_values));
  if (label_id >= elabel_triplet_tomb_.size()) {
    elabel_triplet_tomb_.resize(label_id + 1);
  }

  if (elabel_triplet_tomb_.get(label_id)) {  // Add back a deleted label
    elabel_triplet_tomb_.reset(label_id);
  }
  VLOG(10) << "Add edge label: " << edge_label << ", id: " << (int) label_id
           << ", prop size: " << e_schemas_[label_id]->property_names.size();
}

label_t Schema::vertex_label_num() const {
  return static_cast<label_t>(vlabel_indexer_.size() - vlabel_tomb_.count());
}

label_t Schema::vertex_label_frontier() const {
  return static_cast<label_t>(vlabel_indexer_.size());
}

label_t Schema::edge_label_num() const {
  return static_cast<label_t>(elabel_indexer_.size() - elabel_tomb_.count());
}

label_t Schema::edge_label_frontier() const {
  return static_cast<label_t>(elabel_indexer_.size());
}

bool Schema::contains_vertex_label(const std::string& label) const {
  label_t ret;
  return vlabel_indexer_.get_index(label, ret) && !vlabel_tomb_.get(ret);
}

bool Schema::contains_edge_label(const std::string& label) const {
  label_t ret;
  return elabel_indexer_.get_index(label, ret) && !elabel_tomb_.get(ret);
}

bool Schema::vertex_label_valid(label_t label_id) const {
  return label_id < vlabel_indexer_.size() && !vlabel_tomb_.get(label_id);
}

bool Schema::edge_label_valid(label_t label_id) const {
  return label_id < elabel_indexer_.size() && !elabel_tomb_.get(label_id);
}

label_t Schema::get_vertex_label_id(const std::string& label) const {
  label_t ret;
  if (!vlabel_indexer_.get_index(label, ret)) {
    THROW_RUNTIME_ERROR("Fail to get vertex label: " + label);
  }
  if (vlabel_tomb_.get(ret)) {
    THROW_RUNTIME_ERROR("Vertex label " + label + " was deleted");
  }
  return ret;
}

label_t Schema::get_edge_label_id(const std::string& label) const {
  label_t ret;
  if (!elabel_indexer_.get_index(label, ret)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Edge label " + label + " not found");
  }
  if (elabel_tomb_.get(ret)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Edge label " + label + " was deleted");
  }
  return ret;
}

std::vector<label_t> Schema::get_vertex_label_ids() const {
  std::vector<label_t> ret;
  for (label_t i = 0; i < vlabel_indexer_.size(); i++) {
    if (!vlabel_tomb_.get(i)) {
      ret.push_back(i);
    }
  }
  return ret;
}

std::vector<label_t> Schema::get_edge_label_ids() const {
  std::vector<label_t> ret;
  for (label_t i = 0; i < elabel_indexer_.size(); i++) {
    if (!elabel_tomb_.get(i)) {
      ret.push_back(i);
    }
  }
  return ret;
}

void Schema::set_vertex_properties(
    label_t label_id, const std::vector<DataType>& types,
    const std::vector<Property>& default_property_values) {
  ensure_vertex_label_valid(label_id);
  v_schemas_[label_id]->set_properties(types, default_property_values);
}

std::vector<DataType> Schema::get_vertex_properties(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_properties(index);
}

std::vector<DataTypeId> Schema::get_vertex_properties_id(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_properties_id(index);
}

std::vector<DataType> Schema::get_vertex_properties(label_t label) const {
  ensure_vertex_label_valid(label);
  return extract_with_invalid_flags(v_schemas_.at(label)->property_types,
                                    v_schemas_.at(label)->vprop_soft_deleted);
}

std::vector<DataTypeId> Schema::get_vertex_properties_id(label_t label) const {
  ensure_vertex_label_valid(label);
  std::vector<DataTypeId> property_ids;
  for (size_t i = 0; i < v_schemas_.at(label)->property_types.size(); i++) {
    if (!v_schemas_.at(label)->vprop_soft_deleted[i]) {
      property_ids.push_back(v_schemas_.at(label)->property_types[i].id());
    }
  }
  return property_ids;
}

const std::vector<Property>& Schema::get_vertex_default_property_values(
    label_t label_id) const {
  return v_schemas_[label_id]->get_default_property_values();
}

std::vector<std::string> Schema::get_vertex_property_names(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_property_names(index);
}

std::vector<std::string> Schema::get_vertex_property_names(
    label_t label) const {
  ensure_vertex_label_valid(label);
  return extract_with_invalid_flags(v_schemas_[label]->property_names,
                                    v_schemas_[label]->vprop_soft_deleted);
}

const std::string& Schema::get_vertex_description(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_description(index);
}

const std::string& Schema::get_vertex_description(label_t label) const {
  ensure_vertex_label_valid(label);
  return v_schemas_[label]->description;
}

size_t Schema::get_max_vnum(const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return v_schemas_[index]->max_num;
}

bool Schema::exist(const std::string& src_label, const std::string& dst_label,
                   const std::string& edge_label) const {
  if (!contains_vertex_label(src_label) || !contains_vertex_label(dst_label) ||
      !contains_edge_label(edge_label)) {
    return false;
  }
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  return exist(src, dst, edge);
}

bool Schema::exist(label_t src_label, label_t dst_label,
                   label_t edge_label) const {
  return edge_triplet_valid(src_label, dst_label, edge_label) &&
         e_schemas_.count(
             generate_edge_label(src_label, dst_label, edge_label)) > 0;
}

std::vector<DataType> Schema::get_edge_properties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_properties(src, dst, edge);
}

std::vector<DataTypeId> Schema::get_edge_properties_id(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_properties_id(src, dst, edge);
}

std::vector<DataType> Schema::get_edge_properties(label_t src_label,
                                                  label_t dst_label,
                                                  label_t label) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(label);
  ensure_edge_triplet_valid(src_label, dst_label, label);
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  assert(e_schemas_.count(index) > 0);
  return extract_with_invalid_flags(e_schemas_.at(index)->properties,
                                    e_schemas_.at(index)->eprop_soft_deleted);
}

std::vector<DataTypeId> Schema::get_edge_properties_id(label_t src_label,
                                                       label_t dst_label,
                                                       label_t label) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(label);
  ensure_edge_triplet_valid(src_label, dst_label, label);
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  assert(e_schemas_.count(index) > 0);
  std::vector<DataTypeId> property_ids;
  for (size_t i = 0; i < e_schemas_.at(index)->properties.size(); i++) {
    if (!e_schemas_.at(index)->eprop_soft_deleted[i]) {
      property_ids.push_back(e_schemas_.at(index)->properties[i].id());
    }
  }
  return property_ids;
}

const std::vector<Property>& Schema::get_edge_default_property_values(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id) const {
  uint32_t index =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->get_default_property_values();
}

std::string Schema::get_edge_description(const std::string& src_label,
                                         const std::string& dst_label,
                                         const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_description(src, dst, edge);
}

std::string Schema::get_edge_description(label_t src_label, label_t dst_label,
                                         label_t label) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(label);
  ensure_edge_triplet_valid(src_label, dst_label, label);
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  return e_schemas_.at(index)->description;
}

std::vector<std::string> Schema::get_edge_property_names(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_property_names(src, dst, edge);
}

std::vector<std::string> Schema::get_edge_property_names(
    const label_t& src_label, const label_t& dst_label,
    const label_t& label) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(label);
  ensure_edge_triplet_valid(src_label, dst_label, label);
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  assert(e_schemas_.count(index) > 0);
  return extract_with_invalid_flags(e_schemas_.at(index)->property_names,
                                    e_schemas_.at(index)->eprop_soft_deleted);
}

EdgeStrategy Schema::get_outgoing_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  ensure_edge_triplet_valid(src, dst, edge);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->oe_strategy;
}

EdgeStrategy Schema::get_incoming_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  ensure_edge_triplet_valid(src, dst, edge);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->ie_strategy;
}

bool Schema::outgoing_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  ensure_edge_triplet_valid(src, dst, edge);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->oe_mutable;
}

bool Schema::incoming_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  ensure_edge_triplet_valid(src, dst, edge);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->ie_mutable;
}

bool Schema::get_sort_on_compaction(const std::string& src_label,
                                    const std::string& dst_label,
                                    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  ensure_edge_triplet_valid(src, dst, edge);
  return get_sort_on_compaction(src, dst, edge);
}

bool Schema::get_sort_on_compaction(label_t src_label, label_t dst_label,
                                    label_t label) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(label);
  ensure_edge_triplet_valid(src_label, dst_label, label);
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->sort_on_compaction;
}

bool Schema::edge_triplet_valid(label_t src, label_t dst, label_t edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  return (elabel_triplet_tomb_.size() > index) &&
         !elabel_triplet_tomb_.get(index);
}

const std::string& Schema::get_vertex_label_name(label_t index) const {
  std::string ret;
  if (vlabel_tomb_.get(index)) {
    THROW_INTERNAL_EXCEPTION("Label id: " + std::to_string(index) +
                             " was deleted");
  }
  return vlabel_indexer_.get_key(index);
}

const std::string& Schema::get_edge_label_name(label_t index) const {
  std::string ret;
  ensure_edge_label_valid(index);
  return elabel_indexer_.get_key(index);
}

const std::vector<std::tuple<DataType, std::string, size_t>>&
Schema::get_vertex_primary_key(label_t index) const {
  ensure_vertex_label_valid(index);
  return v_schemas_.at(index)->primary_keys;
}

const std::string& Schema::get_vertex_primary_key_name(label_t index) const {
  ensure_vertex_label_valid(index);
  const auto& primary_keys = v_schemas_.at(index)->primary_keys;
  return primary_keys.size() == 1
             ? std::get<1>(primary_keys[0])
             : THROW_INTERNAL_EXCEPTION(
                   "Expect only one primary key, but got " +
                   std::to_string(primary_keys.size()));
}

void Schema::Serialize(std::ostream& os) const {
  vlabel_indexer_.Serialize(os);
  elabel_indexer_.Serialize(os);
  InArchive arc;
  arc << static_cast<uint32_t>(v_schemas_.size());
  for (const auto& v_schema : v_schemas_) {
    arc << (*v_schema);
  }
  arc << static_cast<uint32_t>(e_schemas_.size());
  for (const auto& e_pair : e_schemas_) {
    arc << (uint32_t) e_pair.first << (*e_pair.second);
  }
  arc << description_ << name_ << id_;
  size_t size = arc.GetSize();
  os.write(reinterpret_cast<char*>(&size), sizeof(size));
  os.write(arc.GetBuffer(), size);
  vlabel_tomb_.Serialize(os);
  elabel_tomb_.Serialize(os);
  elabel_triplet_tomb_.Serialize(os);
}

void Schema::Deserialize(std::istream& is) {
  vlabel_indexer_.Deserialize(is);
  elabel_indexer_.Deserialize(is);
  OutArchive arc;
  size_t arc_size;
  is.read(reinterpret_cast<char*>(&arc_size), sizeof(arc_size));
  arc.Allocate(arc_size);
  is.read(arc.GetBuffer(), arc_size);
  uint32_t v_schema_size;
  arc >> v_schema_size;
  v_schemas_.resize(v_schema_size);
  for (uint32_t i = 0; i < v_schema_size; ++i) {
    v_schemas_[i] = std::make_shared<VertexSchema>();
    arc >> (*v_schemas_[i]);
  }
  uint32_t e_schema_size;
  arc >> e_schema_size;
  for (uint32_t i = 0; i < e_schema_size; ++i) {
    uint32_t key;
    auto e_schema = std::make_shared<EdgeSchema>();
    arc >> key >> (*e_schema);
    e_schemas_.emplace(key, e_schema);
  }

  arc >> description_ >> name_ >> id_;
  vlabel_tomb_.Deserialize(is);
  elabel_tomb_.Deserialize(is);
  elabel_triplet_tomb_.Deserialize(is);
}

label_t Schema::vertex_label_to_index(const std::string& label) {
  label_t ret;
  vlabel_indexer_.add(label, ret);
  if (v_schemas_.size() <= ret) {
    v_schemas_.resize(ret + 1);
    vlabel_tomb_.resize(ret + 1);
  }
  return ret;
}

label_t Schema::edge_label_to_index(const std::string& label) {
  label_t ret;
  elabel_indexer_.add(label, ret);
  if (elabel_tomb_.size() <= ret) {
    elabel_tomb_.resize(ret + 1);
  }
  return ret;
}

uint32_t Schema::generate_edge_label(label_t src, label_t dst,
                                     label_t edge) const {
  uint32_t ret = 0;
  ret |= src;
  ret <<= 8;
  ret |= dst;
  ret <<= 8;
  ret |= edge;
  return ret;
}

std::tuple<label_t, label_t, label_t> Schema::parse_edge_label(
    uint32_t edge_label) const {
  label_t edge = edge_label & 0xFF;
  edge_label >>= 8;
  label_t dst = edge_label & 0xFF;
  edge_label >>= 8;
  label_t src = edge_label & 0xFF;
  return std::make_tuple(src, dst, edge);
}

bool Schema::Equals(const Schema& other) const {
  // When compare two schemas, we only compare the properties and strategies
  if (vertex_label_num() != other.vertex_label_num() ||
      edge_label_num() != other.edge_label_num()) {
    return false;
  }
  for (label_t i = 0; i < vertex_label_frontier(); ++i) {
    if (vertex_label_valid(i) ^ other.vertex_label_valid(i)) {
      return false;
    }
    if (!vertex_label_valid(i)) {
      continue;
    }
    std::string label_name = get_vertex_label_name(i);
    {
      auto lhs = get_vertex_properties(label_name);
      auto rhs = other.get_vertex_properties(label_name);
      if (lhs != rhs) {
        return false;
      }
    }
    if (get_max_vnum(label_name) != other.get_max_vnum(label_name)) {
      return false;
    }
  }
  for (label_t src_label = 0; src_label < vertex_label_frontier();
       ++src_label) {
    if (!vertex_label_valid(src_label)) {
      continue;
    }
    for (label_t dst_label = 0; dst_label < vertex_label_frontier();
         ++dst_label) {
      if (!vertex_label_valid(dst_label)) {
        continue;
      }
      for (label_t edge_label = 0; edge_label < edge_label_frontier();
           ++edge_label) {
        std::string src_label_name = get_vertex_label_name(src_label);
        std::string dst_label_name = get_vertex_label_name(dst_label);
        std::string edge_label_name = get_edge_label_name(edge_label);
        auto lhs_exists =
            exist(src_label_name, dst_label_name, edge_label_name);
        auto rhs_exists =
            other.exist(src_label_name, dst_label_name, edge_label_name);
        if (lhs_exists != rhs_exists) {
          return false;
        }
        if (lhs_exists) {
          {
            auto lhs = get_edge_properties(src_label_name, dst_label_name,
                                           edge_label_name);
            auto rhs = other.get_edge_properties(src_label_name, dst_label_name,
                                                 edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
          {
            auto lhs = get_incoming_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            auto rhs = other.get_incoming_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
          {
            auto lhs = get_outgoing_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            auto rhs = other.get_outgoing_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
        }
      }
    }
  }
  return true;
}

neug::result<YAML::Node> Schema::to_yaml() const {
  return Schema::DumpToYaml(*this);
}

namespace config_parsing {

void RelationToEdgeStrategy(const std::string& rel_str,
                            EdgeStrategy& ie_strategy,
                            EdgeStrategy& oe_strategy) {
  if (rel_str == "ONE_TO_MANY") {
    ie_strategy = EdgeStrategy::kSingle;
    oe_strategy = EdgeStrategy::kMultiple;
  } else if (rel_str == "ONE_TO_ONE") {
    ie_strategy = EdgeStrategy::kSingle;
    oe_strategy = EdgeStrategy::kSingle;
  } else if (rel_str == "MANY_TO_ONE") {
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kSingle;
  } else if (rel_str == "MANY_TO_MANY") {
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kMultiple;
  } else {
    LOG(WARNING) << "relation " << rel_str
                 << " is not valid, using default value: kMultiple";
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kMultiple;
  }
}

static bool parse_property_type(YAML::Node node, DataType& type) {
  try {
    type = node.as<neug::DataType>();
    return true;
  } catch (const YAML::BadConversion& e) {
    LOG(ERROR) << "Failed to parse property type: " << e.what();
    return false;
  }
}

static Status parse_vertex_properties(YAML::Node node,
                                      const std::string& label_name,
                                      std::vector<DataType>& types,
                                      std::vector<std::string>& names) {
  if (!node || node.IsNull()) {
    VLOG(10) << "Found no vertex properties specified for vertex: "
             << label_name;
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "Expect properties for " << label_name << " to be a sequence";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Expect properties for " + label_name + " to be a sequence");
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "At least one property is needed for " << label_name;
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "At least one property is needed for " + label_name);
  }

  for (int i = 0; i < prop_num; ++i) {
    std::string prop_name_str;
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "Name of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Name of vertex-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "type of vertex-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    auto prop_type_node = node[i]["property_type"];
    DataType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "Fail to parse property type of vertex-" << label_name
                 << " prop-" << i - 1;
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Fail to parse property type of vertex-" + label_name +
                        " prop-" + std::to_string(i - 1));
    }
    types.push_back(prop_type);
    VLOG(10) << "prop-" << i - 1 << " name: " << prop_name_str
             << " type: " << prop_type.ToString();
    names.push_back(prop_name_str);
  }

  return Status::OK();
}

static Status parse_edge_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<DataType>& types,
                                    std::vector<std::string>& names) {
  if (!node || node.IsNull()) {
    VLOG(10) << "Found no edge properties specified for edge: " << label_name;
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "properties of edge -" << label_name
               << " not set properly, should be a sequence...";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "properties of edge -" + label_name +
                      " not set properly, should be a sequence...");
  }

  int prop_num = node.size();

  for (int i = 0; i < prop_num; ++i) {
    std::string prop_name_str;
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "type of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    auto prop_type_node = node[i]["property_type"];
    DataType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "type of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }

    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "name of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "name of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }

    types.push_back(prop_type);
    names.push_back(prop_name_str);
  }

  return Status::OK();
}

static Status parse_vertex_schema(YAML::Node node, Schema& schema) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "vertex type_name is not set");
  }
  // Cannot add two vertex label with same name
  if (schema.contains_vertex_label(label_name)) {
    LOG(ERROR) << "Vertex label " << label_name << " already exists";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Vertex label " + label_name + " already exists");
  }

  size_t max_num = ((size_t) 1) << 32;
  if (node["x_csr_params"]) {
    auto csr_node = node["x_csr_params"];
    get_scalar(csr_node, "max_vertex_num", max_num);
  }
  std::vector<DataType> property_types;
  std::vector<std::string> property_names;
  std::string description;  // default is empty string

  if (node["description"]) {
    description = node["description"].as<std::string>();
  }

  if (node["nullable"]) {
    LOG(ERROR) << "nullable is not supported yet";
    return Status(StatusCode::ERR_NOT_IMPLEMENTED,
                  "nullable is not supported yet");
  }

  if (node["default_value"]) {
    LOG(ERROR) << "default_value is not supported yet";
    return Status(StatusCode::ERR_NOT_IMPLEMENTED,
                  "default_value is not supported yet");
  }

  RETURN_IF_NOT_OK(parse_vertex_properties(node["properties"], label_name,
                                           property_types, property_names));
  if (!node["primary_keys"]) {
    LOG(ERROR) << "Expect field primary_keys for " << label_name;
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "Expect field primary_keys for " + label_name);
  }
  auto primary_key_node = node["primary_keys"];
  if (!primary_key_node.IsSequence()) {
    LOG(ERROR) << "[Primary_keys] should be sequence";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "[Primary_keys] should be sequence");
  }
  // remove primary key from properties.

  std::vector<int> primary_key_inds(primary_key_node.size(), -1);
  std::vector<std::tuple<DataType, std::string, size_t>> primary_keys;
  for (size_t i = 0; i < primary_key_node.size(); ++i) {
    auto cur_primary_key = primary_key_node[i];
    std::string primary_key_name = primary_key_node[0].as<std::string>();
    for (size_t j = 0; j < property_names.size(); ++j) {
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
    auto pk_type_id = property_types[primary_key_inds[i]].id();
    if (pk_type_id != DataTypeId::kInt64 &&
        pk_type_id != DataTypeId::kVarchar &&
        pk_type_id != DataTypeId::kUInt64 && pk_type_id != DataTypeId::kInt32 &&
        pk_type_id != DataTypeId::kUInt32) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " should be int64/int32/uint64/uint32 or string/varchar";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Primary key " + primary_key_name +
                        " should be int64/int32/uint64/"
                        "uint32 or string/varchar");
    }
    // TODO(zhanglei): extra_info is not used here
    primary_keys.emplace_back(property_types[primary_key_inds[i]],
                              property_names[primary_key_inds[i]],
                              primary_key_inds[i]);
    // remove primary key from properties.
    property_names.erase(property_names.begin() + primary_key_inds[i]);
    property_types.erase(property_types.begin() + primary_key_inds[i]);
  }

  schema.AddVertexLabel(label_name, property_types, property_names,
                        primary_keys, max_num, description);
  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(WARNING) << "type_id is not set properly for type: " << label_name
                 << ", try to use incremental id";
    type_id = schema.vertex_label_frontier() - 1;
  }
  auto label_id = schema.get_vertex_label_id(label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: " << label_name;
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "type_id is not equal to label_id for type: " + label_name);
  }
  return Status::OK();
}

static Status parse_vertices_schema(YAML::Node node, Schema& schema) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return Status(StatusCode::ERR_INVALID_SCHEMA, "vertex is not set properly");
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_vertex_schema(node[i], schema));
  }
  return Status::OK();
}

static Status parse_edge_schema(YAML::Node node, Schema& schema) {
  std::string edge_label_name;
  if (!node["type_name"]) {
    LOG(ERROR) << "edge type_name is not set properly";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "edge type_name is not set properly");
  }
  edge_label_name = node["type_name"].as<std::string>();

  std::vector<DataType> property_types;
  std::vector<std::string> prop_names;
  std::string description;  // default is empty string
  RETURN_IF_NOT_OK(parse_edge_properties(node["properties"], edge_label_name,
                                         property_types, prop_names));

  if (node["description"]) {
    description = node["description"].as<std::string>();
  }
  if (node["nullable"]) {
    LOG(ERROR) << "nullable is not supported yet";
    return Status(StatusCode::ERR_NOT_IMPLEMENTED,
                  "nullable is not supported yet");
  }

  if (node["default_value"]) {
    LOG(ERROR) << "default_value is not supported yet";
    return Status(StatusCode::ERR_NOT_IMPLEMENTED,
                  "default_value is not supported yet");
  }

  EdgeStrategy default_ie = EdgeStrategy::kMultiple;
  EdgeStrategy default_oe = EdgeStrategy::kMultiple;
  bool default_sort_on_compaction = false;

  // get vertex type pair relation
  auto vertex_type_pair_node = node["vertex_type_pair_relations"];
  // vertex_type_pair_node can be a list or a map
  if (!vertex_type_pair_node) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] is not set";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "edge [vertex_type_pair_relations] is not set");
  }
  if (!vertex_type_pair_node.IsSequence()) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] should be a sequence";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "edge [vertex_type_pair_relations] should be a sequence");
  }
  for (size_t i = 0; i < vertex_type_pair_node.size(); ++i) {
    std::string src_label_name, dst_label_name;
    auto cur_node = vertex_type_pair_node[i];
    EdgeStrategy cur_ie = default_ie;
    EdgeStrategy cur_oe = default_oe;
    bool cur_sort_on_compaction = default_sort_on_compaction;
    if (!get_scalar(cur_node, "source_vertex", src_label_name)) {
      LOG(ERROR) << "Expect field source_vertex for edge [" << edge_label_name
                 << "] in vertex_type_pair_relations";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Expect field source_vertex for edge [" + edge_label_name +
                        "] in vertex_type_pair_relations");
    }
    if (!get_scalar(cur_node, "destination_vertex", dst_label_name)) {
      LOG(ERROR) << "Expect field destination_vertex for edge ["
                 << edge_label_name << "] in vertex_type_pair_relations";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Expect field destination_vertex for edge [" +
                        edge_label_name + "] in vertex_type_pair_relations");
    }
    // check whether edge triplet exists in current schema
    if (schema.has_edge_label(src_label_name, dst_label_name,
                              edge_label_name)) {
      LOG(ERROR) << "Edge [" << edge_label_name << "] from [" << src_label_name
                 << "] to [" << dst_label_name << "] already exists";
      return Status(StatusCode::ERR_INVALID_SCHEMA,
                    "Edge [" + edge_label_name + "] from [" + src_label_name +
                        "] to [" + dst_label_name + "] already exists");
    }

    std::string relation_str;
    if (get_scalar(cur_node, "relation", relation_str)) {
      RelationToEdgeStrategy(relation_str, cur_ie, cur_oe);
    } else {
      LOG(WARNING) << "relation not defined, using default ie strategy: "
                   << cur_ie << ", oe strategy: " << cur_oe;
    }
    // check if x_csr_params presents
    bool oe_mutable = true, ie_mutable = true;
    if (cur_node["x_csr_params"]) {
      auto csr_node = cur_node["x_csr_params"];
      if (csr_node["edge_storage_strategy"]) {
        std::string edge_storage_strategy_str;
        if (get_scalar(csr_node, "edge_storage_strategy",
                       edge_storage_strategy_str)) {
          if (edge_storage_strategy_str == "ONLY_IN") {
            cur_oe = EdgeStrategy::kNone;
            VLOG(10) << "Store only in edges for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
          } else if (edge_storage_strategy_str == "ONLY_OUT") {
            cur_ie = EdgeStrategy::kNone;
            VLOG(10) << "Store only out edges for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
          } else if (edge_storage_strategy_str == "BOTH_OUT_IN" ||
                     edge_storage_strategy_str == "BOTH_IN_OUT") {
            VLOG(10) << "Store both in and out edges for edge: "
                     << src_label_name << "-[" << edge_label_name << "]->"
                     << dst_label_name;
          } else {
            LOG(ERROR) << "edge_storage_strategy is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name;
            return Status(
                StatusCode::ERR_INVALID_SCHEMA,
                "edge_storage_strategy is not set properly for edge: " +
                    src_label_name + "-[" + edge_label_name + "]->" +
                    dst_label_name);
          }
        }
      }
      // try to parse sort on compaction
      if (csr_node["sort_on_compaction"]) {
        std::string sort_on_compaction_str;
        if (get_scalar(csr_node, "sort_on_compaction",
                       sort_on_compaction_str)) {
          if (sort_on_compaction_str == "true" ||
              sort_on_compaction_str == "TRUE") {
            VLOG(10) << "Sort on compaction for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
            cur_sort_on_compaction = true;
          } else if (sort_on_compaction_str == "false" ||
                     sort_on_compaction_str == "FALSE") {
            VLOG(10) << "Do not sort on compaction for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
            cur_sort_on_compaction = false;
          } else {
            LOG(ERROR) << "sort_on_compaction is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name << "expect TRUE/FALSE";
            return Status(StatusCode::ERR_INVALID_SCHEMA,
                          "sort_on_compaction is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + "expect TRUE/FALSE");
          }
        }
      } else {
        VLOG(10) << "Do not sort on compaction for edge: " << src_label_name
                 << "-[" << edge_label_name << "]->" << dst_label_name;
      }

      if (csr_node["oe_mutability"]) {
        std::string mutability_str;
        if (get_scalar(csr_node, "oe_mutability", mutability_str)) {
          // mutability_str to upper_case
          std::transform(mutability_str.begin(), mutability_str.end(),
                         mutability_str.begin(), ::toupper);
          if (mutability_str == "IMMUTABLE") {
            oe_mutable = false;
          } else if (mutability_str == "MUTABLE") {
            oe_mutable = true;
          } else {
            LOG(ERROR) << "oe_mutability is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name
                       << ", expect IMMUTABLE/MUTABLE, got:" << mutability_str;
            return Status(StatusCode::ERR_INVALID_SCHEMA,
                          "oe_mutability is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + ", expect IMMUTABLE/MUTABLE");
          }
        }
      }
      if (csr_node["ie_mutability"]) {
        std::string mutability_str;
        if (get_scalar(csr_node, "ie_mutability", mutability_str)) {
          // mutability_str to upper_case
          std::transform(mutability_str.begin(), mutability_str.end(),
                         mutability_str.begin(), ::toupper);
          if (mutability_str == "IMMUTABLE") {
            ie_mutable = false;
          } else if (mutability_str == "MUTABLE") {
            ie_mutable = true;
          } else {
            LOG(ERROR) << "ie_mutability is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name
                       << ", expect IMMUTABLE/MUTABLE, got:" << mutability_str;
            return Status(StatusCode::ERR_INVALID_SCHEMA,
                          "ie_mutability is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + ", expect IMMUTABLE/MUTABLE");
          }
        }
      }
    }

    VLOG(10) << "edge " << edge_label_name << " from " << src_label_name
             << " to " << dst_label_name << " with " << property_types.size()
             << " properties";
    schema.AddEdgeLabel(src_label_name, dst_label_name, edge_label_name,
                        property_types, prop_names, cur_oe, cur_ie, oe_mutable,
                        ie_mutable, cur_sort_on_compaction, description);
  }

  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(WARNING) << "type_id is not set properly for type: " << edge_label_name
                 << ", try to use incremental id";
    type_id = schema.edge_label_frontier() - 1;
  }
  auto label_id = schema.get_edge_label_id(edge_label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: "
               << edge_label_name;
    return Status(
        StatusCode::ERR_INVALID_SCHEMA,
        "type_id is not equal to label_id for type: " + edge_label_name);
  }
  return Status::OK();
}

static Status parse_edges_schema(YAML::Node node, Schema& schema) {
  if (node.IsNull()) {
    LOG(INFO) << "No edge is set";
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return Status(StatusCode::ERR_INVALID_SCHEMA, "edge is not set properly");
  }
  int num = node.size();
  VLOG(10) << "Try to parse " << num << "edge configuration";
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_edge_schema(node[i], schema));
  }
  return Status::OK();
}

static Status parse_schema_from_yaml_node(const YAML::Node& graph_node,
                                          Schema& schema,
                                          const std::string& parent_dir = "") {
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph schema is not set properly";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "graph schema is not set properly");
  }
  if (!expect_config(graph_node, "store_type", std::string("mutable_csr"))) {
    LOG(WARNING) << "store_type is not set properly, use default value: "
                 << "mutable_csr";
  }
  if (graph_node["name"]) {
    schema.SetGraphName(graph_node["name"].as<std::string>());
  }

  if (graph_node["id"]) {
    VLOG(1) << "Got id: " << graph_node["id"].as<std::string>();
    schema.SetGraphId(graph_node["id"].as<std::string>());
  } else {
    VLOG(1) << "id is not set";
  }

  if (graph_node["description"]) {
    schema.SetDescription(graph_node["description"].as<std::string>());
  }

  auto schema_node = graph_node["schema"];

  if (!graph_node["schema"]) {
    LOG(ERROR) << "expect schema field, but not found";
    return Status(StatusCode::ERR_INVALID_SCHEMA,
                  "expect schema field, but not found");
  }

  RETURN_IF_NOT_OK(parse_vertices_schema(schema_node["vertex_types"], schema));

  if (schema_node["edge_types"]) {
    RETURN_IF_NOT_OK(parse_edges_schema(schema_node["edge_types"], schema));
  }
  return Status::OK();
}

static Status parse_schema_config_file(const std::string& path,
                                       Schema& schema) {
  YAML::Node graph_node = YAML::LoadFile(path);
  // get the directory of path
  auto parent_dir = std::filesystem::path(path).parent_path().string();
  return parse_schema_from_yaml_node(graph_node, schema, parent_dir);
}

///////////////////////Dump schema to yaml//////////////////////////
bool dump_vertices_schema(const Schema& schema, YAML::Node& node) {
  auto v_labels = schema.get_vertex_label_ids();
  for (auto& v_label : v_labels) {
    YAML::Node cur_node(YAML::NodeType::Map);
    cur_node["type_name"] = schema.get_vertex_label_name(v_label);
    cur_node["description"] = schema.get_vertex_description(v_label);
    cur_node["type_id"] = std::to_string(v_label);
    cur_node["properties"] = YAML::Node(YAML::NodeType::Sequence);
    auto properties = schema.get_vertex_properties(v_label);
    auto property_names = schema.get_vertex_property_names(v_label);
    CHECK_EQ(properties.size(), property_names.size());
    {
      YAML::Node pk_node(YAML::NodeType::Map);
      pk_node["property_id"] = 0;  // primary key is always the first property
      pk_node["property_name"] = schema.get_vertex_primary_key_name(v_label);
      pk_node["property_type"] = property_type_to_yaml(
          std::get<0>(schema.get_vertex_primary_key(v_label)[0]));
      cur_node["properties"].push_back(pk_node);
    }
    // The first property is the primary key, so we start from 1
    for (size_t i = 0; i < properties.size(); ++i) {
      YAML::Node prop_node(YAML::NodeType::Map);
      prop_node["property_id"] = i + 1;
      prop_node["property_name"] = property_names[i];
      prop_node["property_type"] = property_type_to_yaml(properties[i]);
      cur_node["properties"].push_back(prop_node);
    }
    YAML::Node primary_keys_node(YAML::NodeType::Sequence);
    for (const auto& pk : schema.get_vertex_primary_key(v_label)) {
      primary_keys_node.push_back(std::get<1>(pk));  // push the property name
    }
    cur_node["primary_keys"] = primary_keys_node;

    node.push_back(cur_node);
  }
  return true;
}

bool dump_edges_schema(const Schema& schema, YAML::Node& node) {
  auto v_labels = schema.get_vertex_label_ids();
  auto e_labels = schema.get_edge_label_ids();
  for (auto e_label : e_labels) {
    YAML::Node cur_node;
    cur_node["type_id"] = std::to_string(e_label);
    cur_node["type_name"] = schema.get_edge_label_name(e_label);
    cur_node["properties"] = YAML::Node(YAML::NodeType::Sequence);
    cur_node["vertex_type_pair_relations"] =
        YAML::Node(YAML::NodeType::Sequence);
    bool properties_set = false;

    for (auto src_v : v_labels) {
      for (auto dst_v : v_labels) {
        if (schema.exist(src_v, dst_v, e_label)) {
          if (!properties_set) {
            auto properties = schema.get_edge_properties(src_v, dst_v, e_label);
            auto property_names =
                schema.get_edge_property_names(src_v, dst_v, e_label);
            CHECK_EQ(properties.size(), property_names.size());
            if (cur_node["properties"].size() != 0) {
              if (properties.size() != cur_node["properties"].size()) {
                LOG(ERROR)
                    << "Edge properties size mismatch among different edge "
                       "triplets for edge label: "
                    << schema.get_edge_label_name(e_label);
                return false;
              }
            } else {
              for (size_t i = 0; i < properties.size(); ++i) {
                YAML::Node prop_node;
                prop_node["property_id"] = i;
                prop_node["property_name"] = property_names[i];
                prop_node["property_type"] =
                    property_type_to_yaml(properties[i]);
                cur_node["properties"].push_back(prop_node);
              }
            }
          }

          YAML::Node vertex_type_pair_node;
          vertex_type_pair_node["source_vertex"] =
              schema.get_vertex_label_name(src_v);
          vertex_type_pair_node["destination_vertex"] =
              schema.get_vertex_label_name(dst_v);
          vertex_type_pair_node["relation"] =
              schema.get_edge_strategy(src_v, dst_v, e_label);
          cur_node["vertex_type_pair_relations"].push_back(
              vertex_type_pair_node);
        }
      }
    }
    if (cur_node["vertex_type_pair_relations"].size() == 0) {
      continue;
    }
    node.push_back(cur_node);
  }
  return true;
}

}  // namespace config_parsing

std::string Schema::GetDescription() const { return description_; }

void Schema::SetDescription(const std::string& description) {
  description_ = description;
}

// check whether prop in vprop_names, or is the primary key
bool Schema::vertex_has_property(const std::string& label,
                                 const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  return vertex_has_property(v_label_id, prop);
}

bool Schema::vertex_has_property(label_t v_label,
                                 const std::string& prop) const {
  assert(vertex_label_valid(v_label));
  return v_schemas_.at(v_label)->has_property(prop);
}

bool Schema::vertex_has_primary_key(const std::string& label,
                                    const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  const auto& keys = v_schemas_.at(v_label_id)->primary_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (std::get<1>(keys[i]) == prop) {
      return true;
    }
  }
  return false;
}

bool Schema::edge_has_property(const std::string& src_label,
                               const std::string& dst_label,
                               const std::string& edge_label,
                               const std::string& prop) const {
  auto e_label_id = get_edge_label_id(edge_label);
  auto src_label_id = get_vertex_label_id(src_label);
  auto dst_label_id = get_vertex_label_id(dst_label);
  return edge_has_property(src_label_id, dst_label_id, e_label_id, prop);
}

bool Schema::edge_has_property(label_t src_label, label_t dst_label,
                               label_t edge_label,
                               const std::string& prop) const {
  ensure_vertex_label_valid(src_label);
  ensure_vertex_label_valid(dst_label);
  ensure_edge_label_valid(edge_label);
  ensure_edge_triplet_valid(src_label, dst_label, edge_label);
  auto label_id = generate_edge_label(src_label, dst_label, edge_label);
  assert(e_schemas_.count(label_id) > 0);
  return e_schemas_.at(label_id)->has_property(prop);
}

bool Schema::has_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& label) const {
  label_t edge_label_id;
  if (!contains_vertex_label(src_label) || !contains_vertex_label(dst_label)) {
    LOG(ERROR) << "src_label or dst_label not found:" << src_label << ", "
               << dst_label;
    return false;
  }
  auto src_label_id = get_vertex_label_id(src_label);
  auto dst_label_id = get_vertex_label_id(dst_label);
  if (!elabel_indexer_.get_index(label, edge_label_id)) {
    return false;
  }
  return has_edge_label(src_label_id, dst_label_id, edge_label_id);
}

bool Schema::has_edge_label(label_t src_label, label_t dst_label,
                            label_t edge_label) const {
  uint32_t e_label_id = generate_edge_label(src_label, dst_label, edge_label);
  return e_schemas_.find(e_label_id) != e_schemas_.end();
}

neug::result<Schema> Schema::LoadFromYaml(const std::string& schema_config) {
  Schema schema;
  if (!schema_config.empty() && std::filesystem::exists(schema_config)) {
    auto status =
        config_parsing::parse_schema_config_file(schema_config, schema);
    if (status.ok()) {
      return neug::result<Schema>(std::move(schema));
    } else {
      RETURN_ERROR(status);
    }
  }
  RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_SCHEMA,
                            "Schema config file not found"));
}

neug::result<Schema> Schema::LoadFromYamlNode(
    const YAML::Node& schema_yaml_node) {
  Schema schema;
  auto status =
      config_parsing::parse_schema_from_yaml_node(schema_yaml_node, schema);
  if (status.ok()) {
    return neug::result<Schema>(std::move(schema));
  } else {
    RETURN_ERROR(status);
  }
}

neug::result<YAML::Node> Schema::DumpToYaml(const Schema& schema) {
  YAML::Node graph_node;
  graph_node["name"] = schema.GetGraphName();
  graph_node["id"] = schema.GetGraphId();
  graph_node["description"] = schema.GetDescription();

  YAML::Node vertex_types(YAML::NodeType::Sequence);
  config_parsing::dump_vertices_schema(schema, vertex_types);
  graph_node["schema"]["vertex_types"] = vertex_types;

  YAML::Node edge_types(YAML::NodeType::Sequence);
  config_parsing::dump_edges_schema(schema, edge_types);
  graph_node["schema"]["edge_types"] = edge_types;

  return graph_node;
}

void Schema::AddVertexProperties(
    const std::string& label, const std::vector<std::string>& properties_names,
    const std::vector<DataType>& properties_types,
    const std::vector<Property>& properties_default_values) {
  auto v_label_id = get_vertex_label_id(label);
  assert(v_label_id < v_schemas_.size());
  v_schemas_[v_label_id]->add_properties(properties_names, properties_types,
                                         properties_default_values);
}

bool Schema::IsVertexLabelSoftDeleted(const std::string& label) const {
  auto v_label_id = get_vertex_label_id_internal(label);
  return vlabel_tomb_.get(v_label_id) && !v_schemas_[v_label_id]->empty();
}

bool Schema::IsVertexLabelSoftDeleted(label_t v_label) const {
  return vlabel_tomb_.get(v_label) && !v_schemas_[v_label]->empty();
}

bool Schema::IsEdgeLabelSoftDeleted(const std::string& src_label,
                                    const std::string& dst_label,
                                    const std::string& edge_label) const {
  label_t src = get_vertex_label_id_internal(src_label);
  label_t dst = get_vertex_label_id_internal(dst_label);
  label_t edge = get_edge_label_id_internal(edge_label);
  return IsEdgeLabelSoftDeleted(src, dst, edge);
}

bool Schema::IsEdgeLabelSoftDeleted(label_t src_label, label_t dst_label,
                                    label_t edge_label) const {
  assert(vertex_label_valid(src_label) || IsVertexLabelSoftDeleted(src_label));
  assert(vertex_label_valid(dst_label) || IsVertexLabelSoftDeleted(dst_label));
  assert(edge_label_valid(edge_label));
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
  return elabel_triplet_tomb_.get(index) && e_schemas_.count(index) > 0;
}

bool Schema::IsVertexPropertySoftDeleted(
    const std::string& label, const std::string& property_name) const {
  auto v_label_id = get_vertex_label_id(label);
  assert(v_label_id < v_schemas_.size());
  return v_schemas_[v_label_id]->is_property_soft_deleted(property_name);
}

bool Schema::IsVertexPropertySoftDeleted(
    label_t v_label, const std::string& property_name) const {
  assert(v_label < v_schemas_.size());
  return v_schemas_[v_label]->is_property_soft_deleted(property_name);
}

bool Schema::IsEdgePropertySoftDeleted(const std::string& src_label,
                                       const std::string& dst_label,
                                       const std::string& edge_label,
                                       const std::string& property_name) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  return IsEdgePropertySoftDeleted(src, dst, edge, property_name);
}

bool Schema::IsEdgePropertySoftDeleted(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& property_name) const {
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
  assert(e_schemas_.count(index) > 0);
  return e_schemas_.at(index)->is_property_soft_deleted(property_name);
}

void Schema::RenameVertexProperties(
    const std::string& label, const std::vector<std::string>& properties_names,
    const std::vector<std::string>& properties_renames) {
  auto v_label_id = get_vertex_label_id(label);
  assert(v_label_id < v_schemas_.size());
  v_schemas_[v_label_id]->rename_properties(properties_names,
                                            properties_renames);
}

void Schema::DeleteVertexProperties(
    const std::string& label, const std::vector<std::string>& properties_names,
    bool is_soft) {
  auto v_label_id = get_vertex_label_id(label);
  assert(v_label_id < v_schemas_.size());
  v_schemas_[v_label_id]->delete_properties(properties_names, is_soft);
}

void Schema::RevertDeleteVertexProperties(
    const std::string& label,
    const std::vector<std::string>& properties_names) {
  auto v_label_id = get_vertex_label_id(label);
  assert(v_label_id < v_schemas_.size());
  v_schemas_[v_label_id]->revert_delete_properties(properties_names);
}

void Schema::DeleteVertexLabel(const std::string& label, bool is_soft) {
  auto v_label_id = get_vertex_label_id_internal(label);
  return DeleteVertexLabel(v_label_id, is_soft);
}

void Schema::DeleteVertexLabel(label_t v_label_id, bool is_soft) {
  assert(v_label_id < v_schemas_.size());
  if (!is_soft) {
    v_schemas_[v_label_id]->clear();
  }
  vlabel_tomb_.set(v_label_id);
}

void Schema::DeleteEdgeLabel(const std::string& label, bool is_soft) {
  auto e_label_id = get_edge_label_id_internal(label);
  for (label_t src_v_label = 0; src_v_label < vlabel_indexer_.size();
       src_v_label++) {
    if (vlabel_tomb_.get(src_v_label)) {
      continue;
    }
    for (label_t dst_v_label = 0; dst_v_label < vlabel_indexer_.size();
         dst_v_label++) {
      if (vlabel_tomb_.get(dst_v_label)) {
        continue;
      }
      if (exist(src_v_label, dst_v_label, e_label_id)) {
        uint32_t index =
            generate_edge_label(src_v_label, dst_v_label, e_label_id);
        if (!is_soft) {
          e_schemas_.erase(index);
        }
        elabel_triplet_tomb_.set(index);
      }
    }
  }

  elabel_tomb_.set(e_label_id);
}

void Schema::DeleteEdgeLabel(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string& edge_label, bool is_soft) {
  label_t src = get_vertex_label_id_internal(src_label);
  label_t dst = get_vertex_label_id_internal(dst_label);
  label_t edge = get_edge_label_id_internal(edge_label);
  return DeleteEdgeLabel(src, dst, edge, is_soft);
}

void Schema::DeleteEdgeLabel(const label_t& src, const label_t& dst,
                             const label_t& edge, bool is_soft) {
  uint32_t index = generate_edge_label(src, dst, edge);
  // After delete one edge_triplet, scan all edge_triplets to see if
  // this edge_label is still used. If not, mark it as deleted.
  if (e_schemas_.count(index)) {
    if (!is_soft) {
      e_schemas_.erase(index);
    }
    elabel_triplet_tomb_.set(index);
    bool edge_label_still_used = false;
    for (const auto& pair : e_schemas_) {
      auto e_label_id =
          get_edge_label_id_internal(pair.second->edge_label_name);
      if (e_label_id == edge) {
        edge_label_still_used = true;
        break;
      }
    }
    if (!edge_label_still_used) {
      elabel_tomb_.set(edge);
    }
  }
}

void Schema::AddEdgeProperties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label,
    const std::vector<std::string>& properties_names,
    const std::vector<DataType>& properties_types,
    const std::vector<Property>& properties_default_values) {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  uint32_t index = generate_edge_label(src, dst, edge);
  // Check if the property name already exists
  assert(e_schemas_.count(index) > 0);
  e_schemas_.at(index)->add_properties(properties_names, properties_types,
                                       properties_default_values);
}

void Schema::RenameEdgeProperties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label,
    const std::vector<std::string>& properties_names,
    const std::vector<std::string>& properties_renames) {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  e_schemas_.at(index)->rename_properties(properties_names, properties_renames);
}

void Schema::DeleteEdgeProperties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label,
    const std::vector<std::string>& properties_names, bool is_soft) {
  label_t src = get_vertex_label_id_internal(src_label);
  label_t dst = get_vertex_label_id_internal(dst_label);
  label_t edge = get_edge_label_id_internal(edge_label);
  uint32_t index = generate_edge_label(src, dst, edge);
  assert(e_schemas_.count(index) > 0);
  e_schemas_.at(index)->delete_properties(properties_names, is_soft);
}

void Schema::RevertDeleteEdgeProperties(
    label_t src_label, label_t dst_label, label_t edge_label,
    const std::vector<std::string>& properties_names) {
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
  assert(e_schemas_.count(index) > 0);
  e_schemas_.at(index)->revert_delete_properties(properties_names);
}

void Schema::RevertDeleteEdgeProperties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label,
    const std::vector<std::string>& properties_names) {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  RevertDeleteEdgeProperties(src, dst, edge, properties_names);
}

std::string Schema::get_edge_strategy(label_t src_label, label_t dst_label,
                                      label_t edge_label) const {
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
  if (!edge_triplet_valid(src_label, dst_label, edge_label)) {
    THROW_RUNTIME_ERROR(
        "Edge label triplet not found: " + std::to_string(src_label) + "-" +
        std::to_string(edge_label) + "->" + std::to_string(dst_label));
  }
  assert(edge_triplet_valid(src_label, dst_label, edge_label));
  assert(e_schemas_.count(index) > 0);
  auto oe_strategy = e_schemas_.at(index)->oe_strategy;
  auto ie_strategy = e_schemas_.at(index)->ie_strategy;
  if (oe_strategy == EdgeStrategy::kMultiple) {
    if (ie_strategy == EdgeStrategy::kMultiple) {
      return "MANY_TO_MANY";
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      return "MANY_TO_ONE";
    } else {
      THROW_RUNTIME_ERROR("ie_strategy should not be none");
    }
  } else if (oe_strategy == EdgeStrategy::kSingle) {
    if (ie_strategy == EdgeStrategy::kMultiple) {
      return "ONE_TO_MANY";
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      return "ONE_TO_ONE";
    } else {
      THROW_RUNTIME_ERROR("ie_strategy should not be none");
    }
  } else {
    THROW_RUNTIME_ERROR("oe_strategy should not be none");
  }
}

void Schema::RevertDeleteVertexLabel(const std::string& v_label) {
  label_t v_label_id = get_vertex_label_id_internal(v_label);
  if (vlabel_tomb_.get(v_label_id)) {
    vlabel_tomb_.reset(v_label_id);
  }
}

void Schema::RevertDeleteEdgeLabel(label_t e_label_id) {
  if (elabel_tomb_.get(e_label_id)) {
    elabel_tomb_.reset(e_label_id);
  }
}

void Schema::RevertDeleteEdgeLabel(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& edge_label) {
  label_t src = get_vertex_label_id_internal(src_label);
  label_t dst = get_vertex_label_id_internal(dst_label);
  label_t edge = get_edge_label_id_internal(edge_label);
  uint32_t index = generate_edge_label(src, dst, edge);
  if (index < elabel_triplet_tomb_.size() && elabel_triplet_tomb_.get(index)) {
    elabel_triplet_tomb_.reset(index);
  }
  if (elabel_tomb_.get(edge)) {
    elabel_tomb_.reset(edge);
  }
}

void Schema::ensure_vertex_label_valid(label_t v_label_id) const {
  if (!vertex_label_valid(v_label_id)) {
    THROW_RUNTIME_ERROR("Vertex label id " + std::to_string(v_label_id) +
                        " is not valid");
  }
}

void Schema::ensure_edge_label_valid(label_t e_label_id) const {
  if (!edge_label_valid(e_label_id)) {
    THROW_RUNTIME_ERROR("Edge label id " + std::to_string(e_label_id) +
                        " is not valid");
  }
}

void Schema::ensure_edge_triplet_valid(label_t src_label, label_t dst_label,
                                       label_t edge_label) const {
  if (!edge_triplet_valid(src_label, dst_label, edge_label)) {
    THROW_RUNTIME_ERROR(
        "Edge label triplet not found: " + std::to_string(src_label) + "-" +
        std::to_string(edge_label) + "->" + std::to_string(dst_label));
  }
}

label_t Schema::get_vertex_label_id_internal(const std::string& label) const {
  label_t ret;
  if (!vlabel_indexer_.get_index(label, ret)) {
    THROW_RUNTIME_ERROR("Fail to get vertex label: " + label);
  }
  return ret;
}

label_t Schema::get_edge_label_id_internal(const std::string& label) const {
  label_t ret;
  if (!elabel_indexer_.get_index(label, ret)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Edge label " + label + " not found");
  }
  return ret;
}

bool Schema::vertex_has_property_internal(label_t label,
                                          const std::string& prop) const {
  assert(vertex_label_valid(label));
  return v_schemas_.at(label)->has_property_internal(prop);
}

bool Schema::edge_has_property_internal(label_t src_label, label_t dst_label,
                                        label_t edge_label,
                                        const std::string& prop) const {
  auto label_id = generate_edge_label(src_label, dst_label, edge_label);
  assert(e_schemas_.count(label_id) > 0);
  return e_schemas_.at(label_id)->has_property_internal(prop);
}

Schema Schema::Compact() const {
  Schema new_schema;
  new_schema.name_ = name_;
  new_schema.id_ = id_;
  new_schema.description_ = description_;

  for (label_t v_label = 0; v_label < v_schemas_.size(); ++v_label) {
    if (vlabel_tomb_.get(v_label)) {
      continue;
    }
    auto vlabel_name = vlabel_indexer_.get_key(v_label);
    label_t new_label;
    if (!new_schema.vlabel_indexer_.add(vlabel_name, new_label)) {
      THROW_RUNTIME_ERROR("Failed to add vertex label: " + vlabel_name);
    }
    assert(new_label == new_schema.v_schemas_.size());
    new_schema.v_schemas_.push_back(v_schemas_[v_label]);
  }

  for (label_t e_label = 0; e_label < elabel_indexer_.size(); ++e_label) {
    if (elabel_tomb_.get(e_label)) {
      continue;
    }
    auto elabel_name = elabel_indexer_.get_key(e_label);
    label_t new_label;
    if (!new_schema.elabel_indexer_.add(elabel_name, new_label)) {
      THROW_RUNTIME_ERROR("Failed to add edge label: " + elabel_name);
    }
    assert(new_label == new_schema.elabel_indexer_.size() - 1);
  }

  uint32_t max_e_triplet_index = 0;
  for (const auto& pair : e_schemas_) {
    label_t src_v, dst_v, e_label;
    std::tie(src_v, dst_v, e_label) = parse_edge_label(pair.first);
    if (vlabel_tomb_.get(src_v) || vlabel_tomb_.get(dst_v) ||
        elabel_tomb_.get(e_label) ||
        !edge_triplet_valid(src_v, dst_v, e_label)) {
      continue;
    }
    auto src_label_name = vlabel_indexer_.get_key(src_v);
    auto dst_label_name = vlabel_indexer_.get_key(dst_v);
    auto e_label_name = elabel_indexer_.get_key(e_label);
    label_t new_src_v, new_dst_v, new_e_label;
    if (!new_schema.vlabel_indexer_.get_index(src_label_name, new_src_v)) {
      THROW_RUNTIME_ERROR("Failed to get vertex label: " + src_label_name);
    }
    if (!new_schema.vlabel_indexer_.get_index(dst_label_name, new_dst_v)) {
      THROW_RUNTIME_ERROR("Failed to get vertex label: " + dst_label_name);
    }
    if (!new_schema.elabel_indexer_.get_index(e_label_name, new_e_label)) {
      THROW_RUNTIME_ERROR("Failed to get edge label: " + e_label_name);
    }

    auto new_index =
        new_schema.generate_edge_label(new_src_v, new_dst_v, new_e_label);
    max_e_triplet_index = std::max(max_e_triplet_index, new_index);
    new_schema.e_schemas_[new_index] = pair.second;
  }
  new_schema.vlabel_tomb_.resize(new_schema.v_schemas_.size());
  new_schema.elabel_tomb_.resize(new_schema.elabel_indexer_.size());
  new_schema.elabel_triplet_tomb_.resize(max_e_triplet_index + 1);
  return new_schema;
}

InArchive& operator<<(InArchive& in_archive, const DataType& type) {
  auto id = type.id();
  in_archive << id;
  auto type_info = type.RawExtraTypeInfo();
  if (type_info) {
    in_archive << (char) 1;
    if (id == DataTypeId::kList) {
      const auto& list_type_info = type_info->Cast<ListTypeInfo>();
      in_archive << list_type_info.child_type;
    } else if (id == DataTypeId::kStruct) {
      const auto& struct_type_info = type_info->Cast<StructTypeInfo>();
      const auto& child_types = struct_type_info.child_types;
      in_archive << (size_t) child_types.size();
      for (const auto& child_type : child_types) {
        in_archive << child_type;
      }
    } else if (id == DataTypeId::kVarchar) {
      const auto& varchar_type_info = type_info->Cast<StringTypeInfo>();
      in_archive << varchar_type_info.max_length;
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "unsupported data type with extra type info - " + type.ToString());
    }
  } else {
    in_archive << (char) 0;  // indicate no extra type info
  }
  return in_archive;
}

OutArchive& operator>>(OutArchive& out_archive, DataType& type) {
  DataTypeId id;
  out_archive >> id;

  char has_extra_type_info;
  out_archive >> has_extra_type_info;
  if (has_extra_type_info) {
    if (id == DataTypeId::kList) {
      DataType child_type;
      out_archive >> child_type;
      type = DataType::List(child_type);
    } else if (id == DataTypeId::kStruct) {
      size_t child_types_size;
      out_archive >> child_types_size;
      std::vector<DataType> child_types(child_types_size);
      for (size_t i = 0; i < child_types_size; ++i) {
        out_archive >> child_types[i];
      }
      type = DataType::Struct(child_types);
    } else if (id == DataTypeId::kVarchar) {
      size_t max_length;
      out_archive >> max_length;
      type = DataType::Varchar(max_length);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "unsupported data type with extra type info - " + std::to_string(id));
    }
  } else {
    type = DataType(id);
  }

  return out_archive;
}

InArchive& operator<<(InArchive& archive, const VertexSchema& v_schema) {
  archive << v_schema.label_name << v_schema.property_types
          << v_schema.property_names << v_schema.primary_keys
          << v_schema.default_property_values << v_schema.description
          << v_schema.max_num << v_schema.vprop_soft_deleted;
  return archive;
}

OutArchive& operator>>(OutArchive& archive, VertexSchema& v_schema) {
  archive >> v_schema.label_name >> v_schema.property_types >>
      v_schema.property_names >> v_schema.primary_keys >>
      v_schema.default_property_values >> v_schema.description >>
      v_schema.max_num >> v_schema.vprop_soft_deleted;
  process_default_values(v_schema.default_property_values,
                         v_schema.default_property_strings);
  return archive;
}

InArchive& operator<<(InArchive& archive, const EdgeSchema& e_schema) {
  archive << e_schema.src_label_name << e_schema.dst_label_name
          << e_schema.edge_label_name << e_schema.sort_on_compaction
          << e_schema.description << e_schema.ie_mutable << e_schema.oe_mutable
          << e_schema.ie_strategy << e_schema.oe_strategy << e_schema.properties
          << e_schema.property_names << e_schema.default_property_values
          << e_schema.eprop_soft_deleted;
  return archive;
}

OutArchive& operator>>(OutArchive& archive, EdgeSchema& e_schema) {
  archive >> e_schema.src_label_name >> e_schema.dst_label_name >>
      e_schema.edge_label_name >> e_schema.sort_on_compaction >>
      e_schema.description >> e_schema.ie_mutable >> e_schema.oe_mutable >>
      e_schema.ie_strategy >> e_schema.oe_strategy >> e_schema.properties >>
      e_schema.property_names >> e_schema.default_property_values >>
      e_schema.eprop_soft_deleted;
  process_default_values(e_schema.default_property_values,
                         e_schema.default_property_strings);
  return archive;
}

}  // namespace neug
