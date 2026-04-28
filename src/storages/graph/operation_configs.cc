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

#include "neug/storages/graph/operation_configs.h"
#include "neug/common/extra_type_info.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

void CreateVertexTypeConfig::Serialize(InArchive& arc) const {
  arc << vertex_type_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
  arc << static_cast<uint32_t>(primary_key_names.size());
  for (const auto& key : primary_key_names) {
    arc << key;
  }
}

CreateVertexTypeConfig CreateVertexTypeConfig::Deserialize(OutArchive& arc) {
  CreateVertexTypeConfigBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.WithVertexTypeName(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.WithProperty(type, name, default_value);
  }
  uint32_t key_size;
  arc >> key_size;
  for (size_t i = 0; i < key_size; ++i) {
    std::string key;
    arc >> key;
    builder.WithPrimaryKeyName(key);
  }
  return builder.Build();
}

void CreateEdgeTypeConfig::Serialize(InArchive& arc) const {
  arc << src_label_name << dst_label_name << edge_label_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
  arc << oe_edge_strategy << ie_edge_strategy;
}

CreateEdgeTypeConfig CreateEdgeTypeConfig::Deserialize(OutArchive& arc) {
  CreateEdgeTypeConfigBuilder builder;
  std::string src_label;
  std::string dst_label;
  std::string edge_label;
  arc >> src_label >> dst_label >> edge_label;
  builder.WithSrcLabel(src_label).WithDstLabel(dst_label).WithEdgeLabel(
      edge_label);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.WithProperty(type, name, default_value);
  }
  EdgeStrategy oe_edge_strategy, ie_edge_strategy;
  arc >> oe_edge_strategy >> ie_edge_strategy;
  builder.WithOEEdgeStrategy(oe_edge_strategy)
      .WithIEEdgeStrategy(ie_edge_strategy);
  return builder.Build();
}

void AddVertexPropertiesConfig::Serialize(InArchive& arc) const {
  arc << vertex_type_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

AddVertexPropertiesConfig AddVertexPropertiesConfig::Deserialize(
    OutArchive& arc) {
  AddVertexPropertiesConfigBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.WithVertexTypeName(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.WithProperty(type, name, default_value);
  }
  return builder.Build();
}

void AddEdgePropertiesConfig::Serialize(InArchive& arc) const {
  arc << src_type_name << dst_type_name << edge_type_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

AddEdgePropertiesConfig AddEdgePropertiesConfig::Deserialize(OutArchive& arc) {
  AddEdgePropertiesConfigBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.WithSrcTypeName(src_type).WithDstTypeName(dst_type).WithEdgeTypeName(
      edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.WithProperty(type, name, default_value);
  }
  return builder.Build();
}

void RenameVertexPropertiesConfig::Serialize(InArchive& arc) const {
  arc << vertex_type_name;
  arc << static_cast<uint32_t>(rename_properties.size());
  for (const auto& [old_name, new_name] : rename_properties) {
    arc << old_name << new_name;
  }
}

RenameVertexPropertiesConfig RenameVertexPropertiesConfig::Deserialize(
    OutArchive& arc) {
  RenameVertexPropertiesConfigBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.WithVertexTypeName(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string old_name;
    std::string new_name;
    arc >> old_name >> new_name;
    builder.WithRenameProperty(old_name, new_name);
  }
  return builder.Build();
}

void RenameEdgePropertiesConfig::Serialize(InArchive& arc) const {
  arc << src_type_name << dst_type_name << edge_type_name;
  arc << static_cast<uint32_t>(rename_properties.size());
  for (const auto& [old_name, new_name] : rename_properties) {
    arc << old_name << new_name;
  }
}

RenameEdgePropertiesConfig RenameEdgePropertiesConfig::Deserialize(
    OutArchive& arc) {
  RenameEdgePropertiesConfigBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.WithSrcTypeName(src_type).WithDstTypeName(dst_type).WithEdgeTypeName(
      edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string old_name;
    std::string new_name;
    arc >> old_name >> new_name;
    builder.WithRenameProperty(old_name, new_name);
  }
  return builder.Build();
}

void DeleteVertexPropertiesConfig::Serialize(InArchive& arc) const {
  arc << vertex_type_name;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& prop_name : delete_properties) {
    arc << prop_name;
  }
}

DeleteVertexPropertiesConfig DeleteVertexPropertiesConfig::Deserialize(
    OutArchive& arc) {
  DeleteVertexPropertiesConfigBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.WithVertexTypeName(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string prop_name;
    arc >> prop_name;
    builder.WithDeleteProperty(prop_name);
  }
  return builder.Build();
}

void DeleteEdgePropertiesConfig::Serialize(InArchive& arc) const {
  arc << src_type_name << dst_type_name << edge_type_name;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& prop_name : delete_properties) {
    arc << prop_name;
  }
}

DeleteEdgePropertiesConfig DeleteEdgePropertiesConfig::Deserialize(
    OutArchive& arc) {
  DeleteEdgePropertiesConfigBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.WithSrcTypeName(src_type).WithDstTypeName(dst_type).WithEdgeTypeName(
      edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string prop_name;
    arc >> prop_name;
    builder.WithDeleteProperty(prop_name);
  }
  return builder.Build();
}

}  // namespace neug