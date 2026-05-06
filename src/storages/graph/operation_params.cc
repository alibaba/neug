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

#include "neug/storages/graph/operation_params.h"
#include "neug/common/extra_type_info.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

void CreateVertexTypeParam::Serialize(InArchive& arc) const {
  arc << vertex_label_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
  arc << static_cast<uint32_t>(primary_key_names.size());
  for (const auto& key : primary_key_names) {
    arc << key;
  }
}

CreateVertexTypeParam CreateVertexTypeParam::Deserialize(OutArchive& arc) {
  CreateVertexTypeParamBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.VertexLabel(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.AddProperty(type, name, default_value);
  }
  uint32_t key_size;
  arc >> key_size;
  for (size_t i = 0; i < key_size; ++i) {
    std::string key;
    arc >> key;
    builder.AddPrimaryKeyName(key);
  }
  return builder.Build();
}

void CreateEdgeTypeParam::Serialize(InArchive& arc) const {
  arc << src_label_name << dst_label_name << edge_label_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
  arc << oe_edge_strategy << ie_edge_strategy;
}

CreateEdgeTypeParam CreateEdgeTypeParam::Deserialize(OutArchive& arc) {
  CreateEdgeTypeParamBuilder builder;
  std::string src_label;
  std::string dst_label;
  std::string edge_label;
  arc >> src_label >> dst_label >> edge_label;
  builder.SrcLabel(src_label).DstLabel(dst_label).EdgeLabel(edge_label);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.AddProperty(type, name, default_value);
  }
  EdgeStrategy oe_edge_strategy, ie_edge_strategy;
  arc >> oe_edge_strategy >> ie_edge_strategy;
  builder.OEEdgeStrategy(oe_edge_strategy).IEEdgeStrategy(ie_edge_strategy);
  return builder.Build();
}

void AddVertexPropertiesParam::Serialize(InArchive& arc) const {
  arc << vertex_label_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

AddVertexPropertiesParam AddVertexPropertiesParam::Deserialize(
    OutArchive& arc) {
  AddVertexPropertiesParamBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.VertexLabel(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.AddProperty(type, name, default_value);
  }
  return builder.Build();
}

void AddEdgePropertiesParam::Serialize(InArchive& arc) const {
  arc << src_label_name << dst_label_name << edge_label_name;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

AddEdgePropertiesParam AddEdgePropertiesParam::Deserialize(OutArchive& arc) {
  AddEdgePropertiesParamBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.SrcLabel(src_type).DstLabel(dst_type).EdgeLabel(edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    DataType type;
    std::string name;
    Property default_value;
    arc >> type >> name >> default_value;
    builder.AddProperty(type, name, default_value);
  }
  return builder.Build();
}

void RenameVertexPropertiesParam::Serialize(InArchive& arc) const {
  arc << vertex_label_name;
  arc << static_cast<uint32_t>(rename_properties.size());
  for (const auto& [old_name, new_name] : rename_properties) {
    arc << old_name << new_name;
  }
}

RenameVertexPropertiesParam RenameVertexPropertiesParam::Deserialize(
    OutArchive& arc) {
  RenameVertexPropertiesParamBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.VertexLabel(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string old_name;
    std::string new_name;
    arc >> old_name >> new_name;
    builder.AddRenameProperty(old_name, new_name);
  }
  return builder.Build();
}

void RenameEdgePropertiesParam::Serialize(InArchive& arc) const {
  arc << src_label_name << dst_label_name << edge_label_name;
  arc << static_cast<uint32_t>(rename_properties.size());
  for (const auto& [old_name, new_name] : rename_properties) {
    arc << old_name << new_name;
  }
}

RenameEdgePropertiesParam RenameEdgePropertiesParam::Deserialize(
    OutArchive& arc) {
  RenameEdgePropertiesParamBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.SrcLabel(src_type).DstLabel(dst_type).EdgeLabel(edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string old_name;
    std::string new_name;
    arc >> old_name >> new_name;
    builder.AddRenameProperty(old_name, new_name);
  }
  return builder.Build();
}

void DeleteVertexPropertiesParam::Serialize(InArchive& arc) const {
  arc << vertex_label_name;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& prop_name : delete_properties) {
    arc << prop_name;
  }
}

DeleteVertexPropertiesParam DeleteVertexPropertiesParam::Deserialize(
    OutArchive& arc) {
  DeleteVertexPropertiesParamBuilder builder;
  std::string vertex_type;
  arc >> vertex_type;
  builder.VertexLabel(vertex_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string prop_name;
    arc >> prop_name;
    builder.AddDeleteProperty(prop_name);
  }
  return builder.Build();
}

void DeleteEdgePropertiesParam::Serialize(InArchive& arc) const {
  arc << src_label_name << dst_label_name << edge_label_name;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& prop_name : delete_properties) {
    arc << prop_name;
  }
}

DeleteEdgePropertiesParam DeleteEdgePropertiesParam::Deserialize(
    OutArchive& arc) {
  DeleteEdgePropertiesParamBuilder builder;
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  arc >> src_type >> dst_type >> edge_type;
  builder.SrcLabel(src_type).DstLabel(dst_type).EdgeLabel(edge_type);
  uint32_t prop_size;
  arc >> prop_size;
  for (size_t i = 0; i < prop_size; ++i) {
    std::string prop_name;
    arc >> prop_name;
    builder.AddDeleteProperty(prop_name);
  }
  return builder.Build();
}

}  // namespace neug