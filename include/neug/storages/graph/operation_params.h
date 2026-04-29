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
#pragma once

#include <string>
#include <tuple>
#include <vector>
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace neug {
class InArchive;
class OutArchive;

class CreateVertexTypeParam {
 private:
  std::string vertex_label_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  std::vector<std::string> primary_key_names;
  CreateVertexTypeParam() = default;
  friend class CreateVertexTypeParamBuilder;

 public:
  const std::string& GetVertexLabel() const { return vertex_label_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }
  const std::vector<std::string>& GetPrimaryKeyNames() const {
    return primary_key_names;
  }

  void Serialize(InArchive& arc) const;
  static CreateVertexTypeParam Deserialize(OutArchive& arc);
};

class CreateVertexTypeParamBuilder {
 public:
  CreateVertexTypeParam config;
  CreateVertexTypeParamBuilder() = default;
  CreateVertexTypeParamBuilder& VertexLabel(
      const std::string& vertex_label_name) {
    config.vertex_label_name = vertex_label_name;
    return *this;
  }

  CreateVertexTypeParamBuilder& Properties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  CreateVertexTypeParamBuilder& AddProperty(const DataType& type,
                                            const std::string& name,
                                            const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  CreateVertexTypeParamBuilder& PrimaryKeyNames(
      const std::vector<std::string>& primary_key_names) {
    config.primary_key_names = primary_key_names;
    return *this;
  }

  CreateVertexTypeParamBuilder& AddPrimaryKeyName(
      const std::string& primary_key_name) {
    config.primary_key_names.emplace_back(primary_key_name);
    return *this;
  }

  CreateVertexTypeParam Build() {
    if (config.vertex_label_name.empty()) {
      LOG(ERROR) << "Vertex label cannot be empty.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex label cannot be empty.");
    }
    if (config.primary_key_names.empty()) {
      LOG(ERROR) << "Primary key names cannot be empty.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Primary key names cannot be empty.");
    }
    return std::move(config);
  }
};

class CreateEdgeTypeParam {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  EdgeStrategy oe_edge_strategy;
  EdgeStrategy ie_edge_strategy;
  CreateEdgeTypeParam() = default;
  friend class CreateEdgeTypeParamBuilder;

 public:
  const std::string& GetSrcLabel() const { return src_label_name; }
  const std::string& GetDstLabel() const { return dst_label_name; }
  const std::string& GetEdgeLabel() const { return edge_label_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }
  EdgeStrategy GetOEEdgeStrategy() const { return oe_edge_strategy; }
  EdgeStrategy GetIEEdgeStrategy() const { return ie_edge_strategy; }

  void Serialize(InArchive& arc) const;
  static CreateEdgeTypeParam Deserialize(OutArchive& arc);
};

class CreateEdgeTypeParamBuilder {
 public:
  CreateEdgeTypeParam config;
  CreateEdgeTypeParamBuilder() {
    config.oe_edge_strategy = EdgeStrategy::kMultiple;
    config.ie_edge_strategy = EdgeStrategy::kMultiple;
  }
  CreateEdgeTypeParamBuilder& SrcLabel(const std::string& src_label) {
    config.src_label_name = src_label;
    return *this;
  }

  CreateEdgeTypeParamBuilder& DstLabel(const std::string& dst_label) {
    config.dst_label_name = dst_label;
    return *this;
  }

  CreateEdgeTypeParamBuilder& EdgeLabel(const std::string& edge_label) {
    config.edge_label_name = edge_label;
    return *this;
  }

  CreateEdgeTypeParamBuilder& Properties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  CreateEdgeTypeParamBuilder& AddProperty(const DataType& type,
                                          const std::string& name,
                                          const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  CreateEdgeTypeParamBuilder& OEEdgeStrategy(EdgeStrategy oe_edge_strategy) {
    config.oe_edge_strategy = oe_edge_strategy;
    return *this;
  }

  CreateEdgeTypeParamBuilder& IEEdgeStrategy(EdgeStrategy ie_edge_strategy) {
    config.ie_edge_strategy = ie_edge_strategy;
    return *this;
  }

  CreateEdgeTypeParam Build() {
    if (config.src_label_name.empty()) {
      LOG(ERROR) << "Source label must be specified.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Source label must be specified.");
    }
    if (config.dst_label_name.empty()) {
      LOG(ERROR) << "Destination label must be specified.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Destination label must be specified.");
    }
    if (config.edge_label_name.empty()) {
      LOG(ERROR) << "Edge label must be specified.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Edge label must be specified.");
    }
    return std::move(config);
  }
};

class AddVertexPropertiesParam {
 private:
  std::string vertex_label_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  AddVertexPropertiesParam() = default;
  friend class AddVertexPropertiesParamBuilder;

 public:
  const std::string& GetVertexLabel() const { return vertex_label_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }

  void Serialize(InArchive& arc) const;
  static AddVertexPropertiesParam Deserialize(OutArchive& arc);
};

class AddVertexPropertiesParamBuilder {
 public:
  AddVertexPropertiesParam config;
  AddVertexPropertiesParamBuilder() = default;
  AddVertexPropertiesParamBuilder& VertexLabel(
      const std::string& vertex_label_name) {
    config.vertex_label_name = vertex_label_name;
    return *this;
  }

  AddVertexPropertiesParamBuilder& Properties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  AddVertexPropertiesParamBuilder& AddProperty(const DataType& type,
                                               const std::string& name,
                                               const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  AddVertexPropertiesParam Build() {
    if (config.vertex_label_name.empty()) {
      LOG(ERROR) << "Vertex label must be specified.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex label must be specified.");
    }
    return std::move(config);
  }
};

class AddEdgePropertiesParam {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  AddEdgePropertiesParam() = default;
  friend class AddEdgePropertiesParamBuilder;

 public:
  const std::string& GetSrcLabel() const { return src_label_name; }
  const std::string& GetDstLabel() const { return dst_label_name; }
  const std::string& GetEdgeLabel() const { return edge_label_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }

  void Serialize(InArchive& arc) const;
  static AddEdgePropertiesParam Deserialize(OutArchive& arc);
};

class AddEdgePropertiesParamBuilder {
 public:
  AddEdgePropertiesParam config;
  AddEdgePropertiesParamBuilder() = default;

  AddEdgePropertiesParamBuilder& SrcLabel(const std::string& src_label_name) {
    config.src_label_name = src_label_name;
    return *this;
  }

  AddEdgePropertiesParamBuilder& DstLabel(const std::string& dst_label_name) {
    config.dst_label_name = dst_label_name;
    return *this;
  }

  AddEdgePropertiesParamBuilder& EdgeLabel(const std::string& edge_label_name) {
    config.edge_label_name = edge_label_name;
    return *this;
  }

  AddEdgePropertiesParamBuilder& Properties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  AddEdgePropertiesParamBuilder& AddProperty(const DataType& type,
                                             const std::string& name,
                                             const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  AddEdgePropertiesParam Build() {
    if (config.src_label_name.empty() || config.dst_label_name.empty() ||
        config.edge_label_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge labels must be specified.");
    }
    return std::move(config);
  }
};

class RenameVertexPropertiesParam {
 private:
  std::string vertex_label_name;
  std::vector<std::pair<std::string, std::string>> rename_properties;
  RenameVertexPropertiesParam() = default;
  friend class RenameVertexPropertiesParamBuilder;

 public:
  const std::string& GetVertexLabel() const { return vertex_label_name; }
  const std::vector<std::pair<std::string, std::string>>& GetRenameProperties()
      const {
    return rename_properties;
  }

  void Serialize(InArchive& arc) const;
  static RenameVertexPropertiesParam Deserialize(OutArchive& arc);
};

class RenameVertexPropertiesParamBuilder {
 public:
  RenameVertexPropertiesParam config;
  RenameVertexPropertiesParamBuilder() = default;

  RenameVertexPropertiesParamBuilder& VertexLabel(
      const std::string& vertex_label_name) {
    config.vertex_label_name = vertex_label_name;
    return *this;
  }

  RenameVertexPropertiesParamBuilder& RenameProperties(
      const std::vector<std::pair<std::string, std::string>>&
          rename_properties) {
    config.rename_properties = rename_properties;
    return *this;
  }

  RenameVertexPropertiesParamBuilder& AddRenameProperty(
      const std::string& old_name, const std::string& new_name) {
    config.rename_properties.emplace_back(old_name, new_name);
    return *this;
  }

  RenameVertexPropertiesParam Build() {
    if (config.vertex_label_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex label must be specified.");
    }
    return std::move(config);
  }
};

class RenameEdgePropertiesParam {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::pair<std::string, std::string>> rename_properties;
  RenameEdgePropertiesParam() = default;
  friend class RenameEdgePropertiesParamBuilder;

 public:
  const std::string& GetSrcLabel() const { return src_label_name; }
  const std::string& GetDstLabel() const { return dst_label_name; }
  const std::string& GetEdgeLabel() const { return edge_label_name; }
  const std::vector<std::pair<std::string, std::string>>& GetRenameProperties()
      const {
    return rename_properties;
  }

  void Serialize(InArchive& arc) const;
  static RenameEdgePropertiesParam Deserialize(OutArchive& arc);
};

class RenameEdgePropertiesParamBuilder {
 public:
  RenameEdgePropertiesParam config;
  RenameEdgePropertiesParamBuilder() = default;

  RenameEdgePropertiesParamBuilder& SrcLabel(
      const std::string& src_label_name) {
    config.src_label_name = src_label_name;
    return *this;
  }

  RenameEdgePropertiesParamBuilder& DstLabel(
      const std::string& dst_label_name) {
    config.dst_label_name = dst_label_name;
    return *this;
  }

  RenameEdgePropertiesParamBuilder& EdgeLabel(
      const std::string& edge_label_name) {
    config.edge_label_name = edge_label_name;
    return *this;
  }

  RenameEdgePropertiesParamBuilder& RenameProperties(
      const std::vector<std::pair<std::string, std::string>>&
          rename_properties) {
    config.rename_properties = rename_properties;
    return *this;
  }

  RenameEdgePropertiesParamBuilder& AddRenameProperty(
      const std::string& old_name, const std::string& new_name) {
    config.rename_properties.emplace_back(old_name, new_name);
    return *this;
  }

  RenameEdgePropertiesParam Build() {
    if (config.src_label_name.empty() || config.dst_label_name.empty() ||
        config.edge_label_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge labels must be specified.");
    }
    return std::move(config);
  }
};

class DeleteVertexPropertiesParam {
 private:
  std::string vertex_label_name;
  std::vector<std::string> delete_properties;
  DeleteVertexPropertiesParam() = default;
  friend class DeleteVertexPropertiesParamBuilder;

 public:
  const std::string& GetVertexLabel() const { return vertex_label_name; }
  const std::vector<std::string>& GetDeleteProperties() const {
    return delete_properties;
  }

  void Serialize(InArchive& arc) const;
  static DeleteVertexPropertiesParam Deserialize(OutArchive& arc);
};

class DeleteVertexPropertiesParamBuilder {
 public:
  DeleteVertexPropertiesParam config;
  DeleteVertexPropertiesParamBuilder() = default;

  DeleteVertexPropertiesParamBuilder& VertexLabel(
      const std::string& vertex_label_name) {
    config.vertex_label_name = vertex_label_name;
    return *this;
  }

  DeleteVertexPropertiesParamBuilder& DeleteProperties(
      const std::vector<std::string>& delete_properties) {
    config.delete_properties = delete_properties;
    return *this;
  }

  DeleteVertexPropertiesParamBuilder& AddDeleteProperty(
      const std::string& property_name) {
    config.delete_properties.emplace_back(property_name);
    return *this;
  }

  DeleteVertexPropertiesParam Build() {
    if (config.vertex_label_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex label must be specified.");
    }
    return std::move(config);
  }
};

class DeleteEdgePropertiesParam {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::string> delete_properties;
  DeleteEdgePropertiesParam() = default;
  friend class DeleteEdgePropertiesParamBuilder;

 public:
  const std::string& GetSrcLabel() const { return src_label_name; }
  const std::string& GetDstLabel() const { return dst_label_name; }
  const std::string& GetEdgeLabel() const { return edge_label_name; }
  const std::vector<std::string>& GetDeleteProperties() const {
    return delete_properties;
  }

  void Serialize(InArchive& arc) const;
  static DeleteEdgePropertiesParam Deserialize(OutArchive& arc);
};

class DeleteEdgePropertiesParamBuilder {
 public:
  DeleteEdgePropertiesParam config;
  DeleteEdgePropertiesParamBuilder() = default;

  DeleteEdgePropertiesParamBuilder& SrcLabel(
      const std::string& src_label_name) {
    config.src_label_name = src_label_name;
    return *this;
  }

  DeleteEdgePropertiesParamBuilder& DstLabel(
      const std::string& dst_label_name) {
    config.dst_label_name = dst_label_name;
    return *this;
  }

  DeleteEdgePropertiesParamBuilder& EdgeLabel(
      const std::string& edge_label_name) {
    config.edge_label_name = edge_label_name;
    return *this;
  }

  DeleteEdgePropertiesParamBuilder& DeleteProperties(
      const std::vector<std::string>& delete_properties) {
    config.delete_properties = delete_properties;
    return *this;
  }

  DeleteEdgePropertiesParamBuilder& AddDeleteProperty(
      const std::string& property_name) {
    config.delete_properties.emplace_back(property_name);
    return *this;
  }

  DeleteEdgePropertiesParam Build() {
    if (config.src_label_name.empty() || config.dst_label_name.empty() ||
        config.edge_label_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge labels must be specified.");
    }
    return std::move(config);
  }
};

}  // namespace neug