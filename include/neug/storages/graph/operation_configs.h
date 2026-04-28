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
#include "neug/execution/common/types/value.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace neug {
class InArchive;
class OutArchive;

class CreateVertexTypeConfig {
 private:
  std::string vertex_type_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  std::vector<std::string> primary_key_names;
  CreateVertexTypeConfig() = default;
  friend class CreateVertexTypeConfigBuilder;

 public:
  const std::string& GetVertexTypeName() const { return vertex_type_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }
  const std::vector<std::string>& GetPrimaryKeyNames() const {
    return primary_key_names;
  }

  void Serialize(InArchive& arc) const;
  static CreateVertexTypeConfig Deserialize(OutArchive& arc);
};

class CreateVertexTypeConfigBuilder {
 public:
  CreateVertexTypeConfig config;
  CreateVertexTypeConfigBuilder() = default;
  CreateVertexTypeConfigBuilder& WithVertexTypeName(
      const std::string& vertex_type_name) {
    config.vertex_type_name = vertex_type_name;
    return *this;
  }

  CreateVertexTypeConfigBuilder& WithProperties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  CreateVertexTypeConfigBuilder& WithProperty(const DataType& type,
                                              const std::string& name,
                                              const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  CreateVertexTypeConfigBuilder& WithPrimaryKeyNames(
      const std::vector<std::string>& primary_key_names) {
    config.primary_key_names = primary_key_names;
    return *this;
  }

  CreateVertexTypeConfigBuilder& WithPrimaryKeyName(
      const std::string& primary_key_name) {
    config.primary_key_names.emplace_back(primary_key_name);
    return *this;
  }

  CreateVertexTypeConfig Build() {
    if (config.vertex_type_name.empty()) {
      LOG(ERROR) << "Vertex type name cannot be empty.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex type name cannot be empty.");
    }
    if (config.primary_key_names.empty()) {
      LOG(ERROR) << "Primary key names cannot be empty.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Primary key names cannot be empty.");
    }
    return std::move(config);
  }
};

class CreateEdgeTypeConfig {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  EdgeStrategy oe_edge_strategy;
  EdgeStrategy ie_edge_strategy;
  CreateEdgeTypeConfig() = default;
  friend class CreateEdgeTypeConfigBuilder;

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
  static CreateEdgeTypeConfig Deserialize(OutArchive& arc);
};

class CreateEdgeTypeConfigBuilder {
 public:
  CreateEdgeTypeConfig config;
  CreateEdgeTypeConfigBuilder() {
    config.oe_edge_strategy = EdgeStrategy::kMultiple;
    config.ie_edge_strategy = EdgeStrategy::kMultiple;
  }
  CreateEdgeTypeConfigBuilder& WithSrcLabel(const std::string& src_label) {
    config.src_label_name = src_label;
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithDstLabel(const std::string& dst_label) {
    config.dst_label_name = dst_label;
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithEdgeLabel(const std::string& edge_label) {
    config.edge_label_name = edge_label;
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithProperties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithProperty(const DataType& type,
                                            const std::string& name,
                                            const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithOEEdgeStrategy(
      EdgeStrategy oe_edge_strategy) {
    config.oe_edge_strategy = oe_edge_strategy;
    return *this;
  }

  CreateEdgeTypeConfigBuilder& WithIEEdgeStrategy(
      EdgeStrategy ie_edge_strategy) {
    config.ie_edge_strategy = ie_edge_strategy;
    return *this;
  }

  CreateEdgeTypeConfig Build() {
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

class AddVertexPropertiesConfig {
 private:
  std::string vertex_type_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  AddVertexPropertiesConfig() = default;
  friend class AddVertexPropertiesConfigBuilder;

 public:
  const std::string& GetVertexTypeName() const { return vertex_type_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }

  void Serialize(InArchive& arc) const;
  static AddVertexPropertiesConfig Deserialize(OutArchive& arc);
};

class AddVertexPropertiesConfigBuilder {
 public:
  AddVertexPropertiesConfig config;
  AddVertexPropertiesConfigBuilder() = default;
  AddVertexPropertiesConfigBuilder& WithVertexTypeName(
      const std::string& vertex_type_name) {
    config.vertex_type_name = vertex_type_name;
    return *this;
  }

  AddVertexPropertiesConfigBuilder& WithProperties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  AddVertexPropertiesConfigBuilder& WithProperty(const DataType& type,
                                                 const std::string& name,
                                                 const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  AddVertexPropertiesConfig Build() {
    if (config.vertex_type_name.empty()) {
      LOG(ERROR) << "Vertex type name must be specified.";
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex type name must be specified.");
    }
    return std::move(config);
  }
};

class AddEdgePropertiesConfig {
 private:
  std::string src_type_name;
  std::string dst_type_name;
  std::string edge_type_name;
  std::vector<std::tuple<DataType, std::string, Property>> properties;
  AddEdgePropertiesConfig() = default;
  friend class AddEdgePropertiesConfigBuilder;

 public:
  const std::string& GetSrcTypeName() const { return src_type_name; }
  const std::string& GetDstTypeName() const { return dst_type_name; }
  const std::string& GetEdgeTypeName() const { return edge_type_name; }
  const std::vector<std::tuple<DataType, std::string, Property>>&
  GetProperties() const {
    return properties;
  }

  void Serialize(InArchive& arc) const;
  static AddEdgePropertiesConfig Deserialize(OutArchive& arc);
};

class AddEdgePropertiesConfigBuilder {
 public:
  AddEdgePropertiesConfig config;
  AddEdgePropertiesConfigBuilder() = default;

  AddEdgePropertiesConfigBuilder& WithSrcTypeName(
      const std::string& src_type_name) {
    config.src_type_name = src_type_name;
    return *this;
  }

  AddEdgePropertiesConfigBuilder& WithDstTypeName(
      const std::string& dst_type_name) {
    config.dst_type_name = dst_type_name;
    return *this;
  }

  AddEdgePropertiesConfigBuilder& WithEdgeTypeName(
      const std::string& edge_type_name) {
    config.edge_type_name = edge_type_name;
    return *this;
  }

  AddEdgePropertiesConfigBuilder& WithProperties(
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties) {
    config.properties = properties;
    return *this;
  }

  AddEdgePropertiesConfigBuilder& WithProperty(const DataType& type,
                                               const std::string& name,
                                               const Property& value) {
    config.properties.emplace_back(type, name, value);
    return *this;
  }

  AddEdgePropertiesConfig Build() {
    if (config.src_type_name.empty() || config.dst_type_name.empty() ||
        config.edge_type_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge type names must be specified.");
    }
    return std::move(config);
  }
};

class RenameVertexPropertiesConfig {
 private:
  std::string vertex_type_name;
  std::vector<std::pair<std::string, std::string>> rename_properties;
  RenameVertexPropertiesConfig() = default;
  friend class RenameVertexPropertiesConfigBuilder;

 public:
  const std::string& GetVertexTypeName() const { return vertex_type_name; }
  const std::vector<std::pair<std::string, std::string>>& GetRenameProperties()
      const {
    return rename_properties;
  }

  void Serialize(InArchive& arc) const;
  static RenameVertexPropertiesConfig Deserialize(OutArchive& arc);
};

class RenameVertexPropertiesConfigBuilder {
 public:
  RenameVertexPropertiesConfig config;
  RenameVertexPropertiesConfigBuilder() = default;

  RenameVertexPropertiesConfigBuilder& WithVertexTypeName(
      const std::string& vertex_type_name) {
    config.vertex_type_name = vertex_type_name;
    return *this;
  }

  RenameVertexPropertiesConfigBuilder& WithRenameProperties(
      const std::vector<std::pair<std::string, std::string>>&
          rename_properties) {
    config.rename_properties = rename_properties;
    return *this;
  }

  RenameVertexPropertiesConfigBuilder& WithRenameProperty(
      const std::string& old_name, const std::string& new_name) {
    config.rename_properties.emplace_back(old_name, new_name);
    return *this;
  }

  RenameVertexPropertiesConfig Build() {
    if (config.vertex_type_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex type name must be specified.");
    }
    return std::move(config);
  }
};

class RenameEdgePropertiesConfig {
 private:
  std::string src_type_name;
  std::string dst_type_name;
  std::string edge_type_name;
  std::vector<std::pair<std::string, std::string>> rename_properties;
  RenameEdgePropertiesConfig() = default;
  friend class RenameEdgePropertiesConfigBuilder;

 public:
  const std::string& GetSrcTypeName() const { return src_type_name; }
  const std::string& GetDstTypeName() const { return dst_type_name; }
  const std::string& GetEdgeTypeName() const { return edge_type_name; }
  const std::vector<std::pair<std::string, std::string>>& GetRenameProperties()
      const {
    return rename_properties;
  }

  void Serialize(InArchive& arc) const;
  static RenameEdgePropertiesConfig Deserialize(OutArchive& arc);
};

class RenameEdgePropertiesConfigBuilder {
 public:
  RenameEdgePropertiesConfig config;
  RenameEdgePropertiesConfigBuilder() = default;

  RenameEdgePropertiesConfigBuilder& WithSrcTypeName(
      const std::string& src_type_name) {
    config.src_type_name = src_type_name;
    return *this;
  }

  RenameEdgePropertiesConfigBuilder& WithDstTypeName(
      const std::string& dst_type_name) {
    config.dst_type_name = dst_type_name;
    return *this;
  }

  RenameEdgePropertiesConfigBuilder& WithEdgeTypeName(
      const std::string& edge_type_name) {
    config.edge_type_name = edge_type_name;
    return *this;
  }

  RenameEdgePropertiesConfigBuilder& WithRenameProperties(
      const std::vector<std::pair<std::string, std::string>>&
          rename_properties) {
    config.rename_properties = rename_properties;
    return *this;
  }

  RenameEdgePropertiesConfigBuilder& WithRenameProperty(
      const std::string& old_name, const std::string& new_name) {
    config.rename_properties.emplace_back(old_name, new_name);
    return *this;
  }

  RenameEdgePropertiesConfig Build() {
    if (config.src_type_name.empty() || config.dst_type_name.empty() ||
        config.edge_type_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge type names must be specified.");
    }
    return std::move(config);
  }
};

class DeleteVertexPropertiesConfig {
 private:
  std::string vertex_type_name;
  std::vector<std::string> delete_properties;
  DeleteVertexPropertiesConfig() = default;
  friend class DeleteVertexPropertiesConfigBuilder;

 public:
  const std::string& GetVertexTypeName() const { return vertex_type_name; }
  const std::vector<std::string>& GetDeleteProperties() const {
    return delete_properties;
  }

  void Serialize(InArchive& arc) const;
  static DeleteVertexPropertiesConfig Deserialize(OutArchive& arc);
};

class DeleteVertexPropertiesConfigBuilder {
 public:
  DeleteVertexPropertiesConfig config;
  DeleteVertexPropertiesConfigBuilder() = default;

  DeleteVertexPropertiesConfigBuilder& WithVertexTypeName(
      const std::string& vertex_type_name) {
    config.vertex_type_name = vertex_type_name;
    return *this;
  }

  DeleteVertexPropertiesConfigBuilder& WithDeleteProperties(
      const std::vector<std::string>& delete_properties) {
    config.delete_properties = delete_properties;
    return *this;
  }

  DeleteVertexPropertiesConfigBuilder& WithDeleteProperty(
      const std::string& property_name) {
    config.delete_properties.emplace_back(property_name);
    return *this;
  }

  DeleteVertexPropertiesConfig Build() {
    if (config.vertex_type_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex type name must be specified.");
    }
    return std::move(config);
  }
};

class DeleteEdgePropertiesConfig {
 private:
  std::string src_type_name;
  std::string dst_type_name;
  std::string edge_type_name;
  std::vector<std::string> delete_properties;
  DeleteEdgePropertiesConfig() = default;
  friend class DeleteEdgePropertiesConfigBuilder;

 public:
  const std::string& GetSrcTypeName() const { return src_type_name; }
  const std::string& GetDstTypeName() const { return dst_type_name; }
  const std::string& GetEdgeTypeName() const { return edge_type_name; }
  const std::vector<std::string>& GetDeleteProperties() const {
    return delete_properties;
  }

  void Serialize(InArchive& arc) const;
  static DeleteEdgePropertiesConfig Deserialize(OutArchive& arc);
};

class DeleteEdgePropertiesConfigBuilder {
 public:
  DeleteEdgePropertiesConfig config;
  DeleteEdgePropertiesConfigBuilder() = default;

  DeleteEdgePropertiesConfigBuilder& WithSrcTypeName(
      const std::string& src_type_name) {
    config.src_type_name = src_type_name;
    return *this;
  }

  DeleteEdgePropertiesConfigBuilder& WithDstTypeName(
      const std::string& dst_type_name) {
    config.dst_type_name = dst_type_name;
    return *this;
  }

  DeleteEdgePropertiesConfigBuilder& WithEdgeTypeName(
      const std::string& edge_type_name) {
    config.edge_type_name = edge_type_name;
    return *this;
  }

  DeleteEdgePropertiesConfigBuilder& WithDeleteProperties(
      const std::vector<std::string>& delete_properties) {
    config.delete_properties = delete_properties;
    return *this;
  }

  DeleteEdgePropertiesConfigBuilder& WithDeleteProperty(
      const std::string& property_name) {
    config.delete_properties.emplace_back(property_name);
    return *this;
  }

  DeleteEdgePropertiesConfig Build() {
    if (config.src_type_name.empty() || config.dst_type_name.empty() ||
        config.edge_type_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source/Destination/Edge type names must be specified.");
    }
    return std::move(config);
  }
};

}  // namespace neug