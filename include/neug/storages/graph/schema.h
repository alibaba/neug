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

#include <stddef.h>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "neug/utils/bitset.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace YAML {
class Node;
}

namespace neug {

class PropertyGraph;
class Schema;

inline void process_default_values(
    std::vector<Property>& default_property_values,
    std::vector<std::string>& default_property_strings) {
  // Keep the ownership of string default property in default_property_strings
  for (auto& prop : default_property_values) {
    if (prop.type() == DataTypeId::kVarchar && prop.as_string_view() != "") {
      default_property_strings.emplace_back(prop.as_string_view());
      prop.set_string_view(std::string_view(default_property_strings.back()));
    }
  }
}

/**
 * @brief Schema definition for a vertex type (label).
 *
 * VertexSchema contains all metadata for a vertex type including:
 * - Label name and description
 * - Property definitions (types, names, defaults)
 * - Primary key specification
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get vertex schema from Schema
 * const neug::Schema& schema = graph.schema();
 * label_t person_id = schema.get_vertex_label_id("Person");
 * auto vertex_schema = schema.get_vertex_schema(person_id);
 *
 * // Access schema information
 * std::cout << "Label: " << vertex_schema->label_name << std::endl;
 * for (size_t i = 0; i < vertex_schema->property_names.size(); ++i) {
 *     std::cout << "Property: " << vertex_schema->property_names[i] <<
 * std::endl;
 * }
 * @endcode
 *
 * @see EdgeSchema For edge type schema
 * @see Schema For complete graph schema management
 *
 * @since v0.1.0
 */
struct VertexSchema {
  VertexSchema() = default;

  /**
   * @brief Construct a VertexSchema with all properties.
   *
   * @param label_name_ Name of the vertex label
   * @param property_types_ Data types for each property
   * @param property_names_ Names for each property
   * @param primary_keys_ Primary key specification (type, name, index)
   * @param default_property_values_ Default values for properties
   * @param description_ Human-readable description
   * @param max_num_ Maximum number of vertices of this type
   *
   * @since v0.1.0
   */
  VertexSchema(const std::string& label_name_,
               const std::vector<DataType>& property_types_,
               const std::vector<std::string>& property_names_,
               const std::vector<std::tuple<DataType, std::string, size_t>>&
                   primary_keys_,
               const std::vector<Property>& default_property_values_ = {},
               const std::string& description_ = "",
               size_t max_num_ = static_cast<size_t>(1) << 32)
      : label_name(label_name_),
        property_types(property_types_),
        property_names(property_names_),
        primary_keys(primary_keys_),
        default_property_values(default_property_values_),
        description(description_),
        max_num(max_num_) {
    vprop_soft_deleted.resize(property_names_.size(), false);
    if (default_property_values.empty()) {
      for (size_t i = 0; i < property_types_.size(); ++i) {
        default_property_values.emplace_back(
            get_default_value(property_types_[i].id()));
      }
    }
    assert(property_types.size() == property_names.size());
    assert(property_types.size() == default_property_values.size());
    process_default_values(default_property_values, default_property_strings);
  }

  void clear();

  inline bool empty() const { return primary_keys.empty(); }

  void add_properties(const std::vector<std::string>& names,
                      const std::vector<DataType>& types,
                      const std::vector<Property>& default_values = {});

  void set_properties(const std::vector<DataType>& types,
                      const std::vector<Property>& default_values = {});

  void rename_properties(const std::vector<std::string>& names,
                         const std::vector<std::string>& renames);

  void delete_properties(const std::vector<std::string>& names,
                         bool is_soft = false);

  void revert_delete_properties(const std::vector<std::string>& names);

  bool is_property_soft_deleted(const std::string& prop) const;

  /**
   * @brief Get the property index. If the property is a primary key, throw an
   * exception.
   */
  int32_t get_property_index(const std::string& prop) const;

  /**
   * @brief Get the property name by index. Primary key properties are not
   * counted.
   */
  std::string get_property_name(size_t index) const;

  bool has_property(const std::string& prop) const;

  const std::vector<Property>& get_default_property_values() const {
    return default_property_values;
  }

  static bool is_pk_same(const VertexSchema& lhs, const VertexSchema& rhs);

  std::string label_name;
  std::vector<DataType> property_types;
  std::vector<std::string> property_names;
  // <DataType, property_name, index_in_property_list>
  std::vector<std::tuple<DataType, std::string, size_t>> primary_keys;
  std::vector<Property> default_property_values;
  std::vector<std::string> default_property_strings;
  std::string description;
  size_t max_num;

  // Mark whether the vertex property is soft deleted
  std::vector<bool> vprop_soft_deleted;

 private:
  bool has_property_internal(const std::string& prop) const;

  friend class Schema;
};

/**
 * @brief Schema definition for an edge type (relationship).
 *
 * EdgeSchema defines an edge type connecting source and destination
 * vertex types. It includes:
 * - Edge label name and endpoints (src -> dst)
 * - Property definitions
 * - Mutability settings
 *
 * **Edge Strategies:**
 * - `kMultiple`: Multiple edges allowed between same vertex pair
 * - `kSingle`: At most one edge between same vertex pair
 * - `kNone`: No edges stored in this direction
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get edge schema
 * const neug::Schema& schema = graph.schema();
 * label_t person = schema.get_vertex_label_id("Person");
 * label_t knows = schema.get_edge_label_id("KNOWS");
 * auto edge_schema = schema.get_edge_schema(person, person, knows);
 *
 * // Access edge schema info
 * std::cout << edge_schema->src_label_name << " -> "
 *           << edge_schema->dst_label_name << std::endl;
 * @endcode
 *
 * @see VertexSchema For vertex type schema
 * @see Schema For complete graph schema management
 *
 * @since v0.1.0
 */
struct EdgeSchema {
  EdgeSchema() = default;

  /**
   * @brief Construct an EdgeSchema with full configuration.
   *
   * @param src_label_name_ Source vertex type name
   * @param dst_label_name_ Destination vertex type name
   * @param edge_label_name_ Edge label name
   * @param sort_on_compaction_ Sort edges during compaction
   * @param description_ Human-readable description
   * @param ie_mutable_ Incoming edges can be modified
   * @param oe_mutable_ Outgoing edges can be modified
   * @param oe_strategy_ Outgoing edge storage strategy
   * @param ie_strategy_ Incoming edge storage strategy
   * @param properties_ Data types for edge properties
   * @param property_names_ Names for edge properties
   * @param default_property_values_ Default values for properties
   *
   * @since v0.1.0
   */
  EdgeSchema(const std::string& src_label_name_,
             const std::string& dst_label_name_,
             const std::string& edge_label_name_, bool sort_on_compaction_,
             const std::string& description_, bool ie_mutable_,
             bool oe_mutable_, EdgeStrategy oe_strategy_,
             EdgeStrategy ie_strategy_,
             const std::vector<DataType>& properties_,
             const std::vector<std::string>& property_names_,
             const std::vector<Property>& default_property_values_ = {})
      : src_label_name(src_label_name_),
        dst_label_name(dst_label_name_),
        edge_label_name(edge_label_name_),
        sort_on_compaction(sort_on_compaction_),
        description(description_),
        ie_mutable(ie_mutable_),
        oe_mutable(oe_mutable_),
        oe_strategy(oe_strategy_),
        ie_strategy(ie_strategy_),
        properties(properties_),
        property_names(property_names_),
        default_property_values(default_property_values_) {
    eprop_soft_deleted.resize(property_names_.size(), false);
    assert(properties.size() == property_names.size());
    if (default_property_values.empty()) {
      for (size_t i = 0; i < properties_.size(); ++i) {
        default_property_values.emplace_back(
            get_default_value(properties_[i].id()));
      }
    }
    assert(properties.size() == default_property_values.size());
    CHECK(properties.size() == property_names.size());
    process_default_values(default_property_values, default_property_strings);
  }

  bool is_bundled() const;

  bool empty() const { return src_label_name.empty(); }

  bool has_property(const std::string& prop) const;

  void add_properties(const std::vector<std::string>& names,
                      const std::vector<DataType>& types,
                      const std::vector<Property>& default_values = {});

  void rename_properties(const std::vector<std::string>& names,
                         const std::vector<std::string>& renames);

  void delete_properties(const std::vector<std::string>& names,
                         bool is_soft = false);

  bool is_property_soft_deleted(const std::string& prop) const;

  void revert_delete_properties(const std::vector<std::string>& names);

  std::string get_property_name(size_t index) const;

  int32_t get_property_index(const std::string& prop) const;

  const std::vector<Property>& get_default_property_values() const {
    return default_property_values;
  }

  std::string src_label_name, dst_label_name, edge_label_name;
  bool sort_on_compaction;
  std::string description;
  bool ie_mutable;
  bool oe_mutable;
  EdgeStrategy oe_strategy;
  EdgeStrategy ie_strategy;
  std::vector<DataType> properties;
  std::vector<std::string> property_names;
  std::vector<Property> default_property_values;
  std::vector<std::string> default_property_strings;

  // Mark whether the edge property is soft deleted
  std::vector<bool> eprop_soft_deleted;

 private:
  bool has_property_internal(const std::string& prop) const;

  friend class Schema;
};

/**
 * @brief Graph schema manager for vertex and edge type definitions.
 *
 * Schema manages the complete type system for a property graph, including:
 * - Vertex types (labels) with properties and primary keys
 * - Edge types with source/destination types and properties
 * - Type lookups and validation
 * - Serialization to/from YAML format
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Access schema from PropertyGraph
 * const neug::Schema& schema = graph.schema();
 *
 * // Get vertex type information
 * if (schema.contains_vertex_label("Person")) {
 *     label_t person_id = schema.get_vertex_label_id("Person");
 *     auto props = schema.get_vertex_properties(person_id);
 *     auto names = schema.get_vertex_property_names(person_id);
 * }
 *
 * // Check edge type existence
 * if (schema.exist("Person", "Person", "KNOWS")) {
 *     auto edge_props = schema.get_edge_properties("Person", "Person",
 * "KNOWS");
 * }
 *
 * // Load schema from YAML file
 * auto result = neug::Schema::LoadFromYaml("/path/to/schema.yaml");
 * if (result.has_value()) {
 *     neug::Schema schema = result.value();
 * }
 * @endcode
 *
 * **Schema File Format (YAML):**
 * @code{.yaml}
 * graph:
 *   vertex_types:
 *     - type_name: Person
 *       properties:
 *         - property_name: id
 *           property_type: INT64
 *         - property_name: name
 *           property_type: VARCHAR
 *       primary_keys: [id]
 *   edge_types:
 *     - type_name: KNOWS
 *       src_type: Person
 *       dst_type: Person
 * @endcode
 *
 * @note Schema is immutable during query execution.
 * @note Use DDL operations (CREATE, DROP) to modify schema.
 *
 * @see VertexSchema For vertex type details
 * @see EdgeSchema For edge type details
 *
 * @since v0.1.0
 */
class Schema {
 public:
  /// @name Plugin ID Constants
  /// @{
  /// Number of reserved built-in plugins
  static constexpr uint8_t RESERVED_PLUGIN_NUM = 1;
  static constexpr uint8_t MAX_PLUGIN_ID = 245;
  static constexpr uint8_t DDL_PLUGIN_ID = 254;
  static constexpr uint8_t ADHOC_READ_PLUGIN_ID = 253;
  static constexpr uint8_t ADHOC_UPDATE_PLUGIN_ID = 252;
  static constexpr uint8_t CYPHER_READ_PLUGIN_ID = 248;
  static constexpr uint8_t CYPHER_READ_DEBUG_PLUGIN_ID = 246;

  static constexpr const char* DDL_PLUGIN_ID_STR = "\xFE";
  static constexpr const char* ADHOC_READ_PLUGIN_ID_STR = "\xFD";
  static constexpr const char* ADHOC_UPDATE_PLUGIN_ID_STR = "\xFC";
  static constexpr const char* CYPHER_READ_PLUGIN_ID_STR = "\xF8";
  static constexpr const char* CYPHER_WRITE_PLUGIN_ID_STR = "\xF7";
  static constexpr const char* CYPHER_READ_DEBUG_PLUGIN_ID_STR = "\xF6";
  static constexpr const char* PRIMITIVE_TYPE_KEY = "primitive_type";
  static constexpr const char* VARCHAR_KEY = "varchar";
  static constexpr const char* MAX_LENGTH_KEY = "max_length";

  static constexpr const size_t MAX_VNUM = static_cast<size_t>(1) << 32;

  using label_type = label_t;
  Schema();
  ~Schema();

  void Clear();

  bool Empty() const {
    return vlabel_indexer_.empty() && elabel_indexer_.empty();
  }

  inline const std::vector<std::shared_ptr<VertexSchema>>&
  get_all_vertex_schemas() const {
    return v_schemas_;
  }

  inline const std::unordered_map<uint32_t, std::shared_ptr<EdgeSchema>>&
  get_all_edge_schemas() const {
    return e_schemas_;
  }

  std::shared_ptr<const VertexSchema> get_vertex_schema(label_t label) const {
    return v_schemas_.at(label);
  }

  std::shared_ptr<VertexSchema> get_vertex_schema(label_t label) {
    return v_schemas_.at(label);
  }

  std::shared_ptr<const EdgeSchema> get_edge_schema(label_t src_label,
                                                    label_t dst_label,
                                                    label_t edge_label) const {
    auto key = generate_edge_label(src_label, dst_label, edge_label);
    assert(e_schemas_.count(key) > 0);
    return e_schemas_.at(key);
  }

  std::shared_ptr<EdgeSchema> get_edge_schema(label_t src_label,
                                              label_t dst_label,
                                              label_t edge_label) {
    auto key = generate_edge_label(src_label, dst_label, edge_label);
    assert(e_schemas_.count(key) > 0);
    return e_schemas_.at(key);
  }

  void AddVertexLabel(
      const std::string& label, const std::vector<DataType>& property_types,
      const std::vector<std::string>& property_names,
      const std::vector<std::tuple<DataType, std::string, size_t>>& primary_key,
      size_t max_vnum = static_cast<size_t>(1) << 32,
      const std::string& description = "",
      const std::vector<Property>& default_property_values = {});

  void AddEdgeLabel(const std::string& src_label, const std::string& dst_label,
                    const std::string& edge_label,
                    const std::vector<DataType>& properties,
                    const std::vector<std::string>& prop_names,
                    EdgeStrategy oe = EdgeStrategy::kMultiple,
                    EdgeStrategy ie = EdgeStrategy::kMultiple,
                    bool oe_mutable = true, bool ie_mutable = true,
                    bool sort_on_compaction = false,
                    const std::string& description = "",
                    const std::vector<Property>& default_property_values = {});

  void DeleteVertexLabel(const std::string& label, bool is_soft = false);

  void DeleteVertexLabel(label_t label, bool is_soft = false);

  void RevertDeleteVertexLabel(const std::string& label);

  void DeleteEdgeLabel(const std::string& label, bool is_soft = false);

  void RevertDeleteEdgeLabel(label_t label);

  void DeleteEdgeLabel(const label_t& src, const label_t& dst,
                       const label_t& edge, bool is_soft = false);

  void DeleteEdgeLabel(const std::string& src, const std::string& dst,
                       const std::string& edge, bool is_soft = false);

  void RevertDeleteEdgeLabel(const std::string& src, const std::string& dst,
                             const std::string& edge);

  void AddVertexProperties(
      const std::string& label,
      const std::vector<std::string>& properties_names,
      const std::vector<DataType>& properties_types,
      const std::vector<Property>& properties_default_values);

  void AddEdgeProperties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label,
      const std::vector<std::string>& properties_names,
      const std::vector<DataType>& properties_types,
      const std::vector<Property>& properties_default_values);

  void RenameVertexProperties(
      const std::string& label,
      const std::vector<std::string>& properties_names,
      const std::vector<std::string>& properties_renames);

  void RenameEdgeProperties(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<std::string>& properties_names,
                            const std::vector<std::string>& properties_renames);

  bool IsVertexLabelSoftDeleted(const std::string& label) const;

  bool IsVertexLabelSoftDeleted(label_t v_label) const;

  bool IsEdgeLabelSoftDeleted(label_t src_label, label_t dst_label,
                              label_t edge_label) const;

  bool IsEdgeLabelSoftDeleted(const std::string& src_label,
                              const std::string& dst_label,
                              const std::string& edge_label) const;

  bool IsVertexPropertySoftDeleted(const std::string& label,
                                   const std::string& property) const;

  bool IsVertexPropertySoftDeleted(label_t label,
                                   const std::string& property) const;

  bool IsEdgePropertySoftDeleted(const std::string& src_label,
                                 const std::string& dst_label,
                                 const std::string& edge_label,
                                 const std::string& property) const;

  bool IsEdgePropertySoftDeleted(label_t src_label, label_t dst_label,
                                 label_t edge_label,
                                 const std::string& property) const;

  void DeleteVertexProperties(const std::string& label,
                              const std::vector<std::string>& properties_names,
                              bool is_soft = false);

  void RevertDeleteVertexProperties(
      const std::string& label,
      const std::vector<std::string>& properties_names);

  void DeleteEdgeProperties(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<std::string>& properties_names,
                            bool is_soft = false);

  void RevertDeleteEdgeProperties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label,
      const std::vector<std::string>& properties_names);

  void RevertDeleteEdgeProperties(
      label_t src_label, label_t dst_label, label_t edge_label,
      const std::vector<std::string>& properties_names);

  label_t vertex_label_num() const;

  /**
   * @brief Get the next vertex label id to be assigned.
   * Vertex label ids in the schema are monotonically incremented,
   * and label ids are not recycled upon vertex label deletion.
   * Each new vertex label is assigned an id one greater than the previous.
   * This method returns the size of vlabel_indexer_, which equals
   * the label id that will be used for the next newly created vertex label.
   */
  label_t vertex_label_frontier() const;

  label_t edge_label_num() const;

  /**
   * @brief Get the next edge label id to be assigned.
   * Edge label ids in the schema are monotonically incremented,
   * and label ids are not recycled upon edge label deletion.
   * Each new edge label is assigned an id one greater than the previous.
   * This method returns the size of vlabel_indexer_, which equals
   * the label id that will be used for the next newly created edge label.
   */
  label_t edge_label_frontier() const;

  bool contains_vertex_label(const std::string& label) const;

  bool vertex_label_valid(label_t label_id) const;

  label_t get_vertex_label_id(const std::string& label) const;

  std::vector<label_t> get_vertex_label_ids() const;

  void set_vertex_properties(
      label_t label_id, const std::vector<DataType>& types,
      const std::vector<Property>& default_property_values = {});

  std::vector<DataType> get_vertex_properties(const std::string& label) const;
  std::vector<DataTypeId> get_vertex_properties_id(
      const std::string& label) const;

  std::vector<DataType> get_vertex_properties(label_t label) const;
  std::vector<DataTypeId> get_vertex_properties_id(label_t label) const;

  const std::vector<Property>& get_vertex_default_property_values(
      label_t label) const;

  std::vector<std::string> get_vertex_property_names(
      const std::string& label) const;

  std::vector<std::string> get_vertex_property_names(label_t label) const;

  const std::string& get_vertex_description(const std::string& label) const;

  const std::string& get_vertex_description(label_t label) const;

  size_t get_max_vnum(const std::string& label) const;

  bool exist(const std::string& src_label, const std::string& dst_label,
             const std::string& edge_label) const;

  bool exist(label_type src_label, label_type dst_label,
             label_type edge_label) const;

  const std::vector<Property>& get_edge_default_property_values(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id) const;

  std::vector<DataType> get_edge_properties(const std::string& src_label,
                                            const std::string& dst_label,
                                            const std::string& label) const;

  std::vector<DataTypeId> get_edge_properties_id(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  std::vector<DataType> get_edge_properties(label_t src_label,
                                            label_t dst_label,
                                            label_t label) const;

  std::vector<DataTypeId> get_edge_properties_id(label_t src_label,
                                                 label_t dst_label,
                                                 label_t label) const;

  std::string get_edge_description(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const;

  std::string get_edge_description(label_t src_label, label_t dst_label,
                                   label_t label) const;

  std::vector<std::string> get_edge_property_names(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  std::vector<std::string> get_edge_property_names(const label_t& src_label,
                                                   const label_t& dst_label,
                                                   const label_t& label) const;

  bool vertex_has_property(const std::string& label,
                           const std::string& prop) const;

  bool vertex_has_property(label_t label, const std::string& prop) const;

  bool vertex_has_primary_key(const std::string& label,
                              const std::string& prop) const;

  bool edge_has_property(const std::string& src_label,
                         const std::string& dst_label,
                         const std::string& edge_label,
                         const std::string& prop) const;

  bool edge_has_property(label_t src_label, label_t dst_label,
                         label_t edge_label, const std::string& prop) const;

  // Even when triplet is soft deleted, it return true
  bool has_edge_label(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label) const;

  // Even when triplet is soft deleted, it return true
  bool has_edge_label(label_t src_label, label_t dst_label,
                      label_t edge_label) const;

  EdgeStrategy get_outgoing_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  EdgeStrategy get_incoming_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  inline EdgeStrategy get_outgoing_edge_strategy(label_t src_label,
                                                 label_t dst_label,
                                                 label_t label) const {
    uint32_t index = generate_edge_label(src_label, dst_label, label);
    assert(e_schemas_.count(index) > 0);
    return e_schemas_.at(index)->oe_strategy;
  }

  inline EdgeStrategy get_incoming_edge_strategy(label_t src_label,
                                                 label_t dst_label,
                                                 label_t label) const {
    uint32_t index = generate_edge_label(src_label, dst_label, label);
    assert(e_schemas_.count(index) > 0);
    return e_schemas_.at(index)->ie_strategy;
  }

  bool outgoing_edge_mutable(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string& label) const;

  bool incoming_edge_mutable(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string& label) const;

  bool get_sort_on_compaction(const std::string& src_label,
                              const std::string& dst_label,
                              const std::string& label) const;

  bool get_sort_on_compaction(label_t src_label, label_t dst_label,
                              label_t label) const;

  bool contains_edge_label(const std::string& label) const;

  bool edge_label_valid(label_t label_id) const;

  bool edge_triplet_valid(label_t src, label_t dst, label_t edge) const;

  label_t get_edge_label_id(const std::string& label) const;

  std::vector<label_t> get_edge_label_ids() const;

  const std::string& get_vertex_label_name(label_t index) const;

  const std::string& get_edge_label_name(label_t index) const;

  const std::vector<std::tuple<DataType, std::string, size_t>>&
  get_vertex_primary_key(label_t index) const;

  const std::string& get_vertex_primary_key_name(label_t index) const;

  void Serialize(std::ostream& os) const;

  void Deserialize(std::istream& is);

  static neug::result<Schema> LoadFromYaml(const std::string& schema_config);

  static neug::result<Schema> LoadFromYamlNode(const YAML::Node& schema_node);

  static neug::result<YAML::Node> DumpToYaml(const Schema& schema);

  bool Equals(const Schema& other) const;

  neug::result<YAML::Node> to_yaml() const;

  inline void SetGraphName(const std::string& name) { name_ = name; }

  inline void SetGraphId(const std::string& id) { id_ = id; }

  inline std::string GetGraphName() const { return name_; }

  inline std::string GetGraphId() const { return id_; }

  std::string GetDescription() const;

  void SetDescription(const std::string& description);

  uint32_t generate_edge_label(label_t src, label_t dst, label_t edge) const;

  std::tuple<label_t, label_t, label_t> parse_edge_label(
      uint32_t edge_label) const;

  /*
  Get the Edge strategy for the specified edge triplet. MANY_TO_MANY,
  MANY_TO_ONE, ONE_TO_MANY, ONE_TO_ONE.
  */
  std::string get_edge_strategy(label_t src, label_t dst, label_t edge) const;

  void ensure_vertex_label_valid(label_t label) const;
  void ensure_edge_label_valid(label_t label) const;
  void ensure_edge_triplet_valid(label_t src, label_t dst, label_t edge) const;
  label_t vertex_label_to_index(const std::string& label);
  label_t edge_label_to_index(const std::string& label);

  /**
   * @brief Compact the schema by removing soft deleted labels and properties.
   * @return A new Schema object with the compacted schema.
   */
  Schema Compact() const;

 private:
  // Internal methods that do not check tombstone
  label_t get_vertex_label_id_internal(const std::string& label) const;
  label_t get_edge_label_id_internal(const std::string& label) const;
  bool vertex_has_property_internal(label_t label,
                                    const std::string& prop) const;
  bool edge_has_property_internal(label_t src_label, label_t dst_label,
                                  label_t edge_label,
                                  const std::string& prop) const;

  std::string name_, id_;
  IdIndexer<std::string, label_t> vlabel_indexer_;
  IdIndexer<std::string, label_t> elabel_indexer_;
  // We use shared_ptr to ensure the pointer to VertexSchema will not change
  // when resizing
  std::vector<std::shared_ptr<VertexSchema>> v_schemas_;
  std::unordered_map<uint32_t, std::shared_ptr<EdgeSchema>> e_schemas_;

  std::string description_;

  Bitset vlabel_tomb_;
  Bitset elabel_tomb_;          // tombstone for edge label
  Bitset elabel_triplet_tomb_;  // tombstone for edge label triplet

  friend class PropertyGraph;
};

InArchive& operator<<(InArchive& arc, const DataType& type);
OutArchive& operator>>(OutArchive& arc, DataType& type);
InArchive& operator<<(InArchive& arc, const VertexSchema& schema);
InArchive& operator<<(InArchive& arc, const EdgeSchema& schema);
OutArchive& operator>>(OutArchive& arc, VertexSchema& schema);
OutArchive& operator>>(OutArchive& arc, EdgeSchema& schema);

}  // namespace neug
