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

#ifndef STORAGES_RT_MUTABLE_GRAPH_SCHEMA_H_
#define STORAGES_RT_MUTABLE_GRAPH_SCHEMA_H_

#include <stddef.h>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "neug/execution/common/utils/bitset.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"

namespace YAML {
class Node;
}

namespace grape {
class LocalIOAdaptor;
}

namespace gs {

class PropertyGraph;
class Schema;

struct VertexSchema {
  VertexSchema() = default;
  VertexSchema(const std::vector<PropertyType>& property_types_,
               const std::vector<std::string>& property_names_,
               const std::vector<std::tuple<PropertyType, std::string, size_t>>&
                   primary_keys_,
               const std::vector<StorageStrategy>& storage_strategies_,
               const std::string& description_ = "",
               size_t max_num_ = static_cast<size_t>(1) << 32)
      : property_types(property_types_),
        property_names(property_names_),
        primary_keys(primary_keys_),
        storage_strategies(storage_strategies_),
        description(description_),
        max_num(max_num_) {
    vprop_soft_deleted.resize(property_names_.size(), false);
    storage_strategies.resize(property_types_.size(), StorageStrategy::kMem);
  }

  void clear();

  inline bool empty() const { return primary_keys.empty(); }

  void add_properties(const std::vector<std::string>& names,
                      const std::vector<PropertyType>& types,
                      const std::vector<StorageStrategy>& strategies);

  void set_properties(const std::vector<PropertyType>& types,
                      const std::vector<StorageStrategy>& strategies);

  void rename_properties(const std::vector<std::string>& names,
                         const std::vector<std::string>& renames);

  void delete_properties(const std::vector<std::string>& names,
                         bool is_soft = false);

  void revert_delete_properties(const std::vector<std::string>& names);

  bool is_property_soft_deleted(const std::string& prop) const;

  int32_t get_property_index(const std::string& prop) const;

  bool has_property(const std::string& prop) const;

  std::vector<PropertyType> property_types;
  std::vector<std::string> property_names;
  // <PropertyType, property_name, index_in_property_list>
  std::vector<std::tuple<PropertyType, std::string, size_t>> primary_keys;
  std::vector<StorageStrategy> storage_strategies;
  std::string description;
  size_t max_num;

  // Mark whether the vertex property is soft deleted
  std::vector<bool> vprop_soft_deleted;

 private:
  bool has_property_internal(const std::string& prop) const;

  friend class Schema;
};

struct EdgeSchema {
  EdgeSchema() = default;
  EdgeSchema(const std::string& src_label_name_,
             const std::string& dst_label_name_,
             const std::string& edge_label_name_, bool sort_on_compaction_,
             const std::string& description_, bool ie_mutable_,
             bool oe_mutable_, EdgeStrategy oe_strategy_,
             EdgeStrategy ie_strategy_,
             const std::vector<PropertyType>& properties_,
             const std::vector<std::string>& property_names_,
             const std::vector<StorageStrategy>& strategies_)
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
        strategies(strategies_) {
    eprop_soft_deleted.resize(property_names_.size(), false);
    strategies.resize(properties_.size(), StorageStrategy::kMem);
    CHECK(properties.size() == property_names.size());
    CHECK(properties.size() == strategies.size());
  }

  bool is_bundled() const;

  bool empty() const { return src_label_name.empty(); }

  bool has_property(const std::string& prop) const;

  void add_properties(const std::vector<std::string>& names,
                      const std::vector<PropertyType>& types,
                      const std::vector<StorageStrategy>& new_strategies = {});

  void rename_properties(const std::vector<std::string>& names,
                         const std::vector<std::string>& renames);

  void delete_properties(const std::vector<std::string>& names,
                         bool is_soft = false);

  bool is_property_soft_deleted(const std::string& prop) const;

  void revert_delete_properties(const std::vector<std::string>& names);

  std::string src_label_name, dst_label_name, edge_label_name;
  bool sort_on_compaction;
  std::string description;
  bool ie_mutable;
  bool oe_mutable;
  EdgeStrategy oe_strategy;
  EdgeStrategy ie_strategy;
  std::vector<PropertyType> properties;
  std::vector<std::string> property_names;
  std::vector<StorageStrategy> strategies;

  // Mark whether the edge property is soft deleted
  std::vector<bool> eprop_soft_deleted;

 private:
  bool has_property_internal(const std::string& prop) const;

  friend class Schema;
};

class Schema {
 public:
  // How many built-in plugins are there.
  // Currently only one builtin plugin, SERVER_APP is supported.
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

  // An array containing all compatible versions of schema.
  static const std::vector<std::string> COMPATIBLE_VERSIONS;
  static constexpr const char* DEFAULT_SCHEMA_VERSION = "v0.0";
  static constexpr const size_t MAX_VNUM = static_cast<size_t>(1) << 32;

  using label_type = label_t;
  Schema();
  ~Schema();

  static const std::vector<std::string>& GetCompatibleVersions();

  void Clear();

  bool Empty() const {
    return vlabel_indexer_.empty() && elabel_indexer_.empty();
  }

  std::shared_ptr<VertexSchema> get_vertex_schema(label_t label) {
    return v_schemas_.at(label);
  }

  std::shared_ptr<EdgeSchema> get_edge_schema(label_t src_label,
                                              label_t dst_label,
                                              label_t edge_label) {
    auto key = generate_edge_label(src_label, dst_label, edge_label);
    assert(e_schemas_.count(key) > 0);
    return e_schemas_.at(key);
  }

  void AddVertexLabel(
      const std::string& label, const std::vector<PropertyType>& property_types,
      const std::vector<std::string>& property_names,
      const std::vector<std::tuple<PropertyType, std::string, size_t>>&
          primary_key,
      const std::vector<StorageStrategy>& strategies = {},
      size_t max_vnum = static_cast<size_t>(1) << 32,
      const std::string& description = "");

  void AddEdgeLabel(const std::string& src_label, const std::string& dst_label,
                    const std::string& edge_label,
                    const std::vector<PropertyType>& properties,
                    const std::vector<std::string>& prop_names,
                    const std::vector<StorageStrategy>& strategies = {},
                    EdgeStrategy oe = EdgeStrategy::kMultiple,
                    EdgeStrategy ie = EdgeStrategy::kMultiple,
                    bool oe_mutable = true, bool ie_mutable = true,
                    bool sort_on_compaction = false,
                    const std::string& description = "");

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

  void AddVertexProperties(const std::string& label,
                           std::vector<std::string>& properties_names,
                           std::vector<PropertyType>& properties_types,
                           std::vector<StorageStrategy>& storage_strategies,
                           std::vector<Property>& properties_default_values);

  void AddEdgeProperties(const std::string& src_label,
                         const std::string& dst_label,
                         const std::string& edge_label,
                         std::vector<std::string>& properties_names,
                         std::vector<PropertyType>& properties_types,
                         std::vector<Property>& properties_default_values);

  void RenameVertexProperties(const std::string& label,
                              std::vector<std::string>& properties_names,
                              std::vector<std::string>& properties_renames);

  void RenameEdgeProperties(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            std::vector<std::string>& properties_names,
                            std::vector<std::string>& properties_renames);

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
                              std::vector<std::string>& properties_names,
                              bool is_soft = false);

  void RevertDeleteVertexProperties(const std::string& label,
                                    std::vector<std::string>& properties_names);

  void DeleteEdgeProperties(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            std::vector<std::string>& properties_names,
                            bool is_soft = false);

  void RevertDeleteEdgeProperties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label,
      const std::vector<std::string>& properties_names);

  void RevertDeleteEdgeProperties(
      label_t src_label, label_t dst_label, label_t edge_label,
      const std::vector<std::string>& properties_names);

  label_t vertex_label_num() const;

  label_t edge_label_num() const;

  bool contains_vertex_label(const std::string& label) const;

  bool vertex_label_valid(label_t label_id) const;

  label_t get_vertex_label_id(const std::string& label) const;

  std::vector<label_t> get_vertex_label_ids() const;

  void set_vertex_properties(
      label_t label_id, const std::vector<PropertyType>& types,
      const std::vector<StorageStrategy>& strategies = {});

  std::vector<PropertyType> get_vertex_properties(
      const std::string& label) const;

  std::vector<PropertyType> get_vertex_properties(label_t label) const;

  std::vector<std::string> get_vertex_property_names(
      const std::string& label) const;

  std::vector<std::string> get_vertex_property_names(label_t label) const;

  const std::string& get_vertex_description(const std::string& label) const;

  const std::string& get_vertex_description(label_t label) const;

  std::vector<StorageStrategy> get_vertex_storage_strategies(
      const std::string& label) const;

  size_t get_max_vnum(const std::string& label) const;

  bool exist(const std::string& src_label, const std::string& dst_label,
             const std::string& edge_label) const;

  bool exist(label_type src_label, label_type dst_label,
             label_type edge_label) const;

  std::vector<PropertyType> get_edge_properties(const std::string& src_label,
                                                const std::string& dst_label,
                                                const std::string& label) const;

  std::vector<PropertyType> get_edge_properties(label_t src_label,
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

  const std::vector<std::tuple<PropertyType, std::string, size_t>>&
  get_vertex_primary_key(label_t index) const;

  const std::string& get_vertex_primary_key_name(label_t index) const;

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) const;

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader);

  static gs::result<Schema> LoadFromYaml(const std::string& schema_config);

  static gs::result<Schema> LoadFromYamlNode(const YAML::Node& schema_node);

  static gs::result<YAML::Node> DumpToYaml(const Schema& schema);

  bool Equals(const Schema& other) const;

  gs::result<YAML::Node> to_yaml() const;

  inline void SetGraphName(const std::string& name) { name_ = name; }

  inline void SetGraphId(const std::string& id) { id_ = id; }

  inline std::string GetGraphName() const { return name_; }

  inline std::string GetGraphId() const { return id_; }

  std::string GetDescription() const;

  void SetDescription(const std::string& description);

  void SetRemotePath(const std::string& remote_path);

  inline std::string GetRemotePath() const { return remote_path_; }

  void SetVersion(const std::string& version);

  std::string GetVersion() const;

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

 private:
  label_t edge_label_to_index(const std::string& label);
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
  std::string version_;
  std::string remote_path_;  // The path to the data on the remote storage

  Bitset vlabel_tomb_;
  Bitset elabel_tomb_;          // tombstone for edge label
  Bitset elabel_triplet_tomb_;  // tombstone for edge label triplet

  friend class PropertyGraph;
};

grape::InArchive& operator<<(grape::InArchive& arc, const VertexSchema& schema);
grape::InArchive& operator<<(grape::InArchive& arc, const EdgeSchema& schema);
grape::OutArchive& operator>>(grape::OutArchive& arc, VertexSchema& schema);
grape::OutArchive& operator>>(grape::OutArchive& arc, EdgeSchema& schema);

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_SCHEMA_H_
