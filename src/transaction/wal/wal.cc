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

#include "neug/transaction/wal/wal.h"

#include <glog/logging.h>
#include <memory>
#include <regex>
#include <sstream>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/transaction/wal/dummy_wal_writer.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

std::string parse_wal_uri(std::string wal_uri, const std::string& work_dir) {
  if (wal_uri.empty()) {
    VLOG(1) << "wal_uri is not set, use default wal_uri";
    wal_uri = wal_dir(work_dir);
  } else if (wal_uri.find("{GRAPH_DATA_DIR}") != std::string::npos) {
    VLOG(1) << "Template {GRAPH_DATA_DIR} found in wal_uri, replace it with "
               "data_dir";
    wal_uri = std::regex_replace(wal_uri, std::regex("\\{GRAPH_DATA_DIR\\}"),
                                 work_dir);
  }
  return wal_uri;
}

std::string get_wal_uri_scheme(const std::string& uri) {
  std::string scheme;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    scheme = uri.substr(0, pos);
  }
  if (scheme.empty()) {
    VLOG(20) << "No scheme found in wal uri: " << uri
             << ", using default scheme: file";
    scheme = "file";
  }
  return scheme;
}

std::string get_wal_uri_path(const std::string& uri) {
  std::string path;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    path = uri.substr(pos + 3);
  } else {
    path = uri;
  }
  return path;
}

void WalWriterFactory::Init() {}

void WalWriterFactory::Finalize() {}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateWalWriter(
    const std::string& wal_uri, int32_t thread_id) {
  auto& known_writers_ = getKnownWalWriters();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = known_writers_.find(scheme);
  if (iter != known_writers_.end()) {
    return iter->second(wal_uri, thread_id);
  } else {
    std::stringstream ss;
    for (const auto& writer : known_writers_) {
      ss << "[" << writer.first << "] ";
    }
    LOG(FATAL) << "Unknown wal writer: " << scheme << " for uri: " << wal_uri
               << ", supported writers are: " << ss.str();
    return nullptr;  // to suppress warning
  }
}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateDummyWalWriter() {
  return std::make_unique<DummyWalWriter>();
}

bool WalWriterFactory::RegisterWalWriter(
    const std::string& wal_writer_type,
    WalWriterFactory::wal_writer_initializer_t initializer) {
  auto& known_writers_ = getKnownWalWriters();
  known_writers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalWriterFactory::wal_writer_initializer_t>&
WalWriterFactory::getKnownWalWriters() {
  static std::unordered_map<
      std::string, WalWriterFactory::wal_writer_initializer_t>* known_writers_ =
      new std::unordered_map<std::string, wal_writer_initializer_t>();
  return *known_writers_;
}

////////////////////////// WalParserFactory //////////////////////////

void WalParserFactory::Init() {}

void WalParserFactory::Finalize() {}

std::unique_ptr<IWalParser> WalParserFactory::CreateWalParser(
    const std::string& wal_uri) {
  auto& know_parsers_ = getKnownWalParsers();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = know_parsers_.find(scheme);
  if (iter != know_parsers_.end()) {
    return iter->second(wal_uri);
  } else {
    std::stringstream ss;
    for (const auto& parser : know_parsers_) {
      ss << "[" << parser.first << "] ";
    }
    LOG(FATAL) << "Unknown wal parser: " << scheme << " for uri: " << wal_uri
               << ", supported parsers are: " << ss.str();
    return nullptr;  // to suppress warning
  }
}

bool WalParserFactory::RegisterWalParser(
    const std::string& wal_writer_type,
    WalParserFactory::wal_parser_initializer_t initializer) {
  auto& known_parsers_ = getKnownWalParsers();
  known_parsers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalParserFactory::wal_parser_initializer_t>&
WalParserFactory::getKnownWalParsers() {
  static std::unordered_map<
      std::string, WalParserFactory::wal_parser_initializer_t>* known_parsers_ =
      new std::unordered_map<std::string, wal_parser_initializer_t>();
  return *known_parsers_;
}

////////////////////////// Serialization operators //////////////////////////

void CreateVertexTypeRedo::Serialize(
    InArchive& arc, const std::string& vertex_type,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    const std::vector<std::string>& primary_key_names) {
  arc << static_cast<uint8_t>(OpType::kCreateVertexType);
  arc << vertex_type;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type;
    arc << name;
    arc << default_value;
  }
  arc << static_cast<uint32_t>(primary_key_names.size());
  for (const auto& key : primary_key_names) {
    arc << key;
  }
}

void CreateVertexTypeRedo::Deserialize(OutArchive& arc,
                                       CreateVertexTypeRedo& redo) {
  arc >> redo.vertex_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.properties.resize(prop_size);
  for (auto& [type, name, default_value] : redo.properties) {
    arc >> type >> name >> default_value;
  }
  uint32_t key_size;
  arc >> key_size;
  redo.primary_key_names.resize(key_size);
  for (auto& key : redo.primary_key_names) {
    arc >> key;
  }
}

void CreateEdgeTypeRedo::Serialize(
    InArchive& arc, const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    EdgeStrategy oe_edge_strategy, EdgeStrategy ie_edge_strategy) {
  arc << static_cast<uint8_t>(OpType::kCreateEdgeType);
  arc << src_type << dst_type << edge_type;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
  arc << oe_edge_strategy << ie_edge_strategy;
}

void CreateEdgeTypeRedo::Deserialize(OutArchive& arc,
                                     CreateEdgeTypeRedo& redo) {
  arc >> redo.src_type >> redo.dst_type >> redo.edge_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.properties.resize(prop_size);
  for (auto& [type, name, default_value] : redo.properties) {
    arc >> type >> name >> default_value;
  }
  arc >> redo.oe_edge_strategy >> redo.ie_edge_strategy;
}

void AddVertexPropertiesRedo::Serialize(
    InArchive& arc, const std::string& vertex_type,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        properties) {
  arc << static_cast<uint8_t>(OpType::kAddVertexProp);
  arc << vertex_type;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

void AddVertexPropertiesRedo::Deserialize(OutArchive& arc,
                                          AddVertexPropertiesRedo& redo) {
  arc >> redo.vertex_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.properties.resize(prop_size);
  for (auto& [type, name, default_value] : redo.properties) {
    arc >> type >> name >> default_value;
  }
}

void AddEdgePropertiesRedo::Serialize(
    InArchive& arc, const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        properties) {
  arc << static_cast<uint8_t>(OpType::kAddEdgeProp);
  arc << src_type << dst_type << edge_type;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& [type, name, default_value] : properties) {
    arc << type << name << default_value;
  }
}

void AddEdgePropertiesRedo::Deserialize(OutArchive& arc,
                                        AddEdgePropertiesRedo& redo) {
  arc >> redo.src_type >> redo.dst_type >> redo.edge_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.properties.resize(prop_size);
  for (auto& [type, name, default_value] : redo.properties) {
    arc >> type >> name >> default_value;
  }
}

void RenameVertexPropertiesRedo::Serialize(
    InArchive& arc, const std::string& vertex_type,
    const std::vector<std::pair<std::string, std::string>>& update_properties) {
  arc << static_cast<uint8_t>(OpType::kRenameVertexProp);
  arc << vertex_type;
  arc << static_cast<uint32_t>(update_properties.size());
  for (const auto& [old_name, new_name] : update_properties) {
    arc << old_name << new_name;
  }
}

void RenameVertexPropertiesRedo::Deserialize(OutArchive& arc,
                                             RenameVertexPropertiesRedo& redo) {
  arc >> redo.vertex_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.update_properties.resize(prop_size);
  for (auto& [old_name, new_name] : redo.update_properties) {
    arc >> old_name >> new_name;
  }
}

void RenameEdgePropertiesRedo::Serialize(
    InArchive& arc, const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::pair<std::string, std::string>>& update_properties) {
  arc << static_cast<uint8_t>(OpType::kRenameEdgeProp);
  arc << src_type << dst_type << edge_type;
  arc << static_cast<uint32_t>(update_properties.size());
  for (const auto& [old_name, new_name] : update_properties) {
    arc << old_name << new_name;
  }
}

void RenameEdgePropertiesRedo::Deserialize(OutArchive& arc,
                                           RenameEdgePropertiesRedo& redo) {
  arc >> redo.src_type >> redo.dst_type >> redo.edge_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.update_properties.resize(prop_size);
  for (auto& [old_name, new_name] : redo.update_properties) {
    arc >> old_name >> new_name;
  }
}

void DeleteVertexPropertiesRedo::Serialize(
    InArchive& arc, const std::string& vertex_type,
    const std::vector<std::string>& delete_properties) {
  arc << static_cast<uint8_t>(OpType::kDeleteVertexProp);
  arc << vertex_type;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& name : delete_properties) {
    arc << name;
  }
}

void DeleteVertexPropertiesRedo::Deserialize(OutArchive& arc,
                                             DeleteVertexPropertiesRedo& redo) {
  arc >> redo.vertex_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.delete_properties.resize(prop_size);
  for (auto& name : redo.delete_properties) {
    arc >> name;
  }
}

void DeleteEdgePropertiesRedo::Serialize(
    InArchive& arc, const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::string>& delete_properties) {
  arc << static_cast<uint8_t>(OpType::kDeleteEdgeProp);
  arc << src_type << dst_type << edge_type;
  arc << static_cast<uint32_t>(delete_properties.size());
  for (const auto& name : delete_properties) {
    arc << name;
  }
}

void DeleteEdgePropertiesRedo::Deserialize(OutArchive& arc,
                                           DeleteEdgePropertiesRedo& redo) {
  arc >> redo.src_type >> redo.dst_type >> redo.edge_type;
  uint32_t prop_size;
  arc >> prop_size;
  redo.delete_properties.resize(prop_size);
  for (auto& name : redo.delete_properties) {
    arc >> name;
  }
}

void DeleteVertexTypeRedo::Serialize(InArchive& arc,
                                     const std::string& vertex_type) {
  arc << static_cast<uint8_t>(OpType::kDeleteVertexType);
  arc << vertex_type;
}

void DeleteVertexTypeRedo::Deserialize(OutArchive& arc,
                                       DeleteVertexTypeRedo& redo) {
  arc >> redo.vertex_type;
}

void DeleteEdgeTypeRedo::Serialize(InArchive& arc, const std::string& src_type,
                                   const std::string& dst_type,
                                   const std::string& edge_type) {
  arc << static_cast<uint8_t>(OpType::kDeleteEdgeType);
  arc << src_type << dst_type << edge_type;
}

void DeleteEdgeTypeRedo::Deserialize(OutArchive& arc,
                                     DeleteEdgeTypeRedo& redo) {
  arc >> redo.src_type >> redo.dst_type >> redo.edge_type;
}

void InsertVertexRedo::Serialize(InArchive& arc, label_t label,
                                 const Property& oid,
                                 const std::vector<Property>& props) {
  arc << static_cast<uint8_t>(OpType::kInsertVertex);
  arc << label << oid;
  arc << static_cast<uint32_t>(props.size());
  for (const auto& prop : props) {
    arc << prop;
  }
}

void InsertVertexRedo::Deserialize(OutArchive& arc, InsertVertexRedo& redo) {
  arc >> redo.label >> redo.oid;
  uint32_t prop_size;
  arc >> prop_size;
  redo.props.resize(prop_size);
  for (auto& prop : redo.props) {
    arc >> prop;
  }
}

void InsertEdgeRedo::Serialize(InArchive& arc, label_t src_label,
                               const Property& src, label_t dst_label,
                               const Property& dst, label_t edge_label,
                               const std::vector<Property>& properties) {
  arc << static_cast<uint8_t>(OpType::kInsertEdge);
  arc << src_label << src << dst_label << dst << edge_label;
  arc << static_cast<uint32_t>(properties.size());
  for (const auto& prop : properties) {
    arc << prop;
  }
}

void InsertEdgeRedo::Deserialize(OutArchive& arc, InsertEdgeRedo& redo) {
  arc >> redo.src_label >> redo.src >> redo.dst_label >> redo.dst >>
      redo.edge_label;
  uint32_t prop_size;
  arc >> prop_size;
  redo.properties.resize(prop_size);
  for (auto& prop : redo.properties) {
    arc >> prop;
  }
}

void UpdateVertexPropRedo::Serialize(InArchive& arc, label_t label,
                                     const Property& oid, int prop_id,
                                     const Property& value) {
  arc << static_cast<uint8_t>(OpType::kUpdateVertexProp);
  arc << label << oid << prop_id << value;
}

void UpdateVertexPropRedo::Deserialize(OutArchive& arc,
                                       UpdateVertexPropRedo& redo) {
  arc >> redo.label >> redo.oid >> redo.prop_id >> redo.value;
}

void UpdateEdgePropRedo::Serialize(InArchive& arc, label_t src_label,
                                   const Property& src, label_t dst_label,
                                   const Property& dst, label_t edge_label,
                                   int32_t oe_offset, int32_t ie_offset,
                                   int prop_id, const Property& value) {
  arc << static_cast<uint8_t>(OpType::kUpdateEdgeProp);
  arc << src_label << src << dst_label << dst << edge_label;
  arc << oe_offset << ie_offset;
  arc << prop_id << value;
}

void UpdateEdgePropRedo::Deserialize(OutArchive& arc,
                                     UpdateEdgePropRedo& redo) {
  arc >> redo.src_label >> redo.src >> redo.dst_label >> redo.dst >>
      redo.edge_label;
  arc >> redo.oe_offset >> redo.ie_offset;
  arc >> redo.prop_id >> redo.value;
}

void RemoveVertexRedo::Serialize(InArchive& arc, label_t label,
                                 const Property& oid) {
  arc << static_cast<uint8_t>(OpType::kRemoveVertex);
  arc << label << oid;
}

void RemoveVertexRedo::Deserialize(OutArchive& arc, RemoveVertexRedo& redo) {
  arc >> redo.label >> redo.oid;
}

void RemoveEdgeRedo::Serialize(InArchive& arc, label_t src_label,
                               const Property& src, label_t dst_label,
                               const Property& dst, label_t edge_label,
                               int32_t oe_offset, int32_t ie_offset) {
  arc << static_cast<uint8_t>(OpType::kRemoveEdge);
  arc << src_label << src << dst_label << dst << edge_label;
  arc << oe_offset << ie_offset;
}

void RemoveEdgeRedo::Deserialize(OutArchive& arc, RemoveEdgeRedo& redo) {
  arc >> redo.src_label >> redo.src >> redo.dst_label >> redo.dst >>
      redo.edge_label;
  arc >> redo.oe_offset >> redo.ie_offset;
}

InArchive& operator<<(InArchive& in_archive,
                      const CreateVertexTypeRedo& value) {
  CreateVertexTypeRedo::Serialize(in_archive, value.vertex_type,
                                  value.properties, value.primary_key_names);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const CreateEdgeTypeRedo& value) {
  CreateEdgeTypeRedo::Serialize(in_archive, value.src_type, value.dst_type,
                                value.edge_type, value.properties,
                                value.oe_edge_strategy, value.ie_edge_strategy);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const AddVertexPropertiesRedo& value) {
  AddVertexPropertiesRedo::Serialize(in_archive, value.vertex_type,
                                     value.properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const AddEdgePropertiesRedo& value) {
  AddEdgePropertiesRedo::Serialize(in_archive, value.src_type, value.dst_type,
                                   value.edge_type, value.properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const RenameVertexPropertiesRedo& value) {
  RenameVertexPropertiesRedo::Serialize(in_archive, value.vertex_type,
                                        value.update_properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const RenameEdgePropertiesRedo& value) {
  RenameEdgePropertiesRedo::Serialize(in_archive, value.src_type,
                                      value.dst_type, value.edge_type,
                                      value.update_properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const DeleteVertexPropertiesRedo& value) {
  DeleteVertexPropertiesRedo::Serialize(in_archive, value.vertex_type,
                                        value.delete_properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const DeleteEdgePropertiesRedo& value) {
  DeleteEdgePropertiesRedo::Serialize(in_archive, value.src_type,
                                      value.dst_type, value.edge_type,
                                      value.delete_properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const DeleteVertexTypeRedo& value) {
  DeleteVertexTypeRedo::Serialize(in_archive, value.vertex_type);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const DeleteEdgeTypeRedo& value) {
  DeleteEdgeTypeRedo::Serialize(in_archive, value.src_type, value.dst_type,
                                value.edge_type);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const InsertVertexRedo& value) {
  InsertVertexRedo::Serialize(in_archive, value.label, value.oid, value.props);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const InsertEdgeRedo& value) {
  InsertEdgeRedo::Serialize(in_archive, value.src_label, value.src,
                            value.dst_label, value.dst, value.edge_label,
                            value.properties);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive,
                      const UpdateVertexPropRedo& value) {
  UpdateVertexPropRedo::Serialize(in_archive, value.label, value.oid,
                                  value.prop_id, value.value);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const UpdateEdgePropRedo& value) {
  UpdateEdgePropRedo::Serialize(in_archive, value.src_label, value.src,
                                value.dst_label, value.dst, value.edge_label,
                                value.oe_offset, value.ie_offset, value.prop_id,
                                value.value);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const RemoveVertexRedo& value) {
  RemoveVertexRedo::Serialize(in_archive, value.label, value.oid);
  return in_archive;
}

InArchive& operator<<(InArchive& in_archive, const RemoveEdgeRedo& value) {
  RemoveEdgeRedo::Serialize(in_archive, value.src_label, value.src,
                            value.dst_label, value.dst, value.edge_label,
                            value.oe_offset, value.ie_offset);
  return in_archive;
}

////////////////////////// Deserialization operators //////////////////////////

OutArchive& operator>>(OutArchive& out_archive, CreateVertexTypeRedo& value) {
  CreateVertexTypeRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, CreateEdgeTypeRedo& value) {
  CreateEdgeTypeRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive,
                       AddVertexPropertiesRedo& value) {
  AddVertexPropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, AddEdgePropertiesRedo& value) {
  AddEdgePropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive,
                       RenameVertexPropertiesRedo& value) {
  RenameVertexPropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive,
                       RenameEdgePropertiesRedo& value) {
  RenameEdgePropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive,
                       DeleteVertexPropertiesRedo& value) {
  DeleteVertexPropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive,
                       DeleteEdgePropertiesRedo& value) {
  DeleteEdgePropertiesRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, DeleteVertexTypeRedo& value) {
  DeleteVertexTypeRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, DeleteEdgeTypeRedo& value) {
  DeleteEdgeTypeRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, InsertVertexRedo& value) {
  InsertVertexRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, InsertEdgeRedo& value) {
  InsertEdgeRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, UpdateVertexPropRedo& value) {
  UpdateVertexPropRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, UpdateEdgePropRedo& value) {
  UpdateEdgePropRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, RemoveVertexRedo& value) {
  RemoveVertexRedo::Deserialize(out_archive, value);
  return out_archive;
}

OutArchive& operator>>(OutArchive& out_archive, RemoveEdgeRedo& value) {
  RemoveEdgeRedo::Deserialize(out_archive, value);
  return out_archive;
}

}  // namespace neug
