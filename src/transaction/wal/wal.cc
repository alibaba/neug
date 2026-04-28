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

void CreateVertexTypeRedo::Serialize(InArchive& arc,
                                     const CreateVertexTypeParam& config) {
  arc << static_cast<uint8_t>(OpType::kCreateVertexType);
  config.Serialize(arc);
}

CreateVertexTypeParam CreateVertexTypeRedo::Deserialize(OutArchive& arc) {
  return CreateVertexTypeParam::Deserialize(arc);
}

void CreateEdgeTypeRedo::Serialize(InArchive& arc,
                                   const CreateEdgeTypeParam& config) {
  arc << static_cast<uint8_t>(OpType::kCreateEdgeType);
  config.Serialize(arc);
}

CreateEdgeTypeParam CreateEdgeTypeRedo::Deserialize(OutArchive& arc) {
  return CreateEdgeTypeParam::Deserialize(arc);
}

void AddVertexPropertiesRedo::Serialize(
    InArchive& arc, const AddVertexPropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kAddVertexProp);
  config.Serialize(arc);
}

AddVertexPropertiesParam AddVertexPropertiesRedo::Deserialize(
    OutArchive& arc) {
  return AddVertexPropertiesParam::Deserialize(arc);
}

void AddEdgePropertiesRedo::Serialize(InArchive& arc,
                                      const AddEdgePropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kAddEdgeProp);
  config.Serialize(arc);
}

AddEdgePropertiesParam AddEdgePropertiesRedo::Deserialize(OutArchive& arc) {
  return AddEdgePropertiesParam::Deserialize(arc);
}

void RenameVertexPropertiesRedo::Serialize(
    InArchive& arc, const RenameVertexPropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kRenameVertexProp);
  config.Serialize(arc);
}

RenameVertexPropertiesParam RenameVertexPropertiesRedo::Deserialize(
    OutArchive& arc) {
  return RenameVertexPropertiesParam::Deserialize(arc);
}

void RenameEdgePropertiesRedo::Serialize(
    InArchive& arc, const RenameEdgePropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kRenameEdgeProp);
  config.Serialize(arc);
}

RenameEdgePropertiesParam RenameEdgePropertiesRedo::Deserialize(
    OutArchive& arc) {
  return RenameEdgePropertiesParam::Deserialize(arc);
}

void DeleteVertexPropertiesRedo::Serialize(
    InArchive& arc, const DeleteVertexPropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kDeleteVertexProp);
  config.Serialize(arc);
}

DeleteVertexPropertiesParam DeleteVertexPropertiesRedo::Deserialize(
    OutArchive& arc) {
  return DeleteVertexPropertiesParam::Deserialize(arc);
}

void DeleteEdgePropertiesRedo::Serialize(
    InArchive& arc, const DeleteEdgePropertiesParam& config) {
  arc << static_cast<uint8_t>(OpType::kDeleteEdgeProp);
  config.Serialize(arc);
}

DeleteEdgePropertiesParam DeleteEdgePropertiesRedo::Deserialize(
    OutArchive& arc) {
  return DeleteEdgePropertiesParam::Deserialize(arc);
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
