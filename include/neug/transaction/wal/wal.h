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

#ifndef INCLUDE_NEUG_TRANSACTION_WAL_WAL_H_
#define INCLUDE_NEUG_TRANSACTION_WAL_WAL_H_

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace gs {

struct WalHeader {
  uint32_t timestamp;
  uint8_t type : 1;
  int32_t length : 31;
};

struct WalContentUnit {
  char* ptr{NULL};
  size_t size{0};
};

struct UpdateWalUnit {
  uint32_t timestamp{0};
  char* ptr{NULL};
  size_t size{0};
};

std::string parse_wal_uri(std::string uri, const std::string& work_dir);
std::string get_wal_uri_scheme(const std::string& uri);
std::string get_wal_uri_path(const std::string& uri);

/**
 * The interface of wal writer.
 */
class IWalWriter {
 public:
  virtual ~IWalWriter() {}

  virtual std::string type() const = 0;
  /**
   * Open a wal file. In our design, each thread has its own wal file.
   * The uri could be a file_path or a remote connection string.
   */
  virtual void open() = 0;

  /**
   * Close the wal writer. If a remote connection is hold by the wal writer,
   * it should be closed.
   */
  virtual void close() = 0;

  /**
   * Append data to the wal file.
   */
  virtual bool append(const char* data, size_t length) = 0;
};

/**
 * The interface of wal parser.
 */
class IWalParser {
 public:
  virtual ~IWalParser() {}

  /**
   * Open wals from a uri and parse the wal files.
   */
  virtual void open(const std::string& wal_uri) = 0;

  virtual void close() = 0;

  virtual uint32_t last_ts() const = 0;

  /*
   * Get the insert wal unit with the given timestamp.
   */
  virtual const WalContentUnit& get_insert_wal(uint32_t ts) const = 0;

  /**
   * Get all the update wal units.
   */
  virtual const std::vector<UpdateWalUnit>& get_update_wals() const = 0;
};

class WalWriterFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)(
      const std::string& wal_uri, int32_t thread_id);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalWriter> CreateDummyWalWriter();

  static std::unique_ptr<IWalWriter> CreateWalWriter(const std::string& wal_uri,
                                                     int32_t thread_id);

  static bool RegisterWalWriter(const std::string& wal_writer_type,
                                wal_writer_initializer_t initializer);

 private:
  static std::unordered_map<std::string, wal_writer_initializer_t>&
  getKnownWalWriters();
};

class WalParserFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)();
  using wal_parser_initializer_t =
      std::unique_ptr<IWalParser> (*)(const std::string& wal_dir);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalParser> CreateWalParser(
      const std::string& wal_uri);

  static bool RegisterWalParser(const std::string& wal_parser_type,
                                wal_parser_initializer_t initializer);

 private:
  static std::unordered_map<std::string, wal_parser_initializer_t>&
  getKnownWalParsers();
};

struct CreateVertexTypeRedo {
  std::string vertex_type;
  std::vector<std::tuple<PropertyType, std::string, Property>> properties;
  std::vector<std::string> primary_key_names;

  static void Serialize(
      grape::InArchive& arc, const std::string& vertex_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names);
  static void Deserialize(grape::OutArchive& arc, CreateVertexTypeRedo& redo);
};

struct CreateEdgeTypeRedo {
  std::string src_type, dst_type, edge_type;
  std::vector<std::tuple<PropertyType, std::string, Property>> properties;
  EdgeStrategy oe_edge_strategy, ie_edge_strategy;

  static void Serialize(
      grape::InArchive& arc, const std::string& src_type,
      const std::string& dst_type, const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      EdgeStrategy oe_edge_strategy, EdgeStrategy ie_edge_strategy);
  static void Deserialize(grape::OutArchive& arc, CreateEdgeTypeRedo& redo);
};

struct AddVertexPropertiesRedo {
  std::string vertex_type;
  std::vector<std::tuple<PropertyType, std::string, Property>> properties;

  static void Serialize(
      grape::InArchive& arc, const std::string& vertex_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties);
  static void Deserialize(grape::OutArchive& arc,
                          AddVertexPropertiesRedo& redo);
};

struct AddEdgePropertiesRedo {
  std::string src_type, dst_type, edge_type;
  std::vector<std::tuple<PropertyType, std::string, Property>> properties;

  static void Serialize(
      grape::InArchive& arc, const std::string& src_type,
      const std::string& dst_type, const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties);
  static void Deserialize(grape::OutArchive& arc, AddEdgePropertiesRedo& redo);
};

struct RenameVertexPropertiesRedo {
  std::string vertex_type;
  std::vector<std::pair<std::string, std::string>> update_properties;

  static void Serialize(grape::InArchive& arc, const std::string& vertex_type,
                        const std::vector<std::pair<std::string, std::string>>&
                            update_properties);
  static void Deserialize(grape::OutArchive& arc,
                          RenameVertexPropertiesRedo& redo);
};

struct RenameEdgePropertiesRedo {
  std::string src_type, dst_type, edge_type;
  std::vector<std::pair<std::string, std::string>> update_properties;

  static void Serialize(grape::InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const std::vector<std::pair<std::string, std::string>>&
                            update_properties);
  static void Deserialize(grape::OutArchive& arc,
                          RenameEdgePropertiesRedo& redo);
};

struct DeleteVertexPropertiesRedo {
  std::string vertex_type;
  std::vector<std::string> delete_properties;

  static void Serialize(grape::InArchive& arc, const std::string& vertex_type,
                        const std::vector<std::string>& delete_properties);
  static void Deserialize(grape::OutArchive& arc,
                          DeleteVertexPropertiesRedo& redo);
};

struct DeleteEdgePropertiesRedo {
  std::string src_type, dst_type, edge_type;
  std::vector<std::string> delete_properties;

  static void Serialize(grape::InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const std::vector<std::string>& delete_properties);
  static void Deserialize(grape::OutArchive& arc,
                          DeleteEdgePropertiesRedo& redo);
};

struct DeleteVertexTypeRedo {
  std::string vertex_type;

  static void Serialize(grape::InArchive& arc, const std::string& vertex_type);
  static void Deserialize(grape::OutArchive& arc, DeleteVertexTypeRedo& redo);
};

struct DeleteEdgeTypeRedo {
  std::string src_type, dst_type, edge_type;

  static void Serialize(grape::InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type);
  static void Deserialize(grape::OutArchive& arc, DeleteEdgeTypeRedo& redo);
};

struct InsertVertexRedo {
  label_t label;
  Property oid;
  std::vector<Property> props;

  static void Serialize(grape::InArchive& arc, label_t label,
                        const Property& oid,
                        const std::vector<Property>& props);
  static void Deserialize(grape::OutArchive& arc, InsertVertexRedo& redo);
};

struct InsertEdgeRedo {
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;
  std::vector<Property> properties;

  static void Serialize(grape::InArchive& arc, label_t src_label,
                        const Property& src, label_t dst_label,
                        const Property& dst, label_t edge_label,
                        const std::vector<Property>& properties);
  static void Deserialize(grape::OutArchive& arc, InsertEdgeRedo& redo);
};

struct UpdateVertexPropRedo {
  label_t label;
  Property oid;
  int prop_id;
  Property value;

  static void Serialize(grape::InArchive& arc, label_t label,
                        const Property& oid, int prop_id,
                        const Property& value);
  static void Deserialize(grape::OutArchive& arc, UpdateVertexPropRedo& redo);
};

struct UpdateEdgePropRedo {
  bool dir;
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;
  int prop_id;
  Property value;

  static void Serialize(grape::InArchive& arc, bool dir, label_t src_label,
                        const Property& src, label_t dst_label,
                        const Property& dst, label_t edge_label, int prop_id,
                        const Property& value);
  static void Deserialize(grape::OutArchive& arc, UpdateEdgePropRedo& redo);
};

struct RemoveVertexRedo {
  label_t label;
  Property oid;

  static void Serialize(grape::InArchive& arc, label_t label,
                        const Property& oid);
  static void Deserialize(grape::OutArchive& arc, RemoveVertexRedo& redo);
};

struct RemoveEdgeRedo {
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;

  static void Serialize(grape::InArchive& arc, label_t src_label,
                        const Property& src, label_t dst_label,
                        const Property& dst, label_t edge_label);
  static void Deserialize(grape::OutArchive& arc, RemoveEdgeRedo& redo);
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const CreateVertexTypeRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const CreateEdgeTypeRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const AddVertexPropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const AddEdgePropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const RenameVertexPropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const RenameEdgePropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const DeleteVertexPropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const DeleteEdgePropertiesRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const DeleteVertexTypeRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const DeleteEdgeTypeRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const InsertVertexRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const InsertEdgeRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const UpdateVertexPropRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const UpdateEdgePropRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const RemoveVertexRedo& value);
grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const RemoveEdgeRedo& value);

grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              CreateVertexTypeRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              CreateEdgeTypeRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              AddVertexPropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              AddEdgePropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              RenameVertexPropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              RenameEdgePropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              DeleteVertexPropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              DeleteEdgePropertiesRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              DeleteVertexTypeRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              DeleteEdgeTypeRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              InsertVertexRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              InsertEdgeRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              UpdateVertexPropRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              UpdateEdgePropRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              RemoveVertexRedo& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              RemoveEdgeRedo& value);

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_WAL_WAL_H_
