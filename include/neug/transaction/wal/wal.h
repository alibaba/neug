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
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/storages/graph/operation_params.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

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
  static void Serialize(InArchive& arc, const CreateVertexTypeParam& config);
  static CreateVertexTypeParam Deserialize(OutArchive& arc);
};

struct CreateEdgeTypeRedo {
  static void Serialize(InArchive& arc, const CreateEdgeTypeParam& config);
  static CreateEdgeTypeParam Deserialize(OutArchive& arc);
};

struct AddVertexPropertiesRedo {
  static void Serialize(InArchive& arc, const AddVertexPropertiesParam& config);
  static AddVertexPropertiesParam Deserialize(OutArchive& arc);
};

struct AddEdgePropertiesRedo {
  static void Serialize(InArchive& arc, const AddEdgePropertiesParam& config);
  static AddEdgePropertiesParam Deserialize(OutArchive& arc);
};

struct RenameVertexPropertiesRedo {
  static void Serialize(InArchive& arc,
                        const RenameVertexPropertiesParam& config);
  static RenameVertexPropertiesParam Deserialize(OutArchive& arc);
};

struct RenameEdgePropertiesRedo {
  static void Serialize(InArchive& arc,
                        const RenameEdgePropertiesParam& config);
  static RenameEdgePropertiesParam Deserialize(OutArchive& arc);
};

struct DeleteVertexPropertiesRedo {
  static void Serialize(InArchive& arc,
                        const DeleteVertexPropertiesParam& config);
  static DeleteVertexPropertiesParam Deserialize(OutArchive& arc);
};

struct DeleteEdgePropertiesRedo {
  static void Serialize(InArchive& arc,
                        const DeleteEdgePropertiesParam& config);
  static DeleteEdgePropertiesParam Deserialize(OutArchive& arc);
};

struct DeleteVertexTypeRedo {
  std::string vertex_type;

  static void Serialize(InArchive& arc, const std::string& vertex_type);
  static void Deserialize(OutArchive& arc, DeleteVertexTypeRedo& redo);
};

struct DeleteEdgeTypeRedo {
  std::string src_type, dst_type, edge_type;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type);
  static void Deserialize(OutArchive& arc, DeleteEdgeTypeRedo& redo);
};

struct InsertVertexRedo {
  label_t label;
  Property oid;
  std::vector<Property> props;

  static void Serialize(InArchive& arc, label_t label, const Property& oid,
                        const std::vector<Property>& props);
  static void Deserialize(OutArchive& arc, InsertVertexRedo& redo);
};

struct InsertEdgeRedo {
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;
  std::vector<Property> properties;

  static void Serialize(InArchive& arc, label_t src_label, const Property& src,
                        label_t dst_label, const Property& dst,
                        label_t edge_label,
                        const std::vector<Property>& properties);
  static void Deserialize(OutArchive& arc, InsertEdgeRedo& redo);
};

struct UpdateVertexPropRedo {
  label_t label;
  Property oid;
  int prop_id;
  Property value;

  static void Serialize(InArchive& arc, label_t label, const Property& oid,
                        int prop_id, const Property& value);
  static void Deserialize(OutArchive& arc, UpdateVertexPropRedo& redo);
};

struct UpdateEdgePropRedo {
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;
  int32_t oe_offset, ie_offset;
  int prop_id;
  Property value;

  static void Serialize(InArchive& arc, label_t src_label, const Property& src,
                        label_t dst_label, const Property& dst,
                        label_t edge_label, int32_t oe_offset,
                        int32_t ie_offset, int prop_id, const Property& value);
  static void Deserialize(OutArchive& arc, UpdateEdgePropRedo& redo);
};

struct RemoveVertexRedo {
  label_t label;
  Property oid;

  static void Serialize(InArchive& arc, label_t label, const Property& oid);
  static void Deserialize(OutArchive& arc, RemoveVertexRedo& redo);
};

struct RemoveEdgeRedo {
  label_t src_label;
  Property src;
  label_t dst_label;
  Property dst;
  label_t edge_label;
  int32_t oe_offset, ie_offset;

  static void Serialize(InArchive& arc, label_t src_label, const Property& src,
                        label_t dst_label, const Property& dst,
                        label_t edge_label, int32_t oe_offset,
                        int32_t ie_offset);
  static void Deserialize(OutArchive& arc, RemoveEdgeRedo& redo);
};

InArchive& operator<<(InArchive& in_archive, const DeleteVertexTypeRedo& value);
InArchive& operator<<(InArchive& in_archive, const DeleteEdgeTypeRedo& value);
InArchive& operator<<(InArchive& in_archive, const InsertVertexRedo& value);
InArchive& operator<<(InArchive& in_archive, const InsertEdgeRedo& value);
InArchive& operator<<(InArchive& in_archive, const UpdateVertexPropRedo& value);
InArchive& operator<<(InArchive& in_archive, const UpdateEdgePropRedo& value);
InArchive& operator<<(InArchive& in_archive, const RemoveVertexRedo& value);
InArchive& operator<<(InArchive& in_archive, const RemoveEdgeRedo& value);

OutArchive& operator>>(OutArchive& out_archive, DeleteVertexTypeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, DeleteEdgeTypeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, InsertVertexRedo& value);
OutArchive& operator>>(OutArchive& out_archive, InsertEdgeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, UpdateVertexPropRedo& value);
OutArchive& operator>>(OutArchive& out_archive, UpdateEdgePropRedo& value);
OutArchive& operator>>(OutArchive& out_archive, RemoveVertexRedo& value);
OutArchive& operator>>(OutArchive& out_archive, RemoveEdgeRedo& value);

}  // namespace neug
