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
#include <string_view>
#include <unordered_map>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/wal/wal_codec.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

std::string get_wal_uri_scheme(const std::string& uri);
std::string get_wal_uri_path(const std::string& uri);

/**
 * The interface of wal writer.
 *
 * Implementations own their resources and must release them without throwing
 * from their destructors. close() remains available for callers that need
 * explicit error reporting.
 *
 * A writer belongs to the wal_dir() it was opened on; checkpoint ownership
 * of WAL files is guaranteed by checkpoint rotation, not by writer state.
 */
class IWalWriter {
 public:
  virtual ~IWalWriter() noexcept = default;

  virtual std::string type() const = 0;
  /**
   * Open a WAL file under @p wal_uri and persist the v1 file header.
   * In service mode, each logical execution slot owns one writer. The slot
   * may move between pthread workers and must retain the same writer for the
   * full transaction.
   * The uri could be a file_path or a remote connection string.
   */
  virtual void open(const std::string& wal_uri) = 0;

  /**
   * Close the wal writer. If a remote connection is hold by the wal writer,
   * it should be closed.
   */
  virtual void close() = 0;

  /**
   * Append one complete transaction frame: the frame header (including the
   * checksum computed over the payload) followed by the payload itself.
   */
  virtual bool append_frame(uint32_t commit_timestamp, WalRecordKind kind,
                            const char* payload, size_t length) = 0;
};

/// A validated replay record produced by the parser.
///
/// The payload is a view into a WAL file that the parser keeps mapped; it is
/// valid until the parser is closed or destroyed. file_index and
/// source_offset locate the frame inside the parser's source files and are
/// used for diagnostics.
struct WalReplayUnit {
  uint32_t commit_timestamp{0};
  WalRecordKind kind{WalRecordKind::kInsert};
  std::string_view payload;
  uint32_t file_index{0};
  uint64_t source_offset{0};
};

/**
 * The interface of wal parser.
 *
 * open() validates every WAL file in the given wal_dir() against the v1
 * protocol, collects all complete frames across writer files and exposes
 * them as a strictly timestamp-ordered, duplicate-free replay sequence. Any
 * validation failure aborts open() with a typed recovery error before any
 * graph mutation.
 */
class IWalParser {
 public:
  virtual ~IWalParser() {}

  /**
   * Open and validate the wal files under @p wal_uri.
   */
  virtual void open(const std::string& wal_uri) = 0;

  virtual void close() = 0;

  virtual uint32_t last_ts() const = 0;

  /**
   * All validated frames across every writer file, strictly ascending by
   * commit timestamp. Duplicate timestamps are rejected during open().
   * Payloads are views into files the parser keeps mapped; they stay valid
   * until close() or destruction.
   */
  virtual const std::vector<WalReplayUnit>& replay_units() const = 0;
};

class WalWriterFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)(
      const std::string& wal_uri, int32_t slot_id);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalWriter> CreateDummyWalWriter();

  static std::unique_ptr<IWalWriter> CreateWalWriter(const std::string& wal_uri,
                                                     int32_t slot_id);

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
  std::string vertex_type;
  AddVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const AddVertexPropertiesParam& config);
  static AddVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct AddEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  AddEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const AddEdgePropertiesParam& config);
  static AddEdgePropertiesRedo Deserialize(OutArchive& arc);
};

struct RenameVertexPropertiesRedo {
  std::string vertex_type;
  RenameVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const RenameVertexPropertiesParam& config);
  static RenameVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct RenameEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  RenameEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const RenameEdgePropertiesParam& config);
  static RenameEdgePropertiesRedo Deserialize(OutArchive& arc);
};

struct DeleteVertexPropertiesRedo {
  std::string vertex_type;
  DeleteVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const DeleteVertexPropertiesParam& config);
  static DeleteVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct DeleteEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  DeleteEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const DeleteEdgePropertiesParam& config);
  static DeleteEdgePropertiesRedo Deserialize(OutArchive& arc);
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
  Value oid;
  std::vector<Value> props;

  static void Serialize(InArchive& arc, label_t label, const Value& oid,
                        const std::vector<Value>& props);
  static void Deserialize(OutArchive& arc, InsertVertexRedo& redo);
};

struct InsertEdgeRedo {
  label_t src_label;
  Value src;
  label_t dst_label;
  Value dst;
  label_t edge_label;
  std::vector<Value> properties;

  static void Serialize(InArchive& arc, label_t src_label, const Value& src,
                        label_t dst_label, const Value& dst, label_t edge_label,
                        const std::vector<Value>& properties);
  static void Deserialize(OutArchive& arc, InsertEdgeRedo& redo);
};

struct UpdateVertexPropRedo {
  label_t label;
  Value oid;
  int prop_id;
  Value value;

  static void Serialize(InArchive& arc, label_t label, const Value& oid,
                        int prop_id, const Value& value);
  static void Deserialize(OutArchive& arc, UpdateVertexPropRedo& redo);
};

struct UpdateEdgePropRedo {
  label_t src_label;
  Value src;
  label_t dst_label;
  Value dst;
  label_t edge_label;
  int32_t oe_offset, ie_offset;
  int prop_id;
  Value value;

  static void Serialize(InArchive& arc, label_t src_label, const Value& src,
                        label_t dst_label, const Value& dst, label_t edge_label,
                        int32_t oe_offset, int32_t ie_offset, int prop_id,
                        const Value& value);
  static void Deserialize(OutArchive& arc, UpdateEdgePropRedo& redo);
};

struct RemoveVertexRedo {
  label_t label;
  Value oid;

  static void Serialize(InArchive& arc, label_t label, const Value& oid);
  static void Deserialize(OutArchive& arc, RemoveVertexRedo& redo);
};

struct RemoveEdgeRedo {
  label_t src_label;
  Value src;
  label_t dst_label;
  Value dst;
  label_t edge_label;
  int32_t oe_offset, ie_offset;

  static void Serialize(InArchive& arc, label_t src_label, const Value& src,
                        label_t dst_label, const Value& dst, label_t edge_label,
                        int32_t oe_offset, int32_t ie_offset);
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
