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

#include <stdint.h>
#include <string>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

/// Accumulates WAL operations for a single update transaction.
///
/// Each LogXxx method serializes the corresponding redo entry into an internal
/// buffer and increments the operation count. DDL Log methods additionally set
/// schema_changed_ = true.
///
/// LogCheckpoint() only increments op_num (no serialization) — used for the
/// checkpoint-only commit path where the snapshot is published but no WAL
/// content is written.
///
/// UpdateTransaction::Commit() uses:
///   - op_num() == 0  → nothing to do, early return
///   - op_num() > 0   → must publish snapshot
///   - content_size() > 0 → finalize() + append WAL
///   - content_size() == 0 → skip WAL write (checkpoint-only)
class WalBuilder {
 public:
  WalBuilder();

  // --- DDL logging (auto-sets schema_changed_) ---
  void LogCreateVertexType(const CreateVertexTypeParam& config);
  void LogCreateEdgeType(const CreateEdgeTypeParam& config);
  void LogAddVertexProperties(const AddVertexPropertiesParam& config);
  void LogAddEdgeProperties(const AddEdgePropertiesParam& config);
  void LogRenameVertexProperties(const RenameVertexPropertiesParam& config);
  void LogRenameEdgeProperties(const RenameEdgePropertiesParam& config);
  void LogDeleteVertexProperties(const DeleteVertexPropertiesParam& config);
  void LogDeleteEdgeProperties(const DeleteEdgePropertiesParam& config);
  void LogDeleteVertexType(const std::string& vertex_type);
  void LogDeleteEdgeType(const std::string& src_type,
                         const std::string& dst_type,
                         const std::string& edge_type);

  // --- DML logging ---
  void LogInsertVertex(label_t label, const Value& oid,
                       const std::vector<Value>& props);
  void LogInsertEdge(label_t src_label, const Value& src, label_t dst_label,
                     const Value& dst, label_t edge_label,
                     const std::vector<Value>& properties);
  void LogUpdateVertexProp(label_t label, const Value& oid, int prop_id,
                           const Value& value);
  void LogUpdateEdgeProp(label_t src_label, const Value& src, label_t dst_label,
                         const Value& dst, label_t edge_label,
                         int32_t oe_offset, int32_t ie_offset, int prop_id,
                         const Value& value);
  void LogRemoveVertex(label_t label, const Value& oid);
  void LogRemoveEdge(label_t src_label, const Value& src, label_t dst_label,
                     const Value& dst, label_t edge_label, int32_t oe_offset,
                     int32_t ie_offset);

  /// Checkpoint: only increments op_num, no WAL content serialized.
  void LogCheckpoint() { ++op_num_; }

  // --- Query state ---
  int op_num() const { return op_num_; }
  bool schema_changed() const { return schema_changed_; }

  /// Size of the WAL content (excluding header). 0 means checkpoint-only.
  size_t content_size() const { return arc_.GetSize() - sizeof(WalHeader); }

  /// Finalize the WAL header. Call only when content_size() > 0.
  void finalize(timestamp_t timestamp);

  /// Full buffer (header + content) after finalize().
  char* data() { return arc_.GetBuffer(); }
  size_t size() const { return arc_.GetSize(); }

  /// Reset all state for reuse or release.
  void clear();

 private:
  InArchive arc_;
  int op_num_{0};
  bool schema_changed_{false};
};

}  // namespace neug
