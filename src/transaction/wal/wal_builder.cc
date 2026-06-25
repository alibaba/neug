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

#include "neug/transaction/wal/wal_builder.h"

#include "neug/transaction/wal/wal.h"

namespace neug {

WalBuilder::WalBuilder() = default;

void WalBuilder::ensure_header() {
  if (arc_.Empty()) {
    arc_.Resize(sizeof(WalHeader));
  }
}

void WalBuilder::finalize(timestamp_t timestamp) {
  ensure_header();
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 1;
  header->timestamp = timestamp;
}

void WalBuilder::clear() {
  arc_.Clear();
  op_num_ = 0;
  schema_changed_ = false;
}

// =============================================================================
// DDL logging (auto-sets schema_changed_)
// =============================================================================

void WalBuilder::LogCreateVertexType(const CreateVertexTypeParam& config) {
  ensure_header();
  CreateVertexTypeRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogCreateEdgeType(const CreateEdgeTypeParam& config) {
  ensure_header();
  CreateEdgeTypeRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogAddVertexProperties(
    const AddVertexPropertiesParam& config) {
  ensure_header();
  AddVertexPropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogAddEdgeProperties(const AddEdgePropertiesParam& config) {
  ensure_header();
  AddEdgePropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogRenameVertexProperties(
    const RenameVertexPropertiesParam& config) {
  ensure_header();
  RenameVertexPropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogRenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  ensure_header();
  RenameEdgePropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  ensure_header();
  DeleteVertexPropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteEdgeProperties(
    const DeleteEdgePropertiesParam& config) {
  ensure_header();
  DeleteEdgePropertiesRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteVertexType(const std::string& vertex_type) {
  ensure_header();
  DeleteVertexTypeRedo::Serialize(arc_, vertex_type);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteEdgeType(const std::string& src_type,
                                   const std::string& dst_type,
                                   const std::string& edge_type) {
  ensure_header();
  DeleteEdgeTypeRedo::Serialize(arc_, src_type, dst_type, edge_type);
  ++op_num_;
  schema_changed_ = true;
}

// =============================================================================
// DML logging
// =============================================================================

void WalBuilder::LogInsertVertex(label_t label, const execution::Value& oid,
                                 const std::vector<execution::Value>& props) {
  ensure_header();
  InsertVertexRedo::Serialize(arc_, label, oid, props);
  ++op_num_;
}

void WalBuilder::LogInsertEdge(
    label_t src_label, const execution::Value& src, label_t dst_label,
    const execution::Value& dst, label_t edge_label,
    const std::vector<execution::Value>& properties) {
  ensure_header();
  InsertEdgeRedo::Serialize(arc_, src_label, src, dst_label, dst, edge_label,
                            properties);
  ++op_num_;
}

void WalBuilder::LogUpdateVertexProp(label_t label, const execution::Value& oid,
                                     int prop_id,
                                     const execution::Value& value) {
  ensure_header();
  UpdateVertexPropRedo::Serialize(arc_, label, oid, prop_id, value);
  ++op_num_;
}

void WalBuilder::LogUpdateEdgeProp(
    label_t src_label, const execution::Value& src, label_t dst_label,
    const execution::Value& dst, label_t edge_label, int32_t oe_offset,
    int32_t ie_offset, int prop_id, const execution::Value& value) {
  ensure_header();
  UpdateEdgePropRedo::Serialize(arc_, src_label, src, dst_label, dst,
                                edge_label, oe_offset, ie_offset, prop_id,
                                value);
  ++op_num_;
}

void WalBuilder::LogRemoveVertex(label_t label, const execution::Value& oid) {
  ensure_header();
  RemoveVertexRedo::Serialize(arc_, label, oid);
  ++op_num_;
}

void WalBuilder::LogRemoveEdge(label_t src_label, const execution::Value& src,
                               label_t dst_label, const execution::Value& dst,
                               label_t edge_label, int32_t oe_offset,
                               int32_t ie_offset) {
  ensure_header();
  RemoveEdgeRedo::Serialize(arc_, src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset);
  ++op_num_;
}

}  // namespace neug
