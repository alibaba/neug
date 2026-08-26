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

WalBuilder::WalBuilder() { arc_.Resize(sizeof(WalHeader)); }

void WalBuilder::finalize(timestamp_t timestamp) {
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
  CreateVertexTypeRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogCreateEdgeType(const CreateEdgeTypeParam& config) {
  CreateEdgeTypeRedo::Serialize(arc_, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogAddVertexProperties(
    const std::string& vertex_type, const AddVertexPropertiesParam& config) {
  AddVertexPropertiesRedo::Serialize(arc_, vertex_type, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogAddEdgeProperties(const std::string& src_type,
                                      const std::string& dst_type,
                                      const std::string& edge_type,
                                      const AddEdgePropertiesParam& config) {
  AddEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogRenameVertexProperties(
    const std::string& vertex_type, const RenameVertexPropertiesParam& config) {
  RenameVertexPropertiesRedo::Serialize(arc_, vertex_type, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogRenameEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type, const RenameEdgePropertiesParam& config) {
  RenameEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                      config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteVertexProperties(
    const std::string& vertex_type, const DeleteVertexPropertiesParam& config) {
  DeleteVertexPropertiesRedo::Serialize(arc_, vertex_type, config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type, const DeleteEdgePropertiesParam& config) {
  DeleteEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                      config);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteVertexType(const std::string& vertex_type) {
  DeleteVertexTypeRedo::Serialize(arc_, vertex_type);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDeleteEdgeType(const std::string& src_type,
                                   const std::string& dst_type,
                                   const std::string& edge_type) {
  DeleteEdgeTypeRedo::Serialize(arc_, src_type, dst_type, edge_type);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogCreateIndex(const IndexMeta& meta) {
  CreateIndexRedo::Serialize(arc_, meta);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogAddGraphEntry(const std::string& name,
                                  const ProjectedGraphEntry& entry) {
  AddGraphEntryRedo::Serialize(arc_, name, entry);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDropIndex(const std::string& name) {
  DropIndexRedo::Serialize(arc_, name);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogActivateIndexes() {
  ActivateIndexesRedo::Serialize(arc_);
  ++op_num_;
  schema_changed_ = true;
}

void WalBuilder::LogDropGraphEntry(const std::string& name) {
  DropGraphEntryRedo::Serialize(arc_, name);
  ++op_num_;
  schema_changed_ = true;
}

// =============================================================================
// DML logging
// =============================================================================

void WalBuilder::LogInsertVertex(const std::string& vertex_type,
                                 const Value& oid,
                                 const std::vector<Value>& props) {
  InsertVertexRedo::Serialize(arc_, vertex_type, oid, props);
  ++op_num_;
}

void WalBuilder::LogInsertEdge(const std::string& src_type, const Value& src,
                               const std::string& dst_type, const Value& dst,
                               const std::string& edge_type,
                               const std::vector<Value>& properties) {
  InsertEdgeRedo::Serialize(arc_, src_type, src, dst_type, dst, edge_type,
                            properties);
  ++op_num_;
}

void WalBuilder::LogUpdateVertexProp(const std::string& vertex_type,
                                     const Value& oid, int prop_id,
                                     const Value& value) {
  UpdateVertexPropRedo::Serialize(arc_, vertex_type, oid, prop_id, value);
  ++op_num_;
}

void WalBuilder::LogUpdateEdgeProp(
    const std::string& src_type, const Value& src, const std::string& dst_type,
    const Value& dst, const std::string& edge_type, int32_t oe_offset,
    int32_t ie_offset, int prop_id, const Value& value) {
  UpdateEdgePropRedo::Serialize(arc_, src_type, src, dst_type, dst, edge_type,
                                oe_offset, ie_offset, prop_id, value);
  ++op_num_;
}

void WalBuilder::LogRemoveVertex(const std::string& vertex_type,
                                 const Value& oid) {
  RemoveVertexRedo::Serialize(arc_, vertex_type, oid);
  ++op_num_;
}

void WalBuilder::LogRemoveEdge(const std::string& src_type, const Value& src,
                               const std::string& dst_type, const Value& dst,
                               const std::string& edge_type, int32_t oe_offset,
                               int32_t ie_offset) {
  RemoveEdgeRedo::Serialize(arc_, src_type, src, dst_type, dst, edge_type,
                            oe_offset, ie_offset);
  ++op_num_;
}

}  // namespace neug
