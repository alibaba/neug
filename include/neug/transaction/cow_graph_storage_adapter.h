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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/storage_index.h"
#include "neug/transaction/cow_graph_workspace.h"

namespace neug {

/**
 * @brief Storage mutation adapter over a graph workspace.
 *
 * The adapter serves both workspace modes: COW transactions mutate a private
 * clone (detaching shared modules and recording WAL redo), while in-place
 * bulk/index operations mutate the live published graph directly. Every
 * mutation method dispatches on the workspace mode. Index DDL
 * (StorageIndexDDLInterface) is only implemented for the in-place mode; COW
 * transactions reject it with ERR_NOT_SUPPORTED.
 *
 * The adapter has no transaction-owner or execution-mode dependency. Its
 * caller supplies the visibility timestamps; the owning transaction supplies
 * admission, durability and publication.
 */
class CowGraphStorageAdapter : public StorageUpdateInterface,
                               public StorageIndexDDLInterface {
 public:
  CowGraphStorageAdapter(CowGraphWorkspace& workspace,
                         timestamp_t read_timestamp,
                         timestamp_t write_timestamp, Allocator& alloc)
      : StorageUpdateInterface(workspace.view(), read_timestamp),
        workspace_(workspace),
        graph_(workspace.storage()),
        detach_state_(workspace.detach_state()),
        mut_view_(workspace.view()),
        alloc_(alloc),
        ckp_(workspace.storage().checkpoint()),
        logical_redo_(workspace.logical_redo()),
        write_ts_(write_timestamp) {}
  ~CowGraphStorageAdapter() = default;

  neug::result<StorageIndex*> CreateIndex(
      std::unique_ptr<IndexMeta> meta) override;
  Status DropIndex(const std::string& name) override;
  Status ActivateIndexes() override;
  Status AddGraphEntry(const std::string& name,
                       const ProjectedGraphEntry& entry) override;
  Status DropGraphEntry(const std::string& name) override;

 private:
  void MarkVertexTableDirty(label_t label) override {
    graph_.MarkVertexTableDirty(label);
  }
  void MarkEdgeTableDirty(label_t src, label_t dst, label_t edge) override {
    graph_.MarkEdgeTableDirty(src, dst, edge);
  }
  void MarkSchemaDirty() override {
    graph_.MarkSchemaDirty();
    // In COW mode the planning bump is derived from logical redo
    // (schema_changed()); in in-place mode publication relies on the
    // transaction marking planning changed.
    if (workspace_.is_in_place()) {
      workspace_.MarkPlanningChanged();
    }
  }

  Status UpdateVertexPropertyImpl(label_t label, vid_t lid, int col_id,
                                  const Value& value) override;
  Status UpdateEdgePropertyImpl(label_t src_label, vid_t src, label_t dst_label,
                                vid_t dst, label_t edge_label,
                                int32_t oe_offset, int32_t ie_offset,
                                int32_t col_id, const Value& value) override;
  Status AddVertexImpl(label_t label, const Value& id,
                       const std::vector<Value>& props, vid_t& vid) override;
  Status AddEdgeImpl(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                     label_t edge_label, const std::vector<Value>& properties,
                     const void*& prop) override;
  Status DeleteVertexImpl(label_t label, vid_t lid) override;
  Status DeleteEdgesImpl(label_t src_label, vid_t src, label_t dst_label,
                         vid_t dst, label_t edge_label) override;
  Status DeleteEdgeImpl(label_t src_label, vid_t src, label_t dst_label,
                        vid_t dst, label_t edge_label, int32_t oe_offset,
                        int32_t ie_offset) override;

  result<std::vector<vid_t>> BatchAddVerticesImpl(
      label_t v_label_id,
      std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchAddEdgesImpl(
      label_t src_label, label_t dst_label, label_t edge_label,
      std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchDeleteVerticesImpl(label_t v_label_id,
                                 const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;

  Status CreateVertexTypeImpl(const CreateVertexTypeParam& config) override;
  Status CreateEdgeTypeImpl(const CreateEdgeTypeParam& config) override;
  Status AddVertexPropertiesImpl(
      label_t label, const AddVertexPropertiesParam& config) override;
  Status AddEdgePropertiesImpl(label_t src, label_t dst, label_t edge,
                               const AddEdgePropertiesParam& config) override;
  Status RenameVertexPropertiesImpl(
      label_t label, const RenameVertexPropertiesParam& config) override;
  Status RenameEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const RenameEdgePropertiesParam& config) override;
  Status DeleteVertexPropertiesImpl(
      label_t label, const DeleteVertexPropertiesParam& config) override;
  Status DeleteEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const DeleteEdgePropertiesParam& config) override;
  Status DeleteVertexTypeImpl(label_t label) override;
  Status DeleteEdgeTypeImpl(label_t src, label_t dst, label_t edge) override;

  Status detachVertexTableForInsert(label_t label);
  Status detachVertexTableForDelete(label_t label);
  Status detachVertexColumn(label_t label, int32_t col_id);
  Status detachEdgeTableForInsert(uint32_t edge_triplet_id);
  Status detachEdgeTableForDelete(uint32_t edge_triplet_id);
  Status detachEdgeColumn(uint32_t edge_triplet_id, int32_t col_id);
  Status detachAdjlists(uint32_t edge_triplet_id, vid_t src_lid, vid_t dst_lid,
                        Allocator& alloc);
  Status detachForResize(label_t label, size_t capacity);
  Status detachForResize(label_t src_label, label_t dst_label,
                         label_t edge_label, size_t capacity);
  Status prepareVertexDelete(label_t label, const std::vector<vid_t>& lids);
  Status detachIndex(StorageIndex& index);

  CowGraphWorkspace& workspace_;
  PropertyGraph& graph_;
  CowDetachState& detach_state_;
  GraphView& mut_view_;
  Allocator& alloc_;
  Checkpoint& ckp_;
  WalBuilder& logical_redo_;
  timestamp_t write_ts_;
};

}  // namespace neug
