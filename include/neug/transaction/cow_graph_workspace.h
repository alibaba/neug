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

#include <cstdint>
#include <memory>
#include <vector>

#include "neug/storages/graph/cow_detach_state.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/wal/wal_builder.h"

namespace neug {

/**
 * @brief Private COW graph mutation state.
 *
 * This is shared mutation state, not a transaction. Admission, visibility,
 * durability, publication and allocation lifetime belong to the transaction
 * that owns it.
 */
class CowGraphWorkspace {
 public:
  CowGraphWorkspace(std::shared_ptr<PropertyGraph> cow_graph,
                    uint64_t base_planning_generation);

  CowGraphWorkspace(CowGraphWorkspace&&) noexcept = default;
  CowGraphWorkspace& operator=(CowGraphWorkspace&&) noexcept = default;
  CowGraphWorkspace(const CowGraphWorkspace&) = delete;
  CowGraphWorkspace& operator=(const CowGraphWorkspace&) = delete;

  PropertyGraph& storage() const noexcept { return *cow_graph_; }

  // Owning handle to the private clone; COW mode only (commit hands it to the
  // snapshot store as the replacement snapshot).
  std::shared_ptr<PropertyGraph>& graph() { return cow_graph_; }
  const std::shared_ptr<PropertyGraph>& graph() const { return cow_graph_; }
  CowDetachState& detach_state() { return detach_state_; }
  const CowDetachState& detach_state() const { return detach_state_; }
  GraphView& view() { return view_; }
  const GraphView& view() const { return view_; }
  uint64_t base_planning_generation() const {
    return base_planning_generation_;
  }
  WalBuilder& logical_redo() { return logical_redo_; }
  const WalBuilder& logical_redo() const { return logical_redo_; }
  void MarkBulkMutation() noexcept { bulk_mutation_changed_ = true; }
  // Persistent vertex COPY finalizes timestamp-zero rows immediately before
  // the private graph is consumed by its checkpoint. Keep the target set
  // transaction-local so unrelated dirty vertex tables are not compacted.
  void MarkBulkVertexTableForCheckpoint(label_t vertex_label);
  const std::vector<label_t>& bulk_vertex_tables_for_checkpoint() const {
    return bulk_vertex_tables_for_checkpoint_;
  }
  // Persistent edge COPY compacts edge MVCC state and restores configured
  // neighbor ordering immediately before its checkpoint is consumed. Keep the
  // target set transaction-local so unrelated dirty edge tables are untouched.
  void MarkBulkEdgeTableForCheckpoint(uint32_t edge_triplet_id);
  const std::vector<uint32_t>& bulk_edge_tables_for_checkpoint() const {
    return bulk_edge_tables_for_checkpoint_;
  }
  // Finalize persistent COPY targets recorded in this workspace: compact the
  // timestamp-zero tail of bulk-loaded vertex tables, compact edge MVCC state,
  // and restore configured neighbor ordering. CommitCowWrite invokes this
  // immediately before the checkpoint consumes the private graph, while normal
  // rollback is still safe.
  void FinalizeBulkTablesForCheckpoint();
  bool HasBulkMutation() const noexcept { return bulk_mutation_changed_; }
  void MarkTransientMutation() noexcept { transient_mutation_changed_ = true; }
  bool HasTransientMutation() const noexcept {
    return transient_mutation_changed_;
  }
  bool PlanningChanged() const noexcept {
    return logical_redo_.schema_changed() || bulk_mutation_changed_ ||
           transient_mutation_changed_;
  }
  // Terminal full reset: drops the redo buffer, all detach bookkeeping and
  // every graph/view reference. The workspace owns nothing afterwards and is
  // not reusable — the owning transaction is expected to be released next.
  void Reset() noexcept;

 private:
  std::shared_ptr<PropertyGraph> cow_graph_;
  CowDetachState detach_state_;
  GraphView view_;
  uint64_t base_planning_generation_;
  WalBuilder logical_redo_;
  bool bulk_mutation_changed_{false};
  bool transient_mutation_changed_{false};
  std::vector<label_t> bulk_vertex_tables_for_checkpoint_;
  std::vector<uint32_t> bulk_edge_tables_for_checkpoint_;
};

}  // namespace neug
