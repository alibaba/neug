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

#include "neug/storages/graph/cow_detach_state.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/wal/wal_builder.h"

namespace neug {

/**
 * @brief Graph mutation state in one of two modes.
 *
 * In COW mode the workspace owns a private graph clone plus the sparse set of
 * detached storage modules; commit publishes the clone as a replacement
 * snapshot. In in-place mode the workspace borrows the live published graph
 * and its mutable view for bulk/index operations; mutations are published by
 * bumping the current slot's generations without cloning (they cannot be
 * rolled back, so failure paths publish as well).
 *
 * This is shared mutation state, not a transaction. Admission, visibility,
 * durability, publication and allocation lifetime belong to the transaction
 * that owns it.
 */
class CowGraphWorkspace {
 public:
  CowGraphWorkspace(std::shared_ptr<PropertyGraph> cow_graph,
                    uint64_t base_planning_generation);

  // In-place mode: borrows the live published graph and its mutable view. The
  // caller must hold writer admission (the guarding transaction's write
  // guard) for the lifetime of the workspace.
  CowGraphWorkspace(PropertyGraph& live_graph, GraphView& live_view,
                    uint64_t base_planning_generation);

  CowGraphWorkspace(CowGraphWorkspace&&) noexcept = default;
  CowGraphWorkspace& operator=(CowGraphWorkspace&&) noexcept = default;
  CowGraphWorkspace(const CowGraphWorkspace&) = delete;
  CowGraphWorkspace& operator=(const CowGraphWorkspace&) = delete;

  bool is_in_place() const noexcept { return in_place_; }

  // The mutation target: the private clone (COW) or the live graph
  // (in-place). Valid for the lifetime of the owning transaction.
  PropertyGraph& storage() const noexcept {
    return in_place_ ? *live_graph_ : *cow_graph_;
  }

  // Owning handle to the private clone; COW mode only (commit hands it to the
  // snapshot store as the replacement snapshot).
  std::shared_ptr<PropertyGraph>& graph() { return cow_graph_; }
  const std::shared_ptr<PropertyGraph>& graph() const { return cow_graph_; }
  CowDetachState& detach_state() { return detach_state_; }
  const CowDetachState& detach_state() const { return detach_state_; }
  GraphView& view() { return in_place_ ? *live_view_ : view_; }
  const GraphView& view() const { return in_place_ ? *live_view_ : view_; }
  uint64_t base_planning_generation() const {
    return base_planning_generation_;
  }
  WalBuilder& logical_redo() { return logical_redo_; }
  const WalBuilder& logical_redo() const { return logical_redo_; }
  void MarkBatchMutation() noexcept { batch_mutation_changed_ = true; }
  // In-place mode: request a planning-generation bump at publication time.
  void MarkPlanningChanged() noexcept { planning_changed_ = true; }
  bool PlanningChanged() const noexcept {
    return logical_redo_.schema_changed() || batch_mutation_changed_ ||
           planning_changed_;
  }
  void Reset() noexcept;

 private:
  std::shared_ptr<PropertyGraph> cow_graph_;
  PropertyGraph* live_graph_{nullptr};
  CowDetachState detach_state_;
  GraphView view_;
  GraphView* live_view_{nullptr};
  uint64_t base_planning_generation_;
  WalBuilder logical_redo_;
  bool batch_mutation_changed_{false};
  bool planning_changed_{false};
  bool in_place_{false};
};

}  // namespace neug
