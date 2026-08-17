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

#include "neug/storages/graph/cow_detach_ledger.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/wal/wal_builder.h"

namespace neug {

/**
 * @brief Private graph clone plus the sparse set of detached storage modules.
 *
 * This is shared mutation state, not a transaction. Admission, visibility,
 * durability, publication and allocation lifetime belong to the transaction
 * that owns it.
 */
class CowGraphWriteSet {
 public:
  CowGraphWriteSet(std::shared_ptr<PropertyGraph> cow_graph,
                   uint64_t base_planning_generation);

  CowGraphWriteSet(CowGraphWriteSet&&) noexcept = default;
  CowGraphWriteSet& operator=(CowGraphWriteSet&&) noexcept = default;
  CowGraphWriteSet(const CowGraphWriteSet&) = delete;
  CowGraphWriteSet& operator=(const CowGraphWriteSet&) = delete;

  std::shared_ptr<PropertyGraph>& graph() { return cow_graph_; }
  const std::shared_ptr<PropertyGraph>& graph() const { return cow_graph_; }
  CowDetachLedger& detach_state() { return detach_state_; }
  const CowDetachLedger& detach_state() const { return detach_state_; }
  GraphView& view() { return view_; }
  const GraphView& view() const { return view_; }
  uint64_t base_planning_generation() const {
    return base_planning_generation_;
  }
  WalBuilder& logical_redo() { return logical_redo_; }
  const WalBuilder& logical_redo() const { return logical_redo_; }
  void MarkBatchMutation() noexcept { batch_mutation_changed_ = true; }
  bool PlanningChanged() const noexcept {
    return logical_redo_.schema_changed() || batch_mutation_changed_;
  }
  void Reset() noexcept;

 private:
  std::shared_ptr<PropertyGraph> cow_graph_;
  CowDetachLedger detach_state_;
  GraphView view_;
  uint64_t base_planning_generation_;
  WalBuilder logical_redo_;
  bool batch_mutation_changed_{false};
};

}  // namespace neug
