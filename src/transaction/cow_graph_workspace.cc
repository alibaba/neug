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

#include "neug/transaction/cow_graph_workspace.h"

#include <utility>

namespace neug {

CowGraphWorkspace::CowGraphWorkspace(std::shared_ptr<PropertyGraph> cow_graph,
                                     uint64_t base_planning_generation)
    : cow_graph_(std::move(cow_graph)),
      detach_state_(CowDetachState::FromSchema(cow_graph_->schema())),
      view_(*cow_graph_),
      base_planning_generation_(base_planning_generation) {}

void CowGraphWorkspace::Reset() noexcept {
  logical_redo_.clear();
  bulk_mutation_changed_ = false;
  transient_mutation_changed_ = false;
  // Drop the detach bookkeeping as well: every storage module reachable
  // through this workspace is released below, so retaining stale detached
  // markers would skip required detaches (and mutate shared storage) if the
  // workspace were ever reused.
  detach_state_ = CowDetachState();
  view_ = GraphView();
  cow_graph_.reset();
}

}  // namespace neug
