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

#include <algorithm>
#include <utility>

#include "neug/storages/graph/property_graph.h"

namespace neug {

CowGraphWorkspace::CowGraphWorkspace(std::shared_ptr<PropertyGraph> cow_graph,
                                     uint64_t base_planning_generation)
    : cow_graph_(std::move(cow_graph)),
      detach_state_(CowDetachState::FromSchema(cow_graph_->schema())),
      view_(*cow_graph_),
      base_planning_generation_(base_planning_generation) {}

void CowGraphWorkspace::MarkBulkVertexTableForCheckpoint(label_t vertex_label) {
  bulk_mutation_changed_ = true;
  if (std::find(bulk_vertex_tables_for_checkpoint_.begin(),
                bulk_vertex_tables_for_checkpoint_.end(),
                vertex_label) == bulk_vertex_tables_for_checkpoint_.end()) {
    bulk_vertex_tables_for_checkpoint_.push_back(vertex_label);
  }
}

void CowGraphWorkspace::MarkBulkEdgeTableForCheckpoint(
    uint32_t edge_triplet_id) {
  bulk_mutation_changed_ = true;
  if (std::find(bulk_edge_tables_for_checkpoint_.begin(),
                bulk_edge_tables_for_checkpoint_.end(),
                edge_triplet_id) == bulk_edge_tables_for_checkpoint_.end()) {
    bulk_edge_tables_for_checkpoint_.push_back(edge_triplet_id);
  }
}

void CowGraphWorkspace::FinalizeBulkTablesForCheckpoint() {
  auto& graph = *cow_graph_;
  for (label_t vertex_label : bulk_vertex_tables_for_checkpoint_) {
    graph.get_vertex_table(vertex_label).Compact();
  }
  for (uint32_t edge_triplet_id : bulk_edge_tables_for_checkpoint_) {
    const auto [src_label, dst_label, edge_label] =
        graph.schema().parse_edge_label(edge_triplet_id);
    const auto& sort_key =
        graph.schema().get_sort_key_for_nbr(src_label, dst_label, edge_label);
    graph.get_edge_table_by_index(edge_triplet_id).Compact(sort_key);
  }
}

void CowGraphWorkspace::Reset() noexcept {
  logical_redo_.clear();
  bulk_mutation_changed_ = false;
  transient_mutation_changed_ = false;
  bulk_vertex_tables_for_checkpoint_.clear();
  bulk_edge_tables_for_checkpoint_.clear();
  // Drop the detach bookkeeping as well: every storage module reachable
  // through this workspace is released below, so retaining stale detached
  // markers would skip required detaches (and mutate shared storage) if the
  // workspace were ever reused.
  detach_state_ = CowDetachState();
  view_ = GraphView();
  cow_graph_.reset();
}

}  // namespace neug
