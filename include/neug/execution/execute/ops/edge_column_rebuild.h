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

#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/common/columns/edge_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug::execution::ops {

struct EdgeColumnSnapshot {
  std::shared_ptr<IEdgeColumn> column;
  std::vector<EdgeRecord> records;
  std::vector<std::pair<int32_t, int32_t>> offsets;
  std::vector<bool> refresh_rows;
  std::vector<std::shared_ptr<IContextColumn>*> aliases;
};

struct EdgeColumnSnapshots {
  const EdgeColumnSnapshot* Find(const IEdgeColumn* column) const;

  std::vector<EdgeColumnSnapshot> columns;
  std::unordered_map<const IEdgeColumn*, size_t> ids;
};

EdgeColumnSnapshots CaptureEdgeColumnsForRefresh(
    StorageUpdateInterface& graph, Context& ctx,
    const std::set<LabelTriplet>& affected_labels);

void RefreshEdgeColumns(StorageUpdateInterface& graph,
                        EdgeColumnSnapshots& snapshots);

std::pair<int32_t, int32_t> ResolveEdgeOffsets(StorageUpdateInterface& graph,
                                               const EdgeRecord& record);

void RefreshEdgeRecord(StorageUpdateInterface& graph, EdgeRecord& record,
                       const std::pair<int32_t, int32_t>& offsets);

}  // namespace neug::execution::ops
