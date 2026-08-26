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

#include "neug/execution/execute/ops/edge_column_rebuild.h"

#include <cassert>
#include "neug/storages/csr/csr_view_utils.h"

namespace neug::execution::ops {

namespace {

bool ContainsAffectedLabel(const IEdgeColumn& column,
                           const std::set<LabelTriplet>& affected_labels) {
  for (const auto& label : column.get_labels()) {
    if (affected_labels.contains(label)) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<IContextColumn> RebuildEdgeColumn(
    const IEdgeColumn& original, const std::vector<EdgeRecord>& records) {
  assert(original.size() == records.size());
  BDMLEdgeColumnBuilder builder(original.get_labels());
  builder.reserve(records.size());
  for (size_t row = 0; row < records.size(); ++row) {
    if (!original.has_value(row)) {
      builder.push_back_null();
      continue;
    }
    const auto& record = records[row];
    builder.push_back_opt(record.label, record.src, record.dst, record.prop,
                          record.dir);
  }
  return builder.finish();
}

}  // namespace

const EdgeColumnSnapshot* EdgeColumnSnapshots::Find(
    const IEdgeColumn* column) const {
  const auto it = ids.find(column);
  return it == ids.end() ? nullptr : &columns[it->second];
}

std::pair<int32_t, int32_t> ResolveEdgeOffsets(StorageUpdateInterface& graph,
                                               const EdgeRecord& record) {
  const auto outgoing = graph.GetGenericOutgoingGraphView(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  const auto incoming = graph.GetGenericIncomingGraphView(
      record.label.dst_label, record.label.src_label, record.label.edge_label);
  return record_to_csr_offset_pair(
      outgoing, incoming, record,
      graph.schema().get_edge_properties(record.label.src_label,
                                         record.label.dst_label,
                                         record.label.edge_label));
}

void RefreshEdgeRecord(StorageUpdateInterface& graph, EdgeRecord& record,
                       const std::pair<int32_t, int32_t>& offsets) {
  const auto outgoing = graph.GetGenericOutgoingGraphView(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  const auto incoming = graph.GetGenericIncomingGraphView(
      record.label.dst_label, record.label.src_label, record.label.edge_label);
  record.prop =
      get_edge_data_ptr_for_record(outgoing, incoming, record, offsets);
}

EdgeColumnSnapshots CaptureEdgeColumnsForRefresh(
    StorageUpdateInterface& graph, Context& ctx,
    const std::set<LabelTriplet>& affected_labels) {
  EdgeColumnSnapshots snapshots;
  if (affected_labels.empty()) {
    return snapshots;
  }

  auto capture = [&](std::shared_ptr<IContextColumn>& column) {
    auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(column);
    if (!edge_column || !ContainsAffectedLabel(*edge_column, affected_labels)) {
      return;
    }
    auto [it, inserted] =
        snapshots.ids.emplace(edge_column.get(), snapshots.columns.size());
    if (inserted) {
      snapshots.columns.push_back(EdgeColumnSnapshot{std::move(edge_column)});
    }
    snapshots.columns[it->second].aliases.push_back(&column);
  };

  for (auto& chunk : ctx.chunks()) {
    for (auto& column : chunk.columns()) {
      capture(column);
    }
    capture(chunk.head());
  }

  for (auto& snapshot : snapshots.columns) {
    const auto size = snapshot.column->size();
    snapshot.records.resize(size);
    snapshot.offsets.resize(size);
    snapshot.refresh_rows.resize(size, false);
    for (size_t row = 0; row < size; ++row) {
      if (!snapshot.column->has_value(row)) {
        continue;
      }
      auto& record = snapshot.records[row];
      record = snapshot.column->get_edge(row);
      if (!affected_labels.contains(record.label)) {
        continue;
      }
      snapshot.offsets[row] = ResolveEdgeOffsets(graph, record);
      snapshot.refresh_rows[row] = true;
    }
  }
  return snapshots;
}

void RefreshEdgeColumns(StorageUpdateInterface& graph,
                        EdgeColumnSnapshots& snapshots) {
  for (auto& snapshot : snapshots.columns) {
    bool affected = false;
    for (size_t row = 0; row < snapshot.records.size(); ++row) {
      if (!snapshot.refresh_rows[row]) {
        continue;
      }
      auto& record = snapshot.records[row];
      RefreshEdgeRecord(graph, record, snapshot.offsets[row]);
      affected = true;
    }
    if (!affected) {
      continue;
    }
    auto rebuilt = RebuildEdgeColumn(*snapshot.column, snapshot.records);
    for (auto* alias : snapshot.aliases) {
      *alias = rebuilt;
    }
  }
}

}  // namespace neug::execution::ops
