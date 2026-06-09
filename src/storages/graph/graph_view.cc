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

#include "neug/storages/graph/graph_view.h"

namespace neug {

VertexSet GraphView::GetVertexSet(label_t label, timestamp_t ts) const {
  return pg_->get_vertex_table(label).GetVertexSet(ts);
}

bool GraphView::get_lid(label_t label, const execution::Value& oid, vid_t& lid,
                        timestamp_t ts) const {
  return pg_->get_lid(label, oid, lid, ts);
}

execution::Value GraphView::GetOid(label_t label, vid_t lid) const {
  return pg_->GetOid(label, lid, MAX_TIMESTAMP);
}

bool GraphView::IsValidLid(label_t label, vid_t lid, timestamp_t ts) const {
  return pg_->IsValidLid(label, lid, ts);
}

std::shared_ptr<RefColumnBase> GraphView::GetVertexPropertyColumn(
    label_t label, const std::string& prop) const {
  return pg_->GetVertexPropertyColumn(label, prop);
}

std::shared_ptr<RefColumnBase> GraphView::GetVertexPropertyColumn(
    label_t label, int col_id) const {
  return pg_->GetVertexPropertyColumn(label, col_id);
}

CsrView GraphView::GetGenericOutgoingView(label_t src_label, label_t dst_label,
                                          label_t edge_label,
                                          timestamp_t ts) const {
  return pg_->GetGenericOutgoingGraphView(src_label, dst_label, edge_label, ts);
}

CsrView GraphView::GetGenericIncomingView(label_t src_label, label_t dst_label,
                                          label_t edge_label,
                                          timestamp_t ts) const {
  return pg_->GetGenericIncomingGraphView(dst_label, src_label, edge_label, ts);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(label_t src_label,
                                                label_t dst_label,
                                                label_t edge_label,
                                                int prop_id) const {
  return pg_->get_edge_table(src_label, dst_label, edge_label)
      .get_edge_data_accessor(prop_id);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(
    label_t src_label, label_t dst_label, label_t edge_label,
    const std::string& prop_name) const {
  return pg_->get_edge_table(src_label, dst_label, edge_label)
      .get_edge_data_accessor(prop_name);
}

Status GraphView::AddVertex(label_t label, const execution::Value& id,
                            const std::vector<execution::Value>& props,
                            vid_t& vid, timestamp_t ts) {
  return pg_->AddVertex(label, id, props, vid, ts, false);
}

Status GraphView::AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                          vid_t dst_lid, label_t edge_label,
                          const std::vector<execution::Value>& properties,
                          timestamp_t ts, Allocator& alloc, int32_t& oe_offset,
                          const void*& prop) {
  return pg_->AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                      properties, ts, alloc, oe_offset, prop, false);
}

}  // namespace neug
