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
#include <string>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/result.h"

namespace neug {

class GraphView {
 public:
  explicit GraphView(PropertyGraph& storage) : pg_(&storage) {}

  GraphView() = default;
  ~GraphView() = default;

  GraphView(const GraphView&) = default;
  GraphView(GraphView&&) = default;
  GraphView& operator=(const GraphView&) = default;
  GraphView& operator=(GraphView&&) = default;

  const Schema& schema() const { return pg_->schema(); }

  // Vertex-side read API (keyed by label).
  VertexSet GetVertexSet(label_t label, timestamp_t ts) const;
  bool get_lid(label_t label, const execution::Value& oid, vid_t& lid,
               timestamp_t ts) const;
  execution::Value GetOid(label_t label, vid_t lid) const;
  bool IsValidLid(label_t label, vid_t lid, timestamp_t ts) const;
  std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      label_t label, const std::string& prop) const;
  std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(label_t label,
                                                         int col_id) const;

  // Edge-side read API (keyed by triplet).
  CsrView GetGenericOutgoingView(label_t src_label, label_t dst_label,
                                 label_t edge_label, timestamp_t ts) const;
  CsrView GetGenericIncomingView(label_t src_label, label_t dst_label,
                                 label_t edge_label, timestamp_t ts) const;
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const;
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& prop_name) const;

  // Mutators.
  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props, vid_t& vid,
                   timestamp_t ts);

  Status AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                 vid_t dst_lid, label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 timestamp_t ts, Allocator& alloc, int32_t& oe_offset,
                 const void*& prop);

 private:
  PropertyGraph* pg_{nullptr};
};

}  // namespace neug
