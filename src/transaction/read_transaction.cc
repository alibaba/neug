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

#include "neug/transaction/read_transaction.h"

#include <utility>

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/likely.h"

namespace gs {

ReadTransaction::ReadTransaction(const PropertyGraph& graph,
                                 IVersionManager& vm, timestamp_t timestamp)
    : graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

bool ReadTransaction::Commit() {
  release();
  return true;
}

void ReadTransaction::Abort() { release(); }

const PropertyGraph& ReadTransaction::graph() const { return graph_; }

ReadTransaction::vertex_iterator::vertex_iterator(label_t label, vid_t cur,
                                                  vid_t num, timestamp_t ts,
                                                  bool vertex_table_modified,
                                                  const PropertyGraph& graph)
    : label_(label),
      cur_(cur),
      num_(num),
      ts_(ts),
      vertex_table_modifed_(vertex_table_modified),
      graph_(graph) {
  while (cur_ < num_ && !graph_.is_valid_lid(label_, cur_, ts_)) {
    ++cur_;
  }
}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() {
  if (NEUG_UNLIKELY(vertex_table_modifed_)) {
    while (++cur_ < num_ && !graph_.is_valid_lid(label_, cur_, ts_)) {}
  } else {
    ++cur_;
  }
}
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  if (NEUG_UNLIKELY(vertex_table_modifed_)) {
    if (std::min(target, num_) < num_ &&
        !graph_.is_valid_lid(label_, target, ts_)) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Target vertex is deleted");
    }
  }
  cur_ = std::min(target, num_);
}

Any ReadTransaction::vertex_iterator::GetId() const {
  return graph_.get_oid(label_, cur_, ts_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

Any ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}

int ReadTransaction::vertex_iterator::FieldNum() const {
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {label,
          0,
          graph_.lid_num(label),
          timestamp_,
          graph_.vertex_table_modified(label),
          graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(
    label_t label, const Any& id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid, timestamp_)) {
    return {label,
            lid,
            graph_.lid_num(label),
            timestamp_,
            graph_.vertex_table_modified(label),
            graph_};
  } else {
    return {label,
            graph_.lid_num(label),
            graph_.lid_num(label),
            timestamp_,
            graph_.vertex_table_modified(label),
            graph_};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, const Any& id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index, timestamp_);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label, timestamp_);
}

VertexSet ReadTransaction::GetVertexSet(label_t label) const {
  return VertexSet(graph_.lid_num(label), graph_.get_vertex_timestamps(label),
                   timestamp_, graph_.vertex_table_modified(label));
}

bool ReadTransaction::IsValidVertex(label_t label, vid_t index) const {
  return index < graph_.lid_num(label) &&
         graph_.is_valid_lid(label, index, timestamp_);
}

Any ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index, timestamp_);
}

size_t ReadTransaction::GetOutDegree(label_t label, vid_t u,
                                     label_t neighbor_label,
                                     label_t edge_label) const {
  auto es = this->GetGenericOutgoingGraphView(label, neighbor_label, edge_label)
                .get_edges(u);
  int count = 0;
  for (auto it = es.begin(); it != es.end(); ++it) {
    count++;
  }
  return count;
}

size_t ReadTransaction::GetInDegree(label_t label, vid_t u,
                                    label_t neighbor_label,
                                    label_t edge_label) const {
  auto es = this->GetGenericIncomingGraphView(label, neighbor_label, edge_label)
                .get_edges(u);
  int count = 0;
  for (auto it = es.begin(); it != es.end(); ++it) {
    count++;
  }
  return count;
}

void ReadTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    vm_.release_read_timestamp();
    timestamp_ = INVALID_TIMESTAMP;
  }
}

}  // namespace gs
