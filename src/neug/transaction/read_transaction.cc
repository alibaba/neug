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

#include "neug/main/app/cypher_runner_impl.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/version_manager.h"

namespace gs {

ReadTransaction::ReadTransaction(const GraphDBSession& session,
                                 const PropertyGraph& graph, VersionManager& vm,
                                 timestamp_t timestamp)
    : session_(session), graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

bool ReadTransaction::Commit() {
  release();
  return true;
}

void ReadTransaction::Abort() { release(); }

const PropertyGraph& ReadTransaction::graph() const { return graph_; }

ReadTransaction::vertex_iterator::vertex_iterator(label_t label, vid_t cur,
                                                  vid_t num,
                                                  const PropertyGraph& graph)
    : label_(label), cur_(cur), num_(num), graph_(graph) {
  is_deleted_ = graph.is_deleted(label);
}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() {
  if (is_deleted_) {
    while (++cur_ < num_ && !graph_.is_valid_lid(label_, cur_)) {}
  } else {
    ++cur_;
  }
}
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

Any ReadTransaction::vertex_iterator::GetId() const {
  return graph_.get_oid(label_, cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

Any ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}

int ReadTransaction::vertex_iterator::FieldNum() const {
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<CsrConstEdgeIterBase> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

Any ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}

bool ReadTransaction::edge_iterator::IsValid() const {
  return iter_->is_valid();
}

void ReadTransaction::edge_iterator::Next() { iter_->next(); }

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  return iter_->get_neighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {label, 0, graph_.lid_num(label), graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(
    label_t label, const Any& id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, lid, graph_.lid_num(label), graph_};
  } else {
    return {label, graph_.lid_num(label), graph_.lid_num(label), graph_};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, const Any& id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label);
}

VertexSet ReadTransaction::GetVertexSet(label_t label) const {
  return VertexSet(graph_.lid_num(label), graph_.get_vertex_tomb(label));
}

Any ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index);
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return {neighbor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighbor_label, edge_label)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return {neighbor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighbor_label, edge_label)};
}

size_t ReadTransaction::GetOutDegree(label_t label, vid_t u,
                                     label_t neighbor_label,
                                     label_t edge_label) const {
  return graph_.get_outgoing_edges(label, u, neighbor_label, edge_label)
      ->size();
}

size_t ReadTransaction::GetInDegree(label_t label, vid_t u,
                                    label_t neighbor_label,
                                    label_t edge_label) const {
  return graph_.get_incoming_edges(label, u, neighbor_label, edge_label)
      ->size();
}

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

const GraphDBSession& ReadTransaction::GetSession() const { return session_; }

}  // namespace gs
