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

bool ReadTransaction::GetVertexIndex(label_t label, const Property& id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index, timestamp_);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.VertexNum(label, timestamp_);
}

VertexSet ReadTransaction::GetVertexSet(label_t label) const {
  return graph_.GetVertexSet(label, timestamp_);
}

bool ReadTransaction::IsValidVertex(label_t label, vid_t index) const {
  return index < graph_.LidNum(label) &&
         graph_.IsValidLid(label, index, timestamp_);
}

Property ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.GetOid(label, index, timestamp_);
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
