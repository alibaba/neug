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

#include "neug/transaction/snapshot_read_transaction.h"

#include <utility>

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/likely.h"

namespace neug {

SnapshotReadTransaction::SnapshotReadTransaction(ReadSnapshotLease lease)
    : lease_(std::move(lease)) {}

SnapshotReadTransaction::~SnapshotReadTransaction() { release(); }

timestamp_t SnapshotReadTransaction::timestamp() const {
  return lease_.timestamp();
}

bool SnapshotReadTransaction::Commit() {
  release();
  return true;
}

void SnapshotReadTransaction::Abort() { release(); }

const Schema& SnapshotReadTransaction::schema() const {
  return lease_.view().schema();
}

void SnapshotReadTransaction::release() { lease_.release(); }

}  // namespace neug
