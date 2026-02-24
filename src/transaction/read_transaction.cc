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

namespace neug {

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

void ReadTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    vm_.release_read_timestamp();
    timestamp_ = INVALID_TIMESTAMP;
  }
}

}  // namespace neug
