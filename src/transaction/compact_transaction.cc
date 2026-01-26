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

#include "neug/transaction/compact_transaction.h"

#include <glog/logging.h>
#include <limits>
#include <ostream>

#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"

namespace neug {

CompactTransaction::CompactTransaction(PropertyGraph& graph, IWalWriter& logger,
                                       IVersionManager& vm, bool compact_csr,
                                       float reserve_ratio,
                                       timestamp_t timestamp)
    : graph_(graph),
      logger_(logger),
      vm_(vm),
      compact_csr_(compact_csr),
      reserve_ratio_(reserve_ratio),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

CompactTransaction::~CompactTransaction() { Abort(); }

timestamp_t CompactTransaction::timestamp() const { return timestamp_; }

bool CompactTransaction::Commit() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
    header->length = 0;
    header->timestamp = timestamp_;
    header->type = 1;

    if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
      LOG(ERROR) << "Failed to append wal log";
      Abort();
      return false;
    }
    arc_.Clear();

    LOG(INFO) << "before compact - " << timestamp_;
    graph_.Compact(compact_csr_, reserve_ratio_, timestamp_);
    LOG(INFO) << "after compact - " << timestamp_;

    vm_.release_update_timestamp(timestamp_);
    vm_.clear();
    timestamp_ = INVALID_TIMESTAMP;
  }
  return true;
}

void CompactTransaction::Abort() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    arc_.Clear();
    vm_.revert_update_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;
  }
}

}  // namespace neug
