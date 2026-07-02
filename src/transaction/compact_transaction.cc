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
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"

namespace neug {

CompactTransaction::CompactTransaction(GraphSnapshotStore& snapshot_store,
                                       IWalWriter& logger, IVersionManager& vm,
                                       timestamp_t timestamp)
    : guard_(snapshot_store), logger_(logger), vm_(vm), timestamp_(timestamp) {
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
    // In-place compact
    auto& slot = guard_.get();
    slot.mutable_graph()->Compact(timestamp_);
    slot.mutable_view().Rebuild(*slot.mutable_graph());
    LOG(INFO) << "after compact - " << timestamp_;

    vm_.release_compact_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;
  }
  guard_.release();
  return true;
}

void CompactTransaction::Abort() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    arc_.Clear();
    vm_.revert_compact_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;
  }
  guard_.release();
}

}  // namespace neug
