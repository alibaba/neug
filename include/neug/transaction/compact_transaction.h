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

#include "neug/storages/graph_snapshot_store.h"
#include "neug/utils/property/types.h"

namespace neug {

class PropertyGraph;
class IVersionManager;
class WalWriterSlot;

class CompactTransaction {
 public:
  CompactTransaction(GraphSnapshotStore& snapshot_store,
                     WalWriterSlot& wal_writer_slot, IVersionManager& vm,
                     timestamp_t timestamp) noexcept;
  ~CompactTransaction();

  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

 private:
  SnapshotGuard guard_;
  WalWriterSlot& wal_writer_slot_;
  IVersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace neug
