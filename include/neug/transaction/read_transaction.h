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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/read_snapshot_lease.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

class PropertyGraph;
template <typename EDATA_T>
class TypedMutableCsrBase;

/**
 * @brief Read-only transaction for consistent snapshot access to graph data.
 *
 * ReadTransaction provides read access to graph data at a specific timestamp,
 * implementing snapshot isolation. It retains a graph snapshot guard together
 * with the version manager and snapshot timestamp.
 *
 * **Implementation Details:**
 * - Stores const reference to PropertyGraph for read-only access
 * - Maintains timestamp for consistent snapshot view
 * - Calls release() in destructor for cleanup
 * - Commit() simply calls release() and returns true
 *
 * **Thread Safety:** Read operations are safe for concurrent access.
 *
 * @since v0.1.0
 */
class ReadTransaction {
 public:
  /**
   * @brief Construct a ReadTransaction with one coherent read lease.
   *
   * @param lease Timestamp, generation, and pinned snapshot owned together.
   *
   * @since v0.1.0
   */
  explicit ReadTransaction(ReadSnapshotLease lease);

  /**
   * @brief Destructor that calls release().
   *
   * Implementation: Calls release() to clean up resources.
   *
   * @since v0.1.0
   */
  ~ReadTransaction();

  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

  const GraphView& view() const { return lease_.view(); }

  GraphStats statistic() const { return GraphStats(view()); }

  const Schema& schema() const;

 private:
  void release();
  ReadSnapshotLease lease_;
};

}  // namespace neug
