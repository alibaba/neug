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
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/column.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace gs {

class NeugDB;
class IWalWriter;
class ColumnBase;
class Encoder;
class PropertyGraph;
class RefColumnBase;
class Schema;
class AppManager;

}  // namespace gs

namespace server {

/**
 * @brief Database session for executing queries and managing transactions.
 *
 * NeugDBSession provides a session-based interface for interacting with the
 * NeuG database. Each session maintains its own transaction context and
 * application state, enabling concurrent access while ensuring data
 * consistency.
 *
 * **Core Capabilities:**
 * - Transaction management (Read, Insert, Update, Compact)
 * - Query execution with various data sources
 * - Batch operations for performance optimization
 * - Application-specific query processing
 *
 * **Transaction Types:**
 * - ReadTransaction: Read-only snapshot access
 * - InsertTransaction: Add new vertices and edges
 * - UpdateTransaction: Modify existing graph elements
 * - CompactTransaction: Background compaction operations
 *
 * **Thread Safety:** Each session is tied to a specific thread and should
 * not be shared across threads. Sessions are lightweight and can be created
 * per-thread as needed.
 *
 * **Lifecycle:**
 * - Created through NeugDB::CreateSession()
 * - Automatically manages transaction lifecycle
 * - Provides retry mechanisms for failed operations
 *
 * @note This is the primary interface for query execution and transaction
 *       management. Use NeugDB for database lifecycle operations.
 *
 * @since v0.1.0
 */
class NeugDBSession {
 public:
  static constexpr int32_t MAX_RETRY = 3;
  NeugDBSession(gs::PropertyGraph& graph,
                std::shared_ptr<gs::IGraphPlanner> planner,
                std::shared_ptr<gs::IVersionManager> vm, gs::Allocator& alloc,
                gs::IWalWriter& logger, const gs::NeugDBConfig& config_,
                int thread_id)
      : graph_(graph),
        planner_(planner),
        version_manager_(vm),
        alloc_(alloc),
        logger_(logger),
        db_config_(config_),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {}
  ~NeugDBSession() {}

  gs::ReadTransaction GetReadTransaction() const;

  gs::InsertTransaction GetInsertTransaction();

  gs::UpdateTransaction GetUpdateTransaction();

  gs::CompactTransaction GetCompactTransaction();
  const gs::PropertyGraph& graph() const;
  gs::PropertyGraph& graph();

  const gs::Schema& schema() const;

  /**
   * @brief Execute a Cypher query within the session. Expect a json format
   * string.
   * {
   *  "query": "MATCH(n) return count(n)",
   *  "access_mode": "read/insert/update/schema"
   * }
   */
  gs::result<results::CollectiveResults> Eval(const std::string& query);

  int SessionId() const;

  double eval_duration() const;

  int64_t query_num() const;

 private:
  void update_planner_meta();
  void update_planner_stats();
  gs::PropertyGraph& graph_;
  std::shared_ptr<gs::IGraphPlanner> planner_;
  std::shared_ptr<gs::IVersionManager> version_manager_;
  gs::Allocator& alloc_;
  gs::IWalWriter& logger_;
  const gs::NeugDBConfig& db_config_;
  int thread_id_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace server
