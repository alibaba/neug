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

#ifndef ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_SESSION_H_
#define ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_SESSION_H_

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

#include "neug/main/app/app_base.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/column.h"
#include "neug/utils/result.h"

namespace gs {

class NeugDB;
class IWalWriter;
class ColumnBase;
class Encoder;
class PropertyGraph;
class RefColumnBase;
class Schema;
class UpdateBatch;
class AppManager;

/**
 * @brief Database session for executing queries and managing transactions.
 * 
 * NeugDBSession provides a session-based interface for interacting with the
 * NeuG database. Each session maintains its own transaction context and
 * application state, enabling concurrent access while ensuring data consistency.
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
  NeugDBSession(PropertyGraph& graph, AppManager& app_manager,
                std::shared_ptr<IVersionManager> vm, Allocator& alloc,
                IWalWriter& logger, const NeugDBConfig& config_,
                const std::string& work_dir, int thread_id)
      : graph_(graph),
        app_manager_(app_manager),
        version_manager_(vm),
        alloc_(alloc),
        logger_(logger),
        db_config_(config_),
        work_dir_(work_dir),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {
    for (auto& app : apps_) {
      app = nullptr;
    }
  }
  ~NeugDBSession() {}

  ReadTransaction GetReadTransaction() const;

  InsertTransaction GetInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

  bool BatchUpdate(UpdateBatch& batch);

  const PropertyGraph& graph() const;
  PropertyGraph& graph();

  const Schema& schema() const;

  std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  // Get vertex id column.
  std::shared_ptr<RefColumnBase> get_vertex_id_column(uint8_t label) const;

  Result<std::vector<char>> Eval(const std::string& input);

  int SessionId() const;

  double eval_duration() const;

  const AppMetric& GetAppMetric(int idx) const;

  int64_t query_num() const;

  AppBase* GetApp(int idx);

  AppBase* GetApp(const std::string& name);

  inline const std::string& work_dir() const { return work_dir_; }

  inline void SetVersionManager(std::shared_ptr<IVersionManager> vm) {
    version_manager_ = vm;
  }

 private:
  /**
   * @brief Parse the input format of the query.
   *        There are four formats:
   *       0. CppEncoder: This format will be used by interactive-sdk to submit
   * c++ stored prcoedure queries. The second last byte is the query id.
   * @param input The input query.
   * @return The id of the query and a string_view which contains the real input
   * of the query, discard the input format and query type.
   */
  inline Result<std::pair<uint8_t, std::string_view>> parse_query_type(
      const std::string& input) {
    const char* str_data = input.data();
    VLOG(10) << "parse query type for " << input << " size: " << input.size();
    char input_tag = input.back();
    VLOG(10) << "input tag: " << static_cast<int>(input_tag);
    size_t len = input.size();
    if (input_tag == static_cast<uint8_t>(gs::InputFormat::kCppEncoder)) {
      // For cpp encoder, the query id is the second last byte, others are all
      // user-defined payload,
      return std::make_pair((uint8_t) input[len - 2],
                            std::string_view(str_data, len - 2));
    } else if (input_tag ==
               static_cast<uint8_t>(gs::InputFormat::kCypherString)) {
      return std::make_pair((uint8_t) input[len - 2],
                            std::string_view(str_data, len - 1));
    } else {
      return Result<std::pair<uint8_t, std::string_view>>(
          gs::Status(StatusCode::ERR_INVALID_ARGUMENT,
                     "Invalid input tag: " + std::to_string(input_tag)));
    }
  }
  PropertyGraph& graph_;
  AppManager& app_manager_;
  std::shared_ptr<IVersionManager> version_manager_;
  Allocator& alloc_;
  IWalWriter& logger_;
  const NeugDBConfig& db_config_;
  std::string work_dir_;
  int thread_id_;

  std::array<AppWrapper, MAX_PLUGIN_NUM> app_wrappers_;
  std::array<AppBase*, MAX_PLUGIN_NUM> apps_;
  std::array<AppMetric, MAX_PLUGIN_NUM> app_metrics_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_SESSION_H_
