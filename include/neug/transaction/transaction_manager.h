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

#ifndef NEUG_TRANSACTION_TRANSACTION_MANAGER_H_
#define NEUG_TRANSACTION_TRANSACTION_MANAGER_H_

#include "neug/config.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/main/neug_db_session.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"

namespace gs {

struct SessionLocalContext {
  SessionLocalContext(PropertyGraph& graph_, AppManager& app_manager,
                      std::shared_ptr<IVersionManager> version_manager,
                      const std::string& work_dir, int thread_id,
                      MemoryStrategy allocator_strategy,
                      std::unique_ptr<IWalWriter> in_logger,
                      const NeugDBConfig& config_)
      : allocator(allocator_strategy,
                  (allocator_strategy != MemoryStrategy::kSyncToFile
                       ? ""
                       : thread_local_allocator_prefix(work_dir, thread_id))),
        logger(std::move(in_logger)),
        session(graph_, app_manager, version_manager, allocator, *logger,
                config_, work_dir, thread_id) {
    logger->open();
  }
  ~SessionLocalContext() {
    if (logger) {
      logger->close();
    }
  }
  Allocator allocator;
  char _padding0[128 - sizeof(Allocator) % 128];
  std::unique_ptr<IWalWriter> logger;
  char _padding1[4096 - sizeof(std::unique_ptr<IWalWriter>) -
                 sizeof(Allocator) - sizeof(_padding0)];
  NeugDBSession session;
  char _padding2[4096 - sizeof(NeugDBSession) % 4096];
};

/**
 * @brief TransactionManager manages multiple NeugDBSessions, each associated
 * with a thread. It provides methods to obtain different types of transactions
 * (read, insert, update, compact) for each session. It also handles the
 * ingestion of write-ahead logs (WALs) during initialization.
 */
class TransactionManager {
 public:
  TransactionManager(std::shared_ptr<AppManager> app_manager,
                     std::shared_ptr<IVersionManager> version_manager,
                     PropertyGraph& graph, const NeugDBConfig& config,
                     const std::string& work_dir);
  ~TransactionManager() { Clear(); }

  void Clear();

  size_t getExecutedQueryNum() const;

  size_t SessionNum() const { return thread_num_; }

  size_t GetAlloctedMemorySize(int thread_id) const;

  size_t GetEvalDuration(int thread_id) const;

  size_t GetQueryNum(int thread_id) const;

  ReadTransaction GetReadTransaction(int thread_id = 0) const;

  InsertTransaction GetInsertTransaction(int thread_id = 0);

  UpdateTransaction GetUpdateTransaction(int thread_id = 0);

  CompactTransaction GetCompactTransaction(int thread_id = 0);

  NeugDBSession& GetSession(int thread_id = 0);
  const NeugDBSession& GetSession(int thread_id = 0) const;

  /**
   * @brief Switch the version manager to TPVersionManager for serving.
   * This method should be called before starting the server to ensure that
   * the version manager is appropriate for transactional processing workloads.
   *
   * @param thread_num The number of threads to be used by the TPVersionManager.
   */
  void SwitchToTPMode(int32_t thread_num);

  /**
   * @brief Switch the version manager to APVersionManager for analytical
   * processing workloads. This method should be called before starting the
   * server to ensure that the version manager is appropriate for analytical
   * processing workloads.
   *
   * @param thread_num The number of threads to be used by the APVersionManager.
   */
  void SwitchToAPMode(int32_t thread_num);

  void SetVersionManager(std::shared_ptr<IVersionManager> vm);

 private:
  void ingestWals(IWalParser& parser, const std::string& work_dir,
                  MemoryStrategy allocator_strategy, int thread_num);

  int thread_num_;
  std::string work_dir_;
  std::string wal_uri_;

  std::shared_ptr<AppManager> app_manager_;
  std::shared_ptr<IVersionManager> version_manager_;
  PropertyGraph& graph_;
  NeugDBConfig config_;
  MemoryStrategy allocator_strategy_;

  SessionLocalContext* contexts_;
};
}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_TRANSACTION_MANAGER_H_