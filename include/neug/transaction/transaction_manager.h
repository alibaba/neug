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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "neug/config.h"
#include "neug/main/neug_db_session.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"
#include "neug/utils/mmap_array.h"

namespace gs {

class IVersionManager;
class PropertyGraph;

struct SessionLocalContext {
  SessionLocalContext(PropertyGraph& graph_, std::shared_ptr<Allocator> alloc,
                      std::shared_ptr<IVersionManager> version_manager,
                      const std::string& work_dir, int thread_id,
                      std::unique_ptr<IWalWriter> in_logger,
                      const NeugDBConfig& config_)
      : allocator(alloc),
        logger(std::move(in_logger)),
        session(graph_, version_manager, *alloc, *logger, config_, thread_id) {
    logger->open();
  }
  ~SessionLocalContext() {
    if (logger) {
      logger->close();
    }
  }
  std::shared_ptr<Allocator> allocator;
  char _padding0[128 - sizeof(std::shared_ptr<Allocator>)];
  std::unique_ptr<IWalWriter> logger;
  char _padding1[4096 - sizeof(std::unique_ptr<IWalWriter>) -
                 sizeof(std::shared_ptr<Allocator>) - sizeof(_padding0)];
  NeugDBSession session;
  char _padding2[(4096 - sizeof(NeugDBSession) % 4096) % 4096];
};

/**
 * @brief TransactionManager manages multiple NeugDBSessions, each associated
 * with a thread. It provides methods to obtain different types of transactions
 * (read, insert, update, compact) for each session. It also handles the
 * ingestion of write-ahead logs (WALs) during initialization.
 */
class TransactionManager {
 public:
  TransactionManager(std::shared_ptr<IVersionManager> version_manager,
                     PropertyGraph& graph,
                     std::vector<std::shared_ptr<Allocator>>& allocators,
                     const NeugDBConfig& config, const std::string& work_dir,
                     int32_t thread_num = 1);
  ~TransactionManager() { Clear(); }

  void Clear();

  size_t getExecutedQueryNum() const;

  size_t SessionNum() const { return thread_num_; }

  size_t GetEvalDuration(int thread_id) const;

  size_t GetQueryNum(int thread_id) const;

  ReadTransaction GetReadTransaction(int thread_id = 0) const;

  InsertTransaction GetInsertTransaction(int thread_id = 0);

  UpdateTransaction GetUpdateTransaction(int thread_id = 0);

  CompactTransaction GetCompactTransaction(int thread_id = 0);

  NeugDBSession& GetSession(int thread_id = 0);
  const NeugDBSession& GetSession(int thread_id = 0) const;

 private:
  int thread_num_;
  std::string work_dir_;
  std::string wal_uri_;

  std::shared_ptr<IVersionManager> version_manager_;
  PropertyGraph& graph_;
  NeugDBConfig config_;
  MemoryStrategy allocator_strategy_;

  SessionLocalContext* contexts_;
};

}  // namespace gs
