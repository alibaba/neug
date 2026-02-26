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

#include "neug/config.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"

#include "bthread/bthread.h"

namespace neug {
class NeugDBService;

struct SessionLocalContext {
  SessionLocalContext(
      PropertyGraph& graph_, std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      std::shared_ptr<Allocator> alloc,
      std::shared_ptr<IVersionManager> version_manager,
      const std::string& work_dir, int thread_id,
      std::unique_ptr<IWalWriter> in_logger, const NeugDBConfig& config_)
      : allocator(alloc),
        logger(std::move(in_logger)),
        session(graph_, planner, global_query_cache, version_manager, *alloc,
                *logger, config_, thread_id) {
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

class SessionPool;

/**
 * @brief RAII guard for session lifecycle management.
 *
 * SessionGuard provides automatic session release through RAII pattern.
 * When the guard goes out of scope, the session is automatically returned
 * to the SessionPool for reuse.
 *
 * **Usage Example:**
 * @code{.cpp}
 * {
 *   // Acquire session - blocks if none available
 *   auto guard = service.AcquireSession();
 *
 *   // Use session for queries
 *   auto result = guard->Eval(query);
 *
 * } // Session automatically released here
 * @endcode
 *
 * **Thread Safety:** SessionGuard is move-only (non-copyable) to ensure
 * exclusive session ownership. Each guard should be used by a single thread.
 *
 * @see SessionPool for session pool management
 * @see NeugDBSession for query execution
 * @since v0.1.0
 */
class SessionGuard {
 public:
  SessionGuard() : session_(nullptr), pool_(nullptr), session_id_(0) {}

  SessionGuard(NeugDBSession* session, SessionPool* pool, size_t session_id)
      : session_(session), pool_(pool), session_id_(session_id) {}

  ~SessionGuard();

  SessionGuard(SessionGuard&& other) noexcept
      : session_(other.session_),
        pool_(other.pool_),
        session_id_(other.session_id_) {
    other.session_ = nullptr;
    other.pool_ = nullptr;
  }
  SessionGuard(const SessionGuard&) = delete;
  SessionGuard& operator=(const SessionGuard&) = delete;

  inline NeugDBSession* get() const { return session_; }
  inline NeugDBSession* operator->() const { return session_; }
  inline explicit operator bool() const { return session_ != nullptr; }

 private:
  // We don't own the session, just a pointer to it
  NeugDBSession* session_;
  SessionPool* pool_;
  size_t session_id_;
};

/**
 * @brief Pool of database sessions for concurrent query execution.
 *
 * SessionPool manages a fixed-size pool of NeugDBSession instances,
 * providing efficient session reuse for high-throughput scenarios.
 * Sessions are pre-allocated during pool construction and recycled
 * through acquire/release operations.
 *
 * SessionPool is used internally by NeugDBService. For most use cases,
 * access sessions through NeugDBService::AcquireSession() rather than
 * directly through the pool.
 *
 * **Key Features:**
 * - Pre-allocated sessions for zero-allocation query execution
 * - Thread-safe acquire/release with bthread synchronization
 * - Automatic WAL (Write-Ahead Log) management per session
 * - Memory-aligned session contexts for cache efficiency
 *
 * **Pool Size:** Determined by `NeugDBConfig::thread_num`, typically
 * matching the number of concurrent request handlers.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see SessionGuard for RAII session management
 * @since v0.1.0
 */
class SessionPool {
 public:
  explicit SessionPool(
      PropertyGraph& graph, std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      std::shared_ptr<IVersionManager> version_manager,
      std::vector<std::shared_ptr<Allocator>>& allocators,
      const NeugDBConfig& config, const std::string& work_dir) {
    session_num_ = config.thread_num;
    auto wal_uri = parse_wal_uri(config.wal_uri, work_dir);
    WalWriterFactory::Init();
    contexts_ = static_cast<SessionLocalContext*>(
        aligned_alloc(4096, sizeof(SessionLocalContext) * config.thread_num));
    for (int i = 0; i < config.thread_num; ++i) {
      new (&contexts_[i]) SessionLocalContext(
          graph, planner, global_query_cache, allocators[i], version_manager,
          work_dir, i, WalWriterFactory::CreateWalWriter(wal_uri, i), config);
    }
    bthread_cond_init(&cond_, nullptr);
    bthread_mutex_init(&mutex_, nullptr);
    for (size_t i = 0; i < session_num_; ++i) {
      available_sessions_.push_back(i);
    }
    LOG(INFO) << "Initializing SessionPool with " << session_num_
              << " sessions.";
  }

  ~SessionPool() {
    if (contexts_ != nullptr) {
      for (int i = 0; i < session_num_; ++i) {
        contexts_[i].~SessionLocalContext();
      }
      free(contexts_);
      contexts_ = nullptr;
    }
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }

  /**
   * @brief Acquire a session from the pool. Blocks if no session is available.
   * @return SessionGuard managing the acquired session. The session is released
   *         back to the pool when the guard goes out of scope.
   */
  SessionGuard AcquireSession();

  inline size_t SessionNum() const { return session_num_; }

  /**
   * @brief Get the total number of executed queries across all sessions.
   *        Expect lock held by caller.
   * @return Total number of executed queries.
   */
  size_t getExecutedQueryNum() const {
    size_t ret = 0;
    for (int i = 0; i < session_num_; ++i) {
      ret += contexts_[i].session.query_num();
    }
    return ret;
  }

 private:
  void ReleaseSession(size_t session_id);

  friend class SessionGuard;
  friend class NeugDBService;

  SessionLocalContext* contexts_;
  size_t session_num_;
  std::deque<size_t> available_sessions_;
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
};

}  // namespace neug