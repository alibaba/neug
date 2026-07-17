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

#include <cstdlib>
#include <deque>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "neug/config.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/allocators.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/wal/wal.h"

#include "bthread/bthread.h"

namespace neug {
class CheckpointCoordinator;
class SessionPool;

inline constexpr size_t kSessionContextAlignment = 4096;

struct alignas(kSessionContextAlignment) SessionLocalContext {
  SessionLocalContext(
      GraphSnapshotStore& snapshot_store,
      std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      std::shared_ptr<Allocator> alloc,
      std::shared_ptr<IVersionManager> version_manager, int thread_id,
      std::unique_ptr<IWalWriter> in_logger, const std::string& wal_uri,
      CheckpointCoordinator& checkpoint_coordinator,
      const NeugDBConfig& config_)
      : allocator(alloc),
        logger(std::move(in_logger)),
        session(snapshot_store, planner, global_query_cache, version_manager,
                *alloc, *logger, checkpoint_coordinator, config_, thread_id) {
    logger->open(wal_uri);
  }
  ~SessionLocalContext() {
    if (logger) {
      try {
        logger->close();
      } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to close session WAL writer: " << e.what();
      } catch (...) { LOG(WARNING) << "Failed to close session WAL writer"; }
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

static_assert(alignof(SessionLocalContext) == kSessionContextAlignment);
static_assert(sizeof(SessionLocalContext) % kSessionContextAlignment == 0);

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
 * It only prevents concurrent reuse of a pool slot; it is not a VersionManager
 * lease and does not exclude checkpoint or compaction maintenance.
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
 * **Pool Size:** Determined by `NeugDBConfig::max_thread_num`, typically
 * matching the number of concurrent request handlers.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see SessionGuard for RAII session management
 * @since v0.1.0
 */
class SessionPool {
 public:
  // checkpoint_coordinator is only forwarded to each NeugDBSession so that
  // a session can initiate a database-wide checkpoint directly; the pool
  // itself does not participate in the checkpoint protocol.
  explicit SessionPool(
      GraphSnapshotStore& snapshot_store,
      CheckpointCoordinator& checkpoint_coordinator,
      std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      std::shared_ptr<IVersionManager> version_manager,
      const std::vector<std::shared_ptr<Allocator>>& allocators,
      const std::string& wal_uri, const NeugDBConfig& config)
      : contexts_(nullptr), session_num_(config.max_thread_num) {
    WalWriterFactory::Init();
    for (size_t i = 0; i < session_num_; ++i) {
      available_sessions_.push_back(i);
    }

    contexts_ = static_cast<SessionLocalContext*>(
        aligned_alloc(kSessionContextAlignment,
                      sizeof(SessionLocalContext) * config.max_thread_num));
    if (contexts_ == nullptr) {
      throw std::bad_alloc();
    }

    size_t constructed_contexts = 0;
    try {
      for (; constructed_contexts < session_num_; ++constructed_contexts) {
        const auto thread_id = static_cast<int>(constructed_contexts);
        new (&contexts_[constructed_contexts]) SessionLocalContext(
            snapshot_store, planner, global_query_cache,
            allocators.at(constructed_contexts), version_manager, thread_id,
            WalWriterFactory::CreateWalWriter(wal_uri, thread_id), wal_uri,
            checkpoint_coordinator, config);
      }
    } catch (...) {
      while (constructed_contexts > 0) {
        contexts_[--constructed_contexts].~SessionLocalContext();
      }
      free(contexts_);
      contexts_ = nullptr;
      throw;
    }

    bthread_cond_init(&cond_, nullptr);
    bthread_mutex_init(&mutex_, nullptr);
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

  // Installs all service-owned state derived from a checkpoint: rotates every
  // session-owned WAL writer to the new generation, then invalidates the
  // global query cache through one session's local cache (every other session
  // discards its local cache lazily on the next version check). Registered
  // with CheckpointCoordinator as the activation handler at service startup;
  // invoked on the TP manual checkpoint path after active transactions have
  // been drained and the mandatory post-reopen handler has completed, while
  // new transactions remain blocked, so cross-session WAL access is
  // exclusive.
  void RotateWalAndInvalidateCaches(const std::string& wal_uri);

 private:
  void ReleaseSession(size_t session_id);

  friend class SessionGuard;
  SessionLocalContext* contexts_;
  size_t session_num_;
  std::deque<size_t> available_sessions_;
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
};

}  // namespace neug
