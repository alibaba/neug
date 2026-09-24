/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "tp_service_runtime.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include <bthread/bthread.h>
#include <glog/logging.h>

#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/neug_db.h"
#include "neug/server/bthread_runtime_wait.h"
#include "neug/server/tp_execution_slot_pool.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/yaml_utils.h"
#include "service_transaction_manager.h"

namespace neug {

namespace {
constexpr auto kCompactInterval = std::chrono::seconds(30);
constexpr size_t kCompactQueryThreshold = 100000;
}  // namespace

TpServiceRuntime::TpServiceRuntime(NeugDB& db, const ServiceConfig& config)
    : db_(db), service_config_(config) {
  if (db_.IsClosed()) {
    THROW_RUNTIME_ERROR("NeugDB instance is not ready for serving!");
  }
  if (service_config_.thread_num > 0 &&
      service_config_.thread_num >
          static_cast<uint32_t>(db_.config().max_thread_num)) {
    LOG(WARNING) << "Service thread_num (" << service_config_.thread_num
                 << ") exceeds database max_thread_num ("
                 << db_.config().max_thread_num << "); clamping to "
                 << db_.config().max_thread_num << ".";
    service_config_.thread_num =
        static_cast<uint32_t>(db_.config().max_thread_num);
  }

  installBthreadRuntimeWait();
  try {
    bthread_setconcurrency(
        std::max(db_.config().max_thread_num, BTHREAD_MIN_CONCURRENCY));
    execution_slot_pool_ = std::make_unique<TpExecutionSlotPool>(
        db_.graph_snapshot_store(), db_.GetPlanner(), db_.GetQueryCache(),
        *db_.version_manager_, *db_.checkpoint_coordinator_,
        db_.extension_manager(), db_.allocators_, *db_.wal_writers_,
        db_.config());
    transaction_manager_ = std::make_unique<ServiceTransactionManager>(
        *execution_slot_pool_, service_config_.max_explicit_transactions,
        service_config_.explicit_transaction_timeout_ms);
  } catch (...) {
    transaction_manager_.reset();
    execution_slot_pool_.reset();
    restoreNativeRuntimeWait();
    throw;
  }
}

TpServiceRuntime::~TpServiceRuntime() {
  CloseAdmission();
  Drain();
  // Compaction may be waiting for a write lease owned by an explicit session.
  // Drain sessions before joining the compaction thread.
  StopCompaction();
  transaction_manager_.reset();
  execution_slot_pool_.reset();
  restoreNativeRuntimeWait();
}

void TpServiceRuntime::installBthreadRuntimeWait() {
  CHECK(!bthread_runtime_wait_installed_);
  if (!db_.version_manager_->try_set_runtime_wait_if_quiescent(
          &BthreadRuntimeWait)) {
    THROW_RUNTIME_ERROR(
        "Cannot install bthread runtime wait while transactions are active.");
  }
  bthread_runtime_wait_installed_ = true;
}

void TpServiceRuntime::restoreNativeRuntimeWait() noexcept {
  if (!bthread_runtime_wait_installed_) {
    return;
  }
  CHECK(db_.version_manager_->try_set_runtime_wait_if_quiescent(
      &NativeRuntimeWait))
      << "All service transactions must be quiescent before restoring native "
         "runtime wait";
  bthread_runtime_wait_installed_ = false;
}

result<QueryResult> TpServiceRuntime::ExecuteQuery(
    const QueryRequest& request) {
  auto slot = execution_slot_pool_->AcquireExecutionSlot();
  return slot->ExecuteTransactionalQuery(request);
}

result<std::string> TpServiceRuntime::GetSchema() {
  auto slot = execution_slot_pool_->AcquireExecutionSlot();
  auto read_txn = slot->BeginSnapshotReadTransaction();
  auto yaml = read_txn.schema().to_yaml();
  if (!yaml) {
    read_txn.Abort();
    RETURN_ERROR(yaml.error());
  }
  auto json = get_json_string_from_yaml(yaml.value());
  if (!json) {
    read_txn.Abort();
    RETURN_ERROR(json.error());
  }
  read_txn.Commit();
  return json;
}

result<std::string> TpServiceRuntime::GetServiceStatus() {
  return std::string("{\"status\": \"OK\", \"version\": \"" NEUG_VERSION "\"}");
}

result<BeginTransactionResult> TpServiceRuntime::BeginTransaction(
    TransactionMode mode) {
  auto transaction = transaction_manager_->Begin(mode);
  if (!transaction) {
    RETURN_ERROR(transaction.error());
  }
  auto begin = std::move(transaction).value();
  return BeginTransactionResult{std::move(begin.transaction_id),
                                std::move(begin.expires_at)};
}

result<QueryResult> TpServiceRuntime::ExecuteInTransaction(
    std::string_view transaction_id, const QueryRequest& request) {
  return transaction_manager_->Execute(transaction_id, request);
}

Status TpServiceRuntime::CommitTransaction(std::string_view transaction_id) {
  return transaction_manager_->Commit(transaction_id);
}

Status TpServiceRuntime::RollbackTransaction(std::string_view transaction_id) {
  return transaction_manager_->Rollback(transaction_id);
}

void TpServiceRuntime::OpenAdmission() { transaction_manager_->Open(); }

void TpServiceRuntime::CloseAdmission() {
  transaction_manager_->CloseAdmission();
}

void TpServiceRuntime::Drain() { transaction_manager_->Close(); }

ExecutionSlotLease TpServiceRuntime::AcquireExecutionSlot() {
  return execution_slot_pool_->AcquireExecutionSlot();
}

size_t TpServiceRuntime::ExecutionSlotNum() const {
  return execution_slot_pool_->ExecutionSlotNum();
}

size_t TpServiceRuntime::getExecutedQueryNum() const {
  return execution_slot_pool_->getExecutedQueryNum();
}

uint32_t TpServiceRuntime::max_thread_num() const {
  return static_cast<uint32_t>(db_.config().max_thread_num);
}

void TpServiceRuntime::StopCompaction() {
  compact_thread_running_.store(false, std::memory_order_relaxed);
  compact_cv_.notify_all();
  if (compact_thread_.joinable()) {
    compact_thread_.join();
  }
}

void TpServiceRuntime::StartCompaction() {
  if (!service_config_.auto_compaction) {
    return;
  }
  StopCompaction();
  compact_thread_running_.store(true, std::memory_order_relaxed);
  try {
    compact_thread_ = std::thread([this]() {
      size_t last_compaction_at = 0;
      while (compact_thread_running_.load(std::memory_order_relaxed)) {
        const size_t query_num_before = getExecutedQueryNum();
        {
          std::unique_lock<std::mutex> lock(compact_mtx_);
          if (compact_cv_.wait_for(lock, kCompactInterval, [this] {
                return !compact_thread_running_.load(std::memory_order_relaxed);
              })) {
            break;
          }
        }
        if (!compact_thread_running_.load(std::memory_order_relaxed)) {
          break;
        }
        try {
          const size_t query_num_after = getExecutedQueryNum();
          if (query_num_before == query_num_after &&
              query_num_after > last_compaction_at + kCompactQueryThreshold) {
            VLOG(10) << "Trigger auto compaction";
            last_compaction_at = query_num_after;
            auto slot = AcquireExecutionSlot();
            auto transaction = slot->BeginInPlaceCompactionTransaction();
            transaction.Commit();
            VLOG(10) << "Finish compaction";
          }
        } catch (const std::exception& e) {
          LOG(WARNING) << "Auto compaction failed: " << e.what();
        } catch (...) {
          LOG(WARNING) << "Auto compaction failed with unknown error";
        }
      }
    });
  } catch (...) {
    compact_thread_running_.store(false, std::memory_order_relaxed);
    throw;
  }
}

}  // namespace neug
