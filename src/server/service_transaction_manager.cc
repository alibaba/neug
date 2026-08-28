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
#include "service_transaction_manager.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "neug/main/query_request.h"
#include "neug/server/tp_execution_slot_pool.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

namespace {

Status ServiceUnavailable(const std::string& message) {
  return Status(StatusCode::ERR_SERVICE_UNAVAILABLE, message);
}

Status TransactionNotFound() {
  return Status(StatusCode::ERR_NOT_FOUND, "Transaction does not exist.");
}

Status TransactionExpired() {
  return Status(StatusCode::ERR_TX_TIMEOUT, "Transaction has expired.");
}

Status TransactionBusy() {
  return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                "Another request is already using this transaction.");
}

std::string GenerateTransactionId() {
  static constexpr char kHexDigits[] = "0123456789abcdef";
  thread_local std::random_device random_device;

  std::string transaction_id(32, '0');
  for (size_t word_index = 0; word_index < 4; ++word_index) {
    auto word = static_cast<uint32_t>(random_device());
    const auto offset = word_index * 8;
    for (size_t digit_index = 0; digit_index < 8; ++digit_index) {
      transaction_id[offset + 7 - digit_index] = kHexDigits[word & 0x0F];
      word >>= 4;
    }
  }
  return transaction_id;
}

}  // namespace

ServiceTransactionManager::ServiceTransactionManager(
    TpExecutionSlotPool& execution_slot_pool, size_t max_transactions,
    uint64_t timeout_ms)
    : execution_slot_pool_(execution_slot_pool),
      max_transactions_(max_transactions == 0
                            ? execution_slot_pool.ExecutionSlotNum()
                            : max_transactions),
      timeout_(timeout_ms) {
  reaper_ = std::thread([this] { ReaperMain(); });
}

ServiceTransactionManager::~ServiceTransactionManager() {
  Close();
  {
    std::lock_guard lock(mutex_);
    stop_reaper_ = true;
  }
  changed_.notify_all();
  if (reaper_.joinable()) {
    reaper_.join();
  }
}

result<ServiceTransactionManager::BeginResult> ServiceTransactionManager::Begin(
    TransactionMode mode) {
  {
    std::lock_guard lock(mutex_);
    if (!accepting_) {
      RETURN_ERROR(ServiceUnavailable("Transaction service is stopping."));
    }
    if (entries_.size() + pending_begins_ >= max_transactions_) {
      RETURN_ERROR(ServiceUnavailable("Too many active transactions."));
    }
    ++pending_begins_;
  }

  const auto finish_pending_begin = [this] {
    {
      std::lock_guard lock(mutex_);
      CHECK_GT(pending_begins_, 0U);
      --pending_begins_;
    }
    changed_.notify_all();
  };

  try {
    TransactionContext context;
    {
      auto slot = execution_slot_pool_.TryAcquireExecutionSlot();
      if (!slot) {
        finish_pending_begin();
        RETURN_ERROR(ServiceUnavailable("No TP execution slot is available."));
      }
      if (mode == TransactionMode::kReadOnly) {
        context.Begin(slot->BeginSnapshotReadTransaction());
      } else {
        auto transaction = slot->TryBeginSnapshotCowWriteTransaction();
        if (!transaction) {
          finish_pending_begin();
          RETURN_ERROR(transaction.error());
        }
        context.Begin(std::move(transaction).value());
      }
    }

    auto deadline = std::chrono::steady_clock::time_point::max();
    std::optional<std::chrono::system_clock::time_point> expires_at;
    if (timeout_.count() != 0) {
      deadline = std::chrono::steady_clock::now() + timeout_;
      expires_at = std::chrono::system_clock::now() + timeout_;
    }
    auto entry = std::make_shared<Entry>(std::move(context), deadline);
    std::string transaction_id;
    bool accepted = false;
    while (!accepted) {
      transaction_id = GenerateTransactionId();
      std::lock_guard lock(mutex_);
      if (!accepting_) {
        CHECK_GT(pending_begins_, 0U);
        --pending_begins_;
        break;
      }
      if (!entries_.contains(transaction_id)) {
        entries_.emplace(transaction_id, entry);
        CHECK_GT(pending_begins_, 0U);
        --pending_begins_;
        accepted = true;
      }
    }
    changed_.notify_all();
    if (!accepted) {
      entry->context.Rollback();
      RETURN_ERROR(ServiceUnavailable("Transaction service is stopping."));
    }
    return BeginResult{std::move(transaction_id), expires_at};
  } catch (const std::exception& e) {
    finish_pending_begin();
    RETURN_ERROR(Status::RuntimeError(e.what()));
  } catch (...) {
    finish_pending_begin();
    RETURN_ERROR(Status::InternalError("Failed to begin transaction."));
  }
}

result<std::string> ServiceTransactionManager::Execute(
    std::string_view transaction_id, const std::string& request) {
  std::string query;
  AccessMode mode = AccessMode::kUnKnown;
  rapidjson::Document parameters;
  try {
    RETURN_STATUS_ERROR_IF_NOT_OK(
        RequestParser::ParseFromString(request, query, mode, parameters));
  } catch (const std::exception& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT, e.what()));
  }

  auto locked_result = LockEntry(transaction_id);
  if (!locked_result) {
    RETURN_ERROR(locked_result.error());
  }
  auto locked = std::move(locked_result).value();
  auto& entry = locked.entry;
  if (!entry->context.IsActive()) {
    RETURN_ERROR(Status(StatusCode::ERR_TX_STATE_CONFLICT,
                        "Transaction must be rolled back before reuse."));
  }

  result<std::string> response = [&]() -> result<std::string> {
    try {
      auto slot = execution_slot_pool_.TryAcquireExecutionSlot();
      if (!slot) {
        RETURN_ERROR(ServiceUnavailable("No TP execution slot is available."));
      }
      auto query_result = slot->ExecuteQueryInTransaction(
          query, mode, parameters, /*num_threads=*/0, entry->context);
      if (!query_result) {
        RETURN_ERROR(query_result.error());
      }
      try {
        return query_result.value().Serialize();
      } catch (const std::exception& e) {
        entry->context.AbortAndMarkRollbackOnly();
        RETURN_ERROR(Status::RuntimeError(e.what()));
      }
    } catch (const std::exception& e) {
      RETURN_ERROR(Status::RuntimeError(e.what()));
    }
  }();
  if (!response) {
    RETURN_ERROR(response.error());
  }
  if (Expired(*entry)) {
    entry->context.Rollback();
    locked.lock.unlock();
    Remove(transaction_id, entry);
    RETURN_ERROR(TransactionExpired());
  }
  return response;
}

Status ServiceTransactionManager::Commit(std::string_view transaction_id) {
  auto locked_result = LockEntry(transaction_id);
  if (!locked_result) {
    return locked_result.error();
  }
  auto locked = std::move(locked_result).value();
  auto& entry = locked.entry;
  if (!entry->context.IsActive()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Transaction must be rolled back before commit.");
  }

  Status status;
  if (entry->context.IsReadOnly()) {
    status = entry->context.Commit();
  } else {
    status = entry->context.PrepareTpSnapshotCommit();
    if (status.ok() && Expired(*entry)) {
      entry->context.Rollback();
      status = TransactionExpired();
    }
    if (status.ok()) {
      status = entry->context.CommitPreparedTpSnapshot();
    }
  }
  if (!status.ok() && entry->context.IsActive()) {
    entry->context.Rollback();
  }
  locked.lock.unlock();
  Remove(transaction_id, entry);
  return status;
}

Status ServiceTransactionManager::Rollback(std::string_view transaction_id) {
  auto locked_result = LockEntry(transaction_id);
  if (!locked_result) {
    return locked_result.error();
  }
  auto locked = std::move(locked_result).value();
  auto& entry = locked.entry;
  entry->context.Rollback();
  locked.lock.unlock();
  Remove(transaction_id, entry);
  return Status::OK();
}

void ServiceTransactionManager::Close() {
  decltype(entries_) entries;
  {
    std::unique_lock lock(mutex_);
    accepting_ = false;
    changed_.wait(lock, [this] { return pending_begins_ == 0; });
    entries.swap(entries_);
  }
  for (const auto& [_, entry] : entries) {
    std::lock_guard lock(entry->mutex);
    entry->context.Rollback();
  }
}

void ServiceTransactionManager::CloseAdmission() {
  std::lock_guard lock(mutex_);
  accepting_ = false;
}

void ServiceTransactionManager::Open() {
  std::lock_guard lock(mutex_);
  accepting_ = true;
}

result<ServiceTransactionManager::LockedEntry>
ServiceTransactionManager::LockEntry(std::string_view transaction_id) {
  auto entry = Find(transaction_id);
  if (!entry) {
    RETURN_ERROR(TransactionNotFound());
  }
  std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
  if (!entry_lock.owns_lock()) {
    RETURN_ERROR(TransactionBusy());
  }
  if (Expired(*entry)) {
    entry->context.Rollback();
    entry_lock.unlock();
    Remove(transaction_id, entry);
    RETURN_ERROR(TransactionExpired());
  }
  return LockedEntry{std::move(entry), std::move(entry_lock)};
}

ServiceTransactionManager::EntryPtr ServiceTransactionManager::Find(
    std::string_view transaction_id) const {
  std::lock_guard lock(mutex_);
  const auto found = entries_.find(transaction_id);
  return found == entries_.end() ? nullptr : found->second;
}

void ServiceTransactionManager::Remove(std::string_view transaction_id,
                                       const EntryPtr& entry) {
  {
    std::lock_guard lock(mutex_);
    const auto found = entries_.find(transaction_id);
    if (found != entries_.end() && found->second == entry) {
      entries_.erase(found);
    }
  }
  changed_.notify_all();
}

bool ServiceTransactionManager::Expired(const Entry& entry) const noexcept {
  return std::chrono::steady_clock::now() >= entry.deadline;
}

bool ServiceTransactionManager::ReapExpiredTransactions() {
  std::vector<std::pair<std::string, EntryPtr>> candidates;
  {
    std::lock_guard lock(mutex_);
    candidates.reserve(entries_.size());
    for (const auto& [transaction_id, entry] : entries_) {
      if (Expired(*entry)) {
        candidates.emplace_back(transaction_id, entry);
      }
    }
  }
  bool busy_expired = false;
  for (const auto& [transaction_id, entry] : candidates) {
    std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
    if (!entry_lock.owns_lock()) {
      busy_expired = true;
      continue;
    }
    entry->context.Rollback();
    entry_lock.unlock();
    Remove(transaction_id, entry);
  }
  return busy_expired;
}

void ServiceTransactionManager::ReaperMain() {
  std::unique_lock lock(mutex_);
  while (!stop_reaper_) {
    auto next_deadline = std::chrono::steady_clock::time_point::max();
    for (const auto& [_, entry] : entries_) {
      next_deadline = std::min(next_deadline, entry->deadline);
    }
    if (next_deadline == std::chrono::steady_clock::time_point::max()) {
      changed_.wait(lock);
    } else {
      changed_.wait_until(lock, next_deadline);
    }
    if (stop_reaper_) {
      break;
    }
    lock.unlock();
    const bool busy_expired = ReapExpiredTransactions();
    lock.lock();
    if (busy_expired && !stop_reaper_) {
      // Retry without blocking cleanup behind one long-running request.
      changed_.wait_for(lock, std::chrono::milliseconds(1));
    }
  }
}

}  // namespace neug
