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

#include "neug/server/service_transaction_manager.h"

#include <array>
#include <chrono>
#include <utility>

#include <glog/logging.h>
#include <openssl/rand.h>

#include "neug/main/execution_slot.h"
#include "neug/server/neug_db_service.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

namespace {

std::string GenerateTransactionId() {
  std::array<unsigned char, 16> bytes;
  if (RAND_bytes(bytes.data(), bytes.size()) != 1) {
    THROW_RUNTIME_ERROR("Failed to generate a transaction id");
  }

  static constexpr char kBase64Url[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string id;
  id.reserve(22);
  uint32_t accumulator = 0;
  int bits = 0;
  for (const auto byte : bytes) {
    accumulator = (accumulator << 8) | byte;
    bits += 8;
    while (bits >= 6) {
      bits -= 6;
      id.push_back(kBase64Url[(accumulator >> bits) & 0x3f]);
    }
  }
  if (bits != 0) {
    id.push_back(kBase64Url[(accumulator << (6 - bits)) & 0x3f]);
  }
  return id;
}

Status ServiceUnavailable(const std::string& message) {
  return Status(StatusCode::ERR_SERVICE_UNAVAILABLE, message);
}

Status TransactionConflict(const std::string& message) {
  return Status(StatusCode::ERR_TX_STATE_CONFLICT, message);
}

Status TransactionNotFound() {
  return Status(StatusCode::ERR_NOT_FOUND,
                "Explicit transaction was not found or is no longer active.");
}

Status TransactionTimeout() {
  return Status(StatusCode::ERR_TX_TIMEOUT,
                "Explicit transaction lifetime has expired.");
}

}  // namespace

ServiceTransactionManager::ServiceTransactionManager(
    NeugDBService& service, size_t max_transactions,
    uint64_t transaction_timeout_ms, NowFunction now)
    : service_(service),
      max_transactions_(max_transactions == 0 ? service.ExecutionSlotNum()
                                              : max_transactions),
      transaction_timeout_(transaction_timeout_ms),
      automatic_reaper_(!now),
      now_(now ? std::move(now) : [] { return Clock::now(); }) {
  StartReaper();
}

ServiceTransactionManager::~ServiceTransactionManager() noexcept {
  CloseAndDrain();
}

result<std::string> ServiceTransactionManager::BeginTransaction(
    TransactionMode mode) {
  {
    std::lock_guard lock(registry_mutex_);
    if (closing_) {
      RETURN_ERROR(
          ServiceUnavailable("Explicit transaction admission is closed."));
    }
    if (entries_.size() + pending_begins_ >= max_transactions_) {
      RETURN_ERROR(ServiceUnavailable(
          "The explicit transaction capacity has been reached."));
    }
    ++pending_begins_;
  }
  bool pending_begin = true;
  auto finish_pending_begin = [this, &pending_begin]() noexcept {
    if (pending_begin) {
      FinishPendingBegin();
      pending_begin = false;
    }
  };

  std::shared_ptr<SessionEntry> entry;
  try {
    entry = std::make_shared<SessionEntry>();
    auto slot = service_.TryAcquireExecutionSlot();
    if (!slot) {
      finish_pending_begin();
      RETURN_ERROR(ServiceUnavailable("No execution slot is available."));
    }
    if (mode == TransactionMode::kReadOnly) {
      entry->transaction.Begin(slot->BeginReadTransaction());
    } else {
      auto transaction = slot->TryBeginSnapshotCowWriteTransaction();
      if (!transaction) {
        slot = {};
        finish_pending_begin();
        RETURN_ERROR(
            ServiceUnavailable("Another storage writer is currently active."));
      }
      entry->transaction.Begin(std::move(*transaction));
    }
    if (transaction_timeout_.count() != 0) {
      entry->deadline = now_() + transaction_timeout_;
    }
  } catch (...) {
    finish_pending_begin();
    throw;
  }

  try {
    while (true) {
      auto transaction_id = GenerateTransactionId();
      std::unique_lock lock(registry_mutex_);
      if (closing_) {
        --pending_begins_;
        pending_begin = false;
        registry_cv_.notify_all();
        lock.unlock();
        entry->transaction.Rollback();
        RETURN_ERROR(
            ServiceUnavailable("Explicit transaction admission is closed."));
      }
      auto [_, inserted] = entries_.emplace(transaction_id, entry);
      if (!inserted) {
        continue;
      }
      --pending_begins_;
      pending_begin = false;
      if (entry->deadline != Clock::time_point::max()) {
        deadlines_.push(DeadlineEntry{entry->deadline, transaction_id, entry});
      }
      registry_cv_.notify_all();
      return transaction_id;
    }
  } catch (...) {
    finish_pending_begin();
    entry->transaction.Rollback();
    throw;
  }
}

result<std::string> ServiceTransactionManager::ExecuteRequest(
    const std::string& transaction_id, const std::string& request) {
  auto lookup = Lookup(transaction_id);
  if (!lookup) {
    RETURN_ERROR(lookup.error());
  }
  auto entry = std::move(lookup).value();
  std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
  if (!entry_lock.owns_lock()) {
    RETURN_ERROR(TransactionConflict(
        "Another request is already using this explicit transaction."));
  }
  if (entry->phase.load(std::memory_order_acquire) != EntryPhase::kActive) {
    if (entry->phase.load(std::memory_order_relaxed) == EntryPhase::kExpired) {
      RETURN_ERROR(ExpireLocked(transaction_id, entry));
    }
    RETURN_ERROR(TransactionNotFound());
  }
  if (MarkExpiredIfNeeded(entry)) {
    RETURN_ERROR(ExpireLocked(transaction_id, entry));
  }
  if (entry->transaction.IsRollbackOnly()) {
    RETURN_ERROR(TransactionConflict(
        "Transaction is rollback-only; rollback is required."));
  }

  auto slot = service_.TryAcquireExecutionSlot();
  if (!slot) {
    RETURN_ERROR(ServiceUnavailable("No execution slot is available."));
  }
  auto result = slot->ExecuteTransactionalRequest(request, &entry->transaction);
  slot = {};
  if (MarkExpiredIfNeeded(entry)) {
    RETURN_ERROR(ExpireLocked(transaction_id, entry));
  }
  return result;
}

Status ServiceTransactionManager::Commit(const std::string& transaction_id) {
  auto lookup = Lookup(transaction_id);
  if (!lookup) {
    return lookup.error();
  }
  auto entry = std::move(lookup).value();
  std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
  if (!entry_lock.owns_lock()) {
    return TransactionConflict(
        "Another request is already using this explicit transaction.");
  }
  if (entry->phase.load(std::memory_order_acquire) != EntryPhase::kActive) {
    if (entry->phase.load(std::memory_order_relaxed) == EntryPhase::kExpired) {
      return ExpireLocked(transaction_id, entry);
    }
    return TransactionNotFound();
  }
  if (MarkExpiredIfNeeded(entry)) {
    return ExpireLocked(transaction_id, entry);
  }
  if (entry->transaction.IsRollbackOnly()) {
    return TransactionConflict(
        "Transaction is rollback-only; rollback is required.");
  }

  auto status = entry->transaction.PrepareCommit();
  if (!status.ok()) {
    return status;
  }

  EntryPhase expected = EntryPhase::kActive;
  if (entry->deadline != Clock::time_point::max() &&
      now_() >= entry->deadline) {
    entry->phase.compare_exchange_strong(expected, EntryPhase::kExpired,
                                         std::memory_order_acq_rel);
  } else if (!entry->phase.compare_exchange_strong(expected,
                                                   EntryPhase::kCommitting,
                                                   std::memory_order_acq_rel)) {
    // The reaper won the Active -> Expired transition before the durable
    // boundary. The prepared snapshot is still safely abortable.
  }
  if (entry->phase.load(std::memory_order_acquire) == EntryPhase::kExpired) {
    return ExpireLocked(transaction_id, entry);
  }
  CHECK(entry->phase.load(std::memory_order_relaxed) ==
        EntryPhase::kCommitting);

  status = entry->transaction.CommitPrepared();
  entry->phase.store(EntryPhase::kTerminal, std::memory_order_release);
  RemoveEntry(transaction_id, entry);
  return status;
}

Status ServiceTransactionManager::Rollback(const std::string& transaction_id) {
  auto lookup = Lookup(transaction_id);
  if (!lookup) {
    return lookup.error();
  }
  auto entry = std::move(lookup).value();
  std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
  if (!entry_lock.owns_lock()) {
    return TransactionConflict(
        "Another request is already using this explicit transaction.");
  }
  if (entry->phase.load(std::memory_order_acquire) != EntryPhase::kActive) {
    if (entry->phase.load(std::memory_order_relaxed) == EntryPhase::kExpired) {
      return ExpireLocked(transaction_id, entry);
    }
    return TransactionNotFound();
  }
  if (MarkExpiredIfNeeded(entry)) {
    return ExpireLocked(transaction_id, entry);
  }

  entry->phase.store(EntryPhase::kTerminal, std::memory_order_release);
  RemoveEntry(transaction_id, entry);
  entry->transaction.Rollback();
  return Status::OK();
}

result<std::string> ServiceTransactionManager::GetSchema(
    const std::string& transaction_id) {
  auto lookup = Lookup(transaction_id);
  if (!lookup) {
    RETURN_ERROR(lookup.error());
  }
  auto entry = std::move(lookup).value();
  std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
  if (!entry_lock.owns_lock()) {
    RETURN_ERROR(TransactionConflict(
        "Another request is already using this explicit transaction."));
  }
  if (entry->phase.load(std::memory_order_acquire) != EntryPhase::kActive) {
    if (entry->phase.load(std::memory_order_relaxed) == EntryPhase::kExpired) {
      RETURN_ERROR(ExpireLocked(transaction_id, entry));
    }
    RETURN_ERROR(TransactionNotFound());
  }
  if (MarkExpiredIfNeeded(entry)) {
    RETURN_ERROR(ExpireLocked(transaction_id, entry));
  }
  if (entry->transaction.IsRollbackOnly()) {
    RETURN_ERROR(TransactionConflict(
        "Transaction is rollback-only; rollback is required."));
  }

  auto yaml = entry->transaction.schema().to_yaml();
  if (!yaml) {
    RETURN_ERROR(yaml.error());
  }
  return get_json_string_from_yaml(yaml.value());
}

void ServiceTransactionManager::OpenAdmission() {
  {
    std::lock_guard lock(registry_mutex_);
    if (!closing_) {
      return;
    }
    CHECK(entries_.empty());
    CHECK_EQ(pending_begins_, 0U);
    closing_ = false;
  }
  StartReaper();
}

void ServiceTransactionManager::CloseAdmission() noexcept {
  std::lock_guard lock(registry_mutex_);
  closing_ = true;
  registry_cv_.notify_all();
}

void ServiceTransactionManager::CloseAndDrain() noexcept {
  std::thread reaper;
  {
    std::unique_lock lock(registry_mutex_);
    closing_ = true;
    registry_cv_.notify_all();
    registry_cv_.wait(lock, [this] { return pending_begins_ == 0; });
    reaper = std::move(reaper_);
  }
  if (reaper.joinable()) {
    reaper_stop_.store(true, std::memory_order_release);
    registry_cv_.notify_all();
    reaper.join();
  }

  decltype(entries_) entries;
  {
    std::lock_guard lock(registry_mutex_);
    entries.swap(entries_);
    deadlines_ = {};
  }

  for (auto& [_, entry] : entries) {
    std::lock_guard entry_lock(entry->mutex);
    entry->phase.store(EntryPhase::kTerminal, std::memory_order_release);
    entry->transaction.Rollback();
  }
}

void ServiceTransactionManager::StartReaper() {
  if (!automatic_reaper_ || transaction_timeout_.count() == 0 ||
      reaper_.joinable()) {
    return;
  }
  reaper_stop_.store(false, std::memory_order_release);
  reaper_ = std::thread([this] { ReaperLoop(); });
}

void ServiceTransactionManager::ReaperLoop() noexcept {
  while (!reaper_stop_.load(std::memory_order_acquire)) {
    std::unique_lock lock(registry_mutex_);
    if (closing_) {
      return;
    }
    if (deadlines_.empty()) {
      registry_cv_.wait(lock, [this] {
        return closing_ || reaper_stop_.load(std::memory_order_acquire) ||
               !deadlines_.empty();
      });
      continue;
    }
    const auto wait = deadlines_.top().deadline - now_();
    if (wait > Clock::duration::zero()) {
      registry_cv_.wait_for(lock, wait);
      continue;
    }
    lock.unlock();
    ReapExpired();
  }
}

void ServiceTransactionManager::ReapExpired() {
  std::vector<DeadlineEntry> expired;
  {
    std::lock_guard lock(registry_mutex_);
    const auto now = now_();
    while (!deadlines_.empty() && deadlines_.top().deadline <= now) {
      expired.emplace_back(deadlines_.top());
      deadlines_.pop();
    }
  }

  for (auto& deadline : expired) {
    auto entry = deadline.entry.lock();
    if (!entry) {
      continue;
    }
    EntryPhase expected = EntryPhase::kActive;
    if (!entry->phase.compare_exchange_strong(expected, EntryPhase::kExpired,
                                              std::memory_order_acq_rel)) {
      continue;
    }
    std::unique_lock entry_lock(entry->mutex, std::try_to_lock);
    if (!entry_lock.owns_lock()) {
      continue;
    }
    (void) ExpireLocked(deadline.transaction_id, entry);
  }
}

bool ServiceTransactionManager::MarkExpiredIfNeeded(
    const std::shared_ptr<SessionEntry>& entry) {
  auto phase = entry->phase.load(std::memory_order_acquire);
  if (phase == EntryPhase::kExpired) {
    return true;
  }
  if (phase != EntryPhase::kActive ||
      entry->deadline == Clock::time_point::max() || now_() < entry->deadline) {
    return false;
  }
  return entry->phase.compare_exchange_strong(phase, EntryPhase::kExpired,
                                              std::memory_order_acq_rel) ||
         phase == EntryPhase::kExpired;
}

Status ServiceTransactionManager::ExpireLocked(
    const std::string& transaction_id,
    const std::shared_ptr<SessionEntry>& entry) {
  entry->phase.store(EntryPhase::kTerminal, std::memory_order_release);
  RemoveEntry(transaction_id, entry);
  entry->transaction.Rollback();
  return TransactionTimeout();
}

void ServiceTransactionManager::RemoveEntry(
    const std::string& transaction_id,
    const std::shared_ptr<SessionEntry>& entry) {
  std::lock_guard lock(registry_mutex_);
  auto it = entries_.find(transaction_id);
  if (it != entries_.end() && it->second == entry) {
    entries_.erase(it);
  }
}

void ServiceTransactionManager::FinishPendingBegin() noexcept {
  std::lock_guard lock(registry_mutex_);
  CHECK_GT(pending_begins_, 0U);
  --pending_begins_;
  registry_cv_.notify_all();
}

result<std::shared_ptr<ServiceTransactionManager::SessionEntry>>
ServiceTransactionManager::Lookup(const std::string& transaction_id) {
  std::lock_guard lock(registry_mutex_);
  if (closing_) {
    RETURN_ERROR(
        ServiceUnavailable("Explicit transaction admission is closed."));
  }
  auto it = entries_.find(transaction_id);
  if (it == entries_.end()) {
    RETURN_ERROR(TransactionNotFound());
  }
  return it->second;
}

}  // namespace neug
