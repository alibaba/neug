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
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "neug/main/transaction_context.h"
#include "neug/utils/result.h"

namespace neug {

class TpExecutionSlotPool;

/** Owns service-local explicit transactions without retaining execution slots.
 */
class ServiceTransactionManager {
 public:
  struct BeginResult {
    std::string transaction_id;
    // Advisory expiry time derived from the system clock for client display;
    // the authoritative deadline is tracked with the steady clock, so this
    // value may drift under system clock adjustments. Nullopt when session
    // expiry is disabled.
    std::optional<std::chrono::system_clock::time_point> expires_at;
  };

  ServiceTransactionManager(TpExecutionSlotPool& execution_slot_pool,
                            size_t max_transactions, uint64_t timeout_ms);
  ~ServiceTransactionManager();

  ServiceTransactionManager(const ServiceTransactionManager&) = delete;
  ServiceTransactionManager& operator=(const ServiceTransactionManager&) =
      delete;

  result<BeginResult> Begin(TransactionMode mode);
  result<std::string> Execute(std::string_view transaction_id,
                              const std::string& request);
  Status Commit(std::string_view transaction_id);
  Status Rollback(std::string_view transaction_id);

  void Close();
  void CloseAdmission();
  void Open();

 private:
  struct Entry {
    Entry(TransactionContext transaction_context,
          std::chrono::steady_clock::time_point transaction_deadline)
        : context(std::move(transaction_context)),
          deadline(transaction_deadline) {}

    std::mutex mutex;
    TransactionContext context;
    const std::chrono::steady_clock::time_point deadline;
  };

  using EntryPtr = std::shared_ptr<Entry>;

  struct LockedEntry {
    EntryPtr entry;
    std::unique_lock<std::mutex> lock;
  };

  struct TransparentStringHash {
    using is_transparent = void;

    size_t operator()(std::string_view value) const noexcept {
      return std::hash<std::string_view>{}(value);
    }
    size_t operator()(const std::string& value) const noexcept {
      return operator()(std::string_view(value));
    }
  };

  result<LockedEntry> LockEntry(std::string_view transaction_id);
  EntryPtr Find(std::string_view transaction_id) const;
  void Remove(std::string_view transaction_id, const EntryPtr& entry);
  bool Expired(const Entry& entry) const noexcept;
  bool ReapExpiredTransactions();
  void ReaperMain();

  TpExecutionSlotPool& execution_slot_pool_;
  const size_t max_transactions_;
  const std::chrono::milliseconds timeout_;

  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::unordered_map<std::string, EntryPtr, TransparentStringHash,
                     std::equal_to<>>
      entries_;
  size_t pending_begins_{0};
  bool accepting_{true};
  bool stop_reaper_{false};
  std::thread reaper_;
};

}  // namespace neug
