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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "neug/main/transaction_context.h"
#include "neug/utils/result.h"

namespace neug {

class NeugDBService;

namespace test {
class NeugDBServiceTest;
}

/** @brief Service-private owner for explicit transaction sessions. */
class ServiceTransactionManager {
 public:
  using Clock = std::chrono::steady_clock;
  using NowFunction = std::function<Clock::time_point()>;

  ServiceTransactionManager(NeugDBService& service, size_t max_transactions,
                            uint64_t transaction_timeout_ms,
                            NowFunction now = {});
  ~ServiceTransactionManager() noexcept;

  ServiceTransactionManager(const ServiceTransactionManager&) = delete;
  ServiceTransactionManager& operator=(const ServiceTransactionManager&) =
      delete;

  result<std::string> BeginTransaction(TransactionMode mode);
  result<std::string> ExecuteRequest(const std::string& transaction_id,
                                     const std::string& request);
  Status Commit(const std::string& transaction_id);
  Status Rollback(const std::string& transaction_id);
  result<std::string> GetSchema(const std::string& transaction_id);

 private:
  enum class EntryPhase : uint8_t {
    kActive,
    kExpired,
    kCommitting,
    kTerminal,
  };

  struct SessionEntry {
    std::mutex mutex;
    TransactionContext transaction;
    Clock::time_point deadline{Clock::time_point::max()};
    std::atomic<EntryPhase> phase{EntryPhase::kActive};
  };

  struct DeadlineEntry {
    Clock::time_point deadline;
    std::string transaction_id;
    std::weak_ptr<SessionEntry> entry;

    bool operator>(const DeadlineEntry& other) const noexcept {
      return deadline > other.deadline;
    }
  };

  void FinishPendingBegin() noexcept;
  result<std::shared_ptr<SessionEntry>> Lookup(
      const std::string& transaction_id);
  void OpenAdmission();
  void CloseAdmission() noexcept;
  void CloseAndDrain() noexcept;
  void StartReaper();
  void ReaperLoop() noexcept;
  void ReapExpired();
  bool MarkExpiredIfNeeded(const std::shared_ptr<SessionEntry>& entry);
  Status ExpireLocked(const std::string& transaction_id,
                      const std::shared_ptr<SessionEntry>& entry);
  void RemoveEntry(const std::string& transaction_id,
                   const std::shared_ptr<SessionEntry>& entry);

  NeugDBService& service_;
  const size_t max_transactions_;
  const std::chrono::milliseconds transaction_timeout_;
  const bool automatic_reaper_;
  NowFunction now_;
  std::mutex registry_mutex_;
  std::condition_variable registry_cv_;
  std::unordered_map<std::string, std::shared_ptr<SessionEntry>> entries_;
  std::priority_queue<DeadlineEntry, std::vector<DeadlineEntry>,
                      std::greater<DeadlineEntry>>
      deadlines_;
  std::thread reaper_;
  std::atomic<bool> reaper_stop_{false};
  size_t pending_begins_{0};
  bool closing_{false};

  friend class NeugDBService;
  friend class test::NeugDBServiceTest;
};

}  // namespace neug
