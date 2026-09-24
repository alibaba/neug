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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "neug/server/tp_service.h"
#include "neug/utils/service_manager.h"

namespace neug {

class NeugDB;
class ExecutionSlotLease;
class ServiceTransactionManager;
class TpExecutionSlotPool;

/** Owns the protocol-neutral runtime resources for TP service mode. */
class TpServiceRuntime final : public ITpService {
 public:
  TpServiceRuntime(NeugDB& db, const ServiceConfig& config);
  ~TpServiceRuntime() override;

  TpServiceRuntime(const TpServiceRuntime&) = delete;
  TpServiceRuntime& operator=(const TpServiceRuntime&) = delete;

  result<QueryResult> ExecuteQuery(const QueryRequest& request) override;
  result<std::string> GetSchema() override;
  result<std::string> GetServiceStatus() override;
  result<BeginTransactionResult> BeginTransaction(
      TransactionMode mode) override;
  result<QueryResult> ExecuteInTransaction(
      std::string_view transaction_id, const QueryRequest& request) override;
  Status CommitTransaction(std::string_view transaction_id) override;
  Status RollbackTransaction(std::string_view transaction_id) override;

  void OpenAdmission();
  void CloseAdmission();
  void Drain();
  void StartCompaction();
  void StopCompaction();

  ExecutionSlotLease AcquireExecutionSlot();
  size_t ExecutionSlotNum() const;
  size_t getExecutedQueryNum() const;
  const ServiceConfig& config() const { return service_config_; }
  uint32_t max_thread_num() const;

 private:
  void installBthreadRuntimeWait();
  void restoreNativeRuntimeWait() noexcept;

  NeugDB& db_;
  ServiceConfig service_config_;
  std::unique_ptr<TpExecutionSlotPool> execution_slot_pool_;
  std::unique_ptr<ServiceTransactionManager> transaction_manager_;

  std::thread compact_thread_;
  std::atomic<bool> compact_thread_running_{false};
  std::mutex compact_mtx_;
  std::condition_variable compact_cv_;

  bool bthread_runtime_wait_installed_{false};
};

}  // namespace neug
