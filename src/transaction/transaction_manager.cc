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

#include "neug/transaction/transaction_manager.h"

#include <glog/logging.h>
#include <stdlib.h>
#include <filesystem>
#include <new>
#include <ostream>

#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace gs {

class AppManager;
class PropertyGraph;

TransactionManager::TransactionManager(
    std::shared_ptr<IVersionManager> version_manager, PropertyGraph& graph,
    std::vector<std::shared_ptr<Allocator>>& allocators,
    const NeugDBConfig& config, const std::string& work_dir, int32_t thread_num)
    : thread_num_(thread_num),
      work_dir_(work_dir),
      version_manager_(version_manager),
      graph_(graph),
      config_(std::move(config)) {
  allocator_strategy_ = MemoryStrategy::kMemoryOnly;
  if (config_.memory_level == 0) {
    allocator_strategy_ = MemoryStrategy::kSyncToFile;
  } else if (config_.memory_level >= 2) {
    allocator_strategy_ = MemoryStrategy::kHugepagePrefered;
  }
  wal_uri_ = parse_wal_uri(config_.wal_uri, work_dir_);
  WalWriterFactory::Init();
  WalParserFactory::Init();
  // By default, we only allocate one context for embeded mode.
  contexts_ = static_cast<SessionLocalContext*>(
      aligned_alloc(4096, sizeof(SessionLocalContext) * thread_num_));

  if (!version_manager_) {
    THROW_INTERNAL_EXCEPTION("Version manager is null");
  }
  for (int i = 0; i < thread_num_; ++i) {
    new (&contexts_[i]) SessionLocalContext(
        graph_, allocators[i], version_manager_, work_dir, i,
        WalWriterFactory::CreateWalWriter(wal_uri_, i), config_);
  }
}

void TransactionManager::Clear() {
  if (contexts_ != nullptr) {
    for (int i = 0; i < thread_num_; ++i) {
      contexts_[i].~SessionLocalContext();
    }
    free(contexts_);
    contexts_ = nullptr;
  }
}
size_t TransactionManager::getExecutedQueryNum() const {
  size_t ret = 0;
  for (int i = 0; i < thread_num_; ++i) {
    ret += contexts_[i].session.query_num();
  }
  return ret;
}

size_t TransactionManager::GetEvalDuration(int thread_id) const {
  if (thread_id < 0 || thread_id >= thread_num_) {
    return 0;
  }
  return contexts_[thread_id].session.eval_duration();
}

size_t TransactionManager::GetQueryNum(int thread_id) const {
  if (thread_id < 0 || thread_id >= thread_num_) {
    return 0;
  }
  return contexts_[thread_id].session.query_num();
}

ReadTransaction TransactionManager::GetReadTransaction(int thread_id) const {
  uint32_t ts = version_manager_->acquire_read_timestamp();
  return ReadTransaction(graph_, *version_manager_, ts);
}

InsertTransaction TransactionManager::GetInsertTransaction(int thread_id) {
  uint32_t ts = version_manager_->acquire_insert_timestamp();
  return InsertTransaction(graph_, *contexts_[thread_id].allocator,
                           *(contexts_[thread_id].logger), *version_manager_,
                           ts);
}

UpdateTransaction TransactionManager::GetUpdateTransaction(int thread_id) {
  uint32_t ts = version_manager_->acquire_update_timestamp();
  return UpdateTransaction(graph_, *contexts_[thread_id].allocator,
                           *(contexts_[thread_id].logger), *version_manager_,
                           ts);
}

CompactTransaction TransactionManager::GetCompactTransaction(int thread_id) {
  uint32_t ts = version_manager_->acquire_update_timestamp();
  return CompactTransaction(graph_, *(contexts_[thread_id].logger),
                            *version_manager_, config_.enable_auto_compaction,
                            config_.csr_reserve_ratio, ts);
}

server::NeugDBSession& TransactionManager::GetSession(int thread_id) {
  if (thread_id < 0 || thread_id >= thread_num_) {
    THROW_INTERNAL_EXCEPTION(
        "Thread id is out of range: " + std::to_string(thread_id) +
        " >= " + std::to_string(thread_num_));
  }
  return contexts_[thread_id].session;
}

const server::NeugDBSession& TransactionManager::GetSession(
    int thread_id) const {
  if (thread_id < 0 || thread_id >= thread_num_) {
    THROW_INTERNAL_EXCEPTION(
        "Thread id is out of range: " + std::to_string(thread_id) +
        " >= " + std::to_string(thread_num_));
  }
  return contexts_[thread_id].session;
}

}  // namespace gs
