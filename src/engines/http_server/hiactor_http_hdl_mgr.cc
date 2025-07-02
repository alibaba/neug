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

#include "src/engines/http_server/hiactor_http_hdl_mgr.h"
#include <seastar/core/alien.hh>
#include "src/utils/service_utils.h"

namespace server {

HiactorHttpHandlerManager::~HiactorHttpHandlerManager() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
  if (metadata_store_) {
    metadata_store_->Close();
  }
}

void HiactorHttpHandlerManager::Init(const ServiceConfig& config) {
  service_config_ = config;
  actor_sys_ = std::make_unique<actor_system>(
      config.shard_num, config.dpdk_mode, config.enable_thread_resource_pool,
      config.external_thread_num, [this]() { set_exit_state(); });
  // NOTE that in sharding mode EXCLUSIVE, the last shard is reserved for admin
  //  requests.
  query_hdl_ = std::make_unique<graph_db_http_handler>(
      config.query_port, config.shard_num, config.get_cooperative_shard_num(),
      config.enable_adhoc_handler);
}
void HiactorHttpHandlerManager::Start() {
  // Start logic
  if (query_hdl_) {
    query_hdl_->start_query_actors();
  }
}

void HiactorHttpHandlerManager::Stop() {
  // Stop logic
  if (query_hdl_) {
    auto fut = seastar::alien::submit_to(
        *seastar::alien::internal::default_instance, 0,
        [this] { return query_hdl_->stop_query_actors(); });
    fut.wait();
  }
  return;
}

void HiactorHttpHandlerManager::RunAndWaitForExit() {
  // Run and wait for exit logic
  // This might involve starting the actor system and waiting for it to finish
  actor_sys_->launch();
  query_hdl_->start();
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  query_hdl_->stop();
  actor_sys_->terminate();
}
}  // namespace server
