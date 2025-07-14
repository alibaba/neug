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

#ifndef ENGINES_HTTP_SERVER_HIACTOR_HTTP_HDL_MGR_H_
#define ENGINES_HTTP_SERVER_HIACTOR_HTTP_HDL_MGR_H_

#include <memory>
#include "src/engines/http_server/actor_system.h"
#include "src/engines/http_server/handler/graph_db_http_handler.h"
#include "src/storages/metadata/graph_meta_store.h"

#include "src/utils/http_handler_manager.h"
namespace server {

class HiactorHttpHandlerManager : public IHttpHandlerManager {
 public:
  HiactorHttpHandlerManager() = default;
  ~HiactorHttpHandlerManager() override;
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override {
    return query_hdl_ && query_hdl_->is_actors_running();
  }

 private:
  void set_exit_state() {
    LOG(INFO) << "set exit state";
    running_.store(false);
  }
  ServiceConfig service_config_;
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<graph_db_http_handler> query_hdl_;
  std::atomic<bool> running_{false};
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HIACTOR_HTTP_HDL_MGR_H_