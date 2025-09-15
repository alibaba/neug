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
#ifndef ENGINES_COMMON_GRAPH_DB_SERVICE_H_
#define ENGINES_COMMON_GRAPH_DB_SERVICE_H_

#include <cctype>
#include <memory>
#include <string>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/main/neug_db.h"
#include "neug/utils/http_handler_manager.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

#include <yaml-cpp/yaml.h>

namespace server {
/* Stored service configuration, read from interactive_config.yaml
 */

class NeugDBService {
 public:
  NeugDBService(gs::NeugDB& db) : db_(db) {}
  gs::NeugDB& graph_db() { return db_; }
  ~NeugDBService();

  void init(const ServiceConfig& config);

  std::string Start();

  void Stop();

  const ServiceConfig& GetServiceConfig() const;

  bool IsInitialized() const;

  bool IsRunning() const;

  gs::Result<std::string> service_status();

  void run_and_wait_for_exit();

 private:
  NeugDBService() = delete;

 private:
  gs::NeugDB& db_;
  std::unique_ptr<IHttpHandlerManager> hdl_mgr_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  std::mutex mtx_;

  ServiceConfig service_config_;
};

}  // namespace server

#endif  // ENGINES_COMMON_GRAPH_DB_SERVICE_H_
