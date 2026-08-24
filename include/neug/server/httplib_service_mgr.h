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

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/http_service/http_svc.pb.h"
#include "neug/main/execution_slot.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/encoder.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"
#include "neug/utils/service_manager.h"
#include "neug/utils/yaml_utils.h"

#include "httplib.h"

namespace neug {

int32_t status_code_to_http_code(neug::StatusCode code);

class HttplibServiceManager : public IServiceManager {
 public:
  explicit HttplibServiceManager(neug::NeugDB& neug_db,
                                 TpExecutionSlotPool& execution_slot_pool);

  ~HttplibServiceManager();
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override;

 private:
  neug::result<std::string> GetSchemaImpl();
  neug::result<std::string> GetServiceStatusImpl();
  uint32_t resolve_num_threads() const;

  neug::NeugDB& neug_db_;
  TpExecutionSlotPool& execution_slot_pool_;
  std::shared_ptr<neug::IGraphPlanner> planner_;

  ServiceConfig service_config_;
  std::unique_ptr<httplib::Server> server_;
  std::atomic<bool> running_{false};
};

}  // namespace neug