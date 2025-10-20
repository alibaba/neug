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

#include "neug/main/app_manager.h"

#include <glog/logging.h>
#include <stddef.h>
#include <ostream>

#include "neug/main/app/cypher_read_app.h"
#include "neug/main/app/cypher_update_app.h"
#include "neug/storages/graph/schema.h"

namespace gs {

void AppManager::initApps() {  // Currently we only have builtin apps
  for (size_t i = 0; i < 256; ++i) {
    app_factories_[i] = nullptr;
  }
  app_factories_[Schema::ADHOC_READ_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();
  app_factories_[Schema::ADHOC_UPDATE_PLUGIN_ID] =
      std::make_shared<CypherUpdateAppFactory>();
  app_factories_[Schema::CYPHER_READ_DEBUG_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();
  app_factories_[Schema::CYPHER_READ_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();
}

AppWrapper AppManager::CreateApp(uint8_t app_type, int thread_id) {
  if (app_factories_[app_type] == nullptr) {
    LOG(ERROR) << "Stored procedure " << static_cast<int>(app_type)
               << " is not registered.";
    return AppWrapper(NULL, NULL);
  } else {
    return app_factories_[app_type]->CreateApp(db_);
  }
}

bool AppManager::registerApp(const std::string& plugin_path, uint8_t index) {
  // this function will only be called when initializing the graph db
  VLOG(10) << "Registering stored procedure at:" << std::to_string(index)
           << ", path:" << plugin_path;
  if (!app_factories_[index] && app_paths_[index].empty()) {
    app_paths_[index] = plugin_path;
    app_factories_[index] =
        std::make_shared<SharedLibraryAppFactory>(plugin_path);
    return true;
  } else {
    LOG(ERROR) << "Stored procedure has already been registered at:"
               << std::to_string(index) << ", path:" << app_paths_[index];
    return false;
  }
}

}  // namespace gs
