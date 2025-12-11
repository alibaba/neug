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

#include <stdint.h>
#include <algorithm>
#include <array>
#include <memory>
#include <string>

#include "neug/main/app/app_base.h"

namespace gs {

class NeugDB;
class AppManager {
 public:
  explicit AppManager(NeugDB& db) : db_(db) {
    app_paths_.fill("");
    app_factories_.fill(nullptr);
  }
  ~AppManager() {
    std::fill(app_paths_.begin(), app_paths_.end(), "");
    std::fill(app_factories_.begin(), app_factories_.end(), nullptr);
  }

  AppWrapper CreateApp(uint8_t app_type, int thread_id);

  void initApps();

  bool registerApp(const std::string& plugin_path, uint8_t index);

 private:
  NeugDB& db_;
  std::array<std::string, 256> app_paths_;
  std::array<std::shared_ptr<AppFactoryBase>, 256> app_factories_;
};

}  // namespace gs
