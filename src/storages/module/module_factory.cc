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

#include "neug/storages/module/module_factory.h"

namespace neug {

ModuleFactory& ModuleFactory::instance() {
  static ModuleFactory factory;
  return factory;
}

bool ModuleFactory::Register(const std::string& module_type, Creator creator) {
  creators_[module_type] = std::move(creator);
  return true;
}

std::unique_ptr<Module> ModuleFactory::Create(
    const std::string& module_type) const {
  auto it = creators_.find(module_type);
  if (it == creators_.end()) {
    return nullptr;
  }
  return it->second();
}

}  // namespace neug
