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

#include "neug/storages/module/module_broker.h"

#include "neug/utils/exception/exception.h"

namespace neug {

void ModuleBroker::Open(Checkpoint& checkpoint, MemoryLevel level) {
  Open(checkpoint, checkpoint.GetMeta(), level);
}

void ModuleBroker::Open(Checkpoint& checkpoint, const CheckpointManifest& meta,
                        MemoryLevel level) {
  auto& factory = ModuleFactory::instance();
  for (const auto& [name, desc] : meta.modules()) {
    if (desc.is_referenced_module()) {
      continue;
    }
    if (desc.module_type.empty()) {
      // Non-factory entry (e.g. LFIndexer descriptor): the higher-level
      // orchestrator (VertexTable::OpenFrom) processes this directly.
      continue;
    }
    auto module = factory.Create(desc.module_type);
    if (!module) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "ModuleBroker::Open: unknown module_type '" + desc.module_type +
          "' for entry '" + name +
          "'.  Make sure the type is registered via NEUG_REGISTER_MODULE or "
          "NEUG_REGISTER_TEMPLATE_MODULE.");
    }
    module->Open(checkpoint, meta, desc, level);
    modules_[name] = std::move(module);
  }
}

void ModuleBroker::SetModule(const std::string& name,
                             std::unique_ptr<Module>&& module) {
  modules_[name] = std::move(module);
}

void ModuleBroker::Dump(Checkpoint& checkpoint, CheckpointManifest& meta) {
  for (auto& [name, module] : modules_) {
    if (!module) {
      continue;
    }
    module->Dump(checkpoint, meta, name);
  }
}

bool ModuleBroker::Contains(const std::string& name) const {
  return modules_.count(name) > 0;
}

std::unique_ptr<Module> ModuleBroker::TakeModule(const std::string& name) {
  auto it = modules_.find(name);
  if (it == modules_.end()) {
    return nullptr;
  }
  auto out = std::move(it->second);
  modules_.erase(it);
  return out;
}

}  // namespace neug
