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

#include "neug/storages/module/module_store.h"

#include <glog/logging.h>

#include "neug/storages/module/module_factory.h"
#include "neug/storages/snapshot_meta.h"

namespace neug {

void ModuleStore::Open(Checkpoint& ckp, const SnapshotMeta& meta,
                       MemoryLevel level) {
  clear();

  const auto& modules = meta.modules();
  for (const auto& [key, desc] : modules) {
    // Create module via factory
    if (desc.module_type.empty()) {
      VLOG(1) << "Skipping module with empty type: " << key;
      continue;
    }

    auto module = ModuleFactory::instance().Create(desc.module_type);
    if (!module) {
      VLOG(1) << "Unknown module type: " << desc.module_type
              << " for key: " << key << " (skipping)";
      continue;
    }

    // Open the module
    module->Open(ckp, desc, level);

    // Store as shared_ptr
    modules_[key] = std::shared_ptr<Module>(module.release());
  }
}

SnapshotMeta ModuleStore::Dump(Checkpoint& ckp) const {
  SnapshotMeta meta;
  for (const auto& [key, mod] : modules_) {
    if (!mod)
      continue;
    meta.set_module(key, mod->Dump(ckp));
  }
  return meta;
}

std::shared_ptr<Module> ModuleStore::get(const std::string& key) const {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return nullptr;
  }
  return it->second;
}

void ModuleStore::set(const std::string& key, std::shared_ptr<Module> module) {
  modules_[key] = std::move(module);
}

bool ModuleStore::has(const std::string& key) const {
  return modules_.find(key) != modules_.end();
}

void ModuleStore::fork(const std::string& key, Checkpoint& ckp,
                       MemoryLevel level) {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return;
  }

  auto& module = it->second;
  if (!module) {
    return;
  }

  try {
    auto forked = module->Fork(ckp, level);
    if (forked) {
      it->second = std::shared_ptr<Module>(forked.release());
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to fork module [" << key << "]: " << e.what();
  } catch (...) {
    LOG(ERROR) << "Failed to fork module [" << key << "]: unknown error";
  }
}

std::vector<std::string> ModuleStore::keys() const {
  std::vector<std::string> result;
  result.reserve(modules_.size());
  for (const auto& pair : modules_) {
    result.push_back(pair.first);
  }
  return result;
}

}  // namespace neug
