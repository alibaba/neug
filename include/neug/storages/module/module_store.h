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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "neug/config.h"
#include "neug/storages/module/module.h"

namespace neug {

class Checkpoint;
class SnapshotMeta;
class ModuleDescriptor;

/**
 * @brief Container for all graph storage modules.
 *
 * ModuleStore holds shared ownership of all Module instances (Column, CSR,
 * Schema, etc.) via shared_ptr. Copy construction is cheap - it only
 * increments reference counts, enabling zero-overhead sharing between
 * the live PropertyGraph and UpdateTransaction's view.
 *
 * **COW (Copy-on-Write) Pattern**:
 * - Initial state: all modules shared with live graph
 * - On write: fork() the specific module, replace in store
 * - On abort: simply discard the ModuleStore
 */
class ModuleStore {
 public:
  ModuleStore() = default;

  // Shared semantics: copy construction shares all modules
  ModuleStore(const ModuleStore&) = default;
  ModuleStore& operator=(const ModuleStore&) = default;

  // Move semantics
  ModuleStore(ModuleStore&&) = default;
  ModuleStore& operator=(ModuleStore&&) = default;

  /** @brief Open all modules from a checkpoint's SnapshotMeta.
   *
   * This iterates through all module descriptors in the SnapshotMeta,
   * creates each module via the factory, and calls Open on each.
   *
   * @param ckp The checkpoint providing directory context.
   * @param meta The snapshot metadata containing all module descriptors.
   * @param level The memory level for opening modules.
   */
  void Open(Checkpoint& ckp, const SnapshotMeta& meta, MemoryLevel level);

  /** @brief Dump all modules to a SnapshotMeta.
   *
   * Calls Dump on each module in the store and collects the resulting
   * ModuleDescriptors into a SnapshotMeta.
   *
   * @param ckp The checkpoint providing directory context for Dump.
   * @return SnapshotMeta containing all module descriptors.
   */
  SnapshotMeta Dump(Checkpoint& ckp) const;

  /** @brief Get a module by key. Returns nullptr if not found. */
  std::shared_ptr<Module> get(const std::string& key) const;

  /** @brief Set a module with the given key. */
  void set(const std::string& key, std::shared_ptr<Module> module);

  /** @brief Check if a module exists for the given key. */
  bool has(const std::string& key) const;

  /** @brief Fork a specific module, replacing it with the forked copy.
   *
   * If the module doesn't exist, this is a no-op.
   * If fork fails, the original module remains unchanged.
   */
  void fork(const std::string& key, Checkpoint& ckp, MemoryLevel level);

  /** @brief Get all keys in the store. */
  std::vector<std::string> keys() const;

  /** @brief Clear all modules. */
  void clear() { modules_.clear(); }

  /** @brief Check if the store is empty. */
  bool empty() const { return modules_.empty(); }

  /** @brief Get the number of modules. */
  size_t size() const { return modules_.size(); }

 private:
  std::map<std::string, std::shared_ptr<Module>> modules_;
};

}  // namespace neug
