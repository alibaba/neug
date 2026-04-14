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
#include <unordered_map>
#include <utility>

#include "neug/config.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/module/module.h"
#include "neug/storages/module/module_factory.h"
#include "neug/storages/module_descriptor.h"
#include "neug/storages/snapshot_meta.h"
#include "neug/utils/exception/exception.h"

namespace neug {

/**
 * @brief Transient broker that owns named Module instances during Open and
 * Dump cycles.  On Open, ModuleStore reads SnapshotMeta entries, asks the
 * factory to construct a Module per entry, calls Module::Open, and holds the
 * result until the caller TakeModule()s it out.  On Dump, the caller hands
 * each Module into the store via SetModule(name, std::move(unique_ptr))
 * (typically via the owner's TakeXxx() accessor); ModuleStore then iterates
 * and calls Module::Dump on each, writing descriptors to a SnapshotMeta.
 *
 * Scalars that have no Module home of their own (LFIndexer's num_elements,
 * EdgeTable's table_idx, ...) live separately on SnapshotMeta::scalars_; see
 * `module_naming::*` for naming conventions.
 */
class ModuleStore {
 public:
  ModuleStore() = default;
  ~ModuleStore() = default;
  ModuleStore(const ModuleStore&) = delete;
  ModuleStore& operator=(const ModuleStore&) = delete;
  ModuleStore(ModuleStore&&) = default;
  ModuleStore& operator=(ModuleStore&&) = default;

  /**
   * @brief Instantiate (via ModuleFactory) and Open every module entry from
   * @p checkpoint 's SnapshotMeta.  Throws if any entry's `module_type` is not
   * registered with the factory.
   */
  void Open(Checkpoint& checkpoint, MemoryLevel level);

  /**
   * @brief Same as Open(checkpoint, level), but reads module entries from the
   * supplied @p meta instead of the checkpoint's resident meta.  Useful for
   * test fixtures that round-trip via an explicit SnapshotMeta token rather
   * than via UpdateMeta on the checkpoint.
   */
  void Open(Checkpoint& checkpoint, const SnapshotMeta& meta,
            MemoryLevel level);

  /**
   * @brief Hand the store an owned module under @p name.  Used both on the
   * Open side (for fresh modules built outside the factory) and on the Dump
   * side (callers move modules in via their TakeXxx() accessor).  Replaces
   * any existing entry under the same name.
   */
  void SetModule(const std::string& name, std::unique_ptr<Module>&& module);

  /**
   * @brief Iterate every owned module, call Module::Dump on each, and write
   * the resulting descriptors into @p meta.  Existing entries in @p meta
   * keep their values unless their key is in the store.  Does not write
   * scalars — callers manage those directly on @p meta.
   */
  void Dump(Checkpoint& checkpoint, SnapshotMeta& meta);

  /**
   * @brief True if a module is registered under @p name.
   */
  bool Contains(const std::string& name) const;

  /**
   * @brief Borrow a module by name without transferring ownership.  Returns
   * nullptr if absent.
   */
  const Module* GetModule(const std::string& name) const;
  Module* GetModule(const std::string& name);

  /**
   * @brief Move an owned module out of the store and erase the entry.
   * Returns nullptr if absent (or if the entry is borrowed — borrowed
   * entries are not transferable).
   */
  std::unique_ptr<Module> TakeModule(const std::string& name);

  /// Typed overload — `dynamic_cast` the moved-out module to @p T.  When
  /// @p require is true, throws on miss or cast failure (the default; the
  /// hot path).  When false, returns nullptr in those cases.  On cast
  /// failure the original Module is destroyed since the store has already
  /// erased its entry.
  template <typename T>
  std::unique_ptr<T> TakeModule(const std::string& name, bool require = true) {
    auto base = TakeModule(name);
    if (!base) {
      if (require) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "ModuleStore::TakeModule: no module under name '" + name + "'");
      }
      return nullptr;
    }
    Module* raw = base.release();
    if (auto* casted = dynamic_cast<T*>(raw)) {
      return std::unique_ptr<T>(casted);
    }
    // Not a T — rewrap to preserve correct destruction, then fail.
    std::string actual_type = raw ? raw->ModuleTypeName() : std::string{};
    base.reset(raw);  // released via Module's virtual dtor on scope exit
    if (require) {
      THROW_INVALID_ARGUMENT_EXCEPTION("ModuleStore::TakeModule: module '" +
                                       name + "' (type='" + actual_type +
                                       "') does not satisfy requested cast");
    }
    return nullptr;
  }

 private:
  std::map<std::string, std::unique_ptr<Module>> modules_;
};

}  // namespace neug
