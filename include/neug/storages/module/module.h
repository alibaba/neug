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

#include <memory>
#include <string>

#include "neug/config.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

class Checkpoint;

/**
 * @brief Abstract interface for persistent graph-storage modules.
 *
 * Provides four lifecycle operations: Open (restore), Dump (persist),
 * Clone (zero-copy COW clone), and Detach (detach
 * shared storage before mutation).
 */
class Module {
 public:
  virtual ~Module() = default;

  /**
   * @brief Restore module state from descriptor.
   */
  virtual void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                    MemoryLevel level) = 0;

  /**
   * @brief Restore module state with access to the owning manifest.
   *
   * Composite modules can resolve named refs through @p manifest. Simple
   * modules keep using the descriptor-only Open implementation.
   */
  virtual void Open(Checkpoint& ckp, const CheckpointManifest&,
                    const ModuleDescriptor& descriptor, MemoryLevel level) {
    Open(ckp, descriptor, level);
  }

  /**
   * @brief Persist module state and write the descriptor to @p meta under
   * @p key.
   *
   * Composite modules may write additional referenced module entries.
   */
  virtual void Dump(Checkpoint& ckp, CheckpointManifest& meta,
                    const std::string& key) = 0;

  /**
   * @brief Create an independent module object that shares the same storage.
   * Zero-copy: creates a new Module object sharing the same IDataContainer(s).
   */
  virtual std::unique_ptr<Module> Clone() const = 0;

  /**
   * @brief Detach the underlying data containers before mutation, breaking
   * shared ownership with the parent COW clone.
   */
  virtual void Detach(Checkpoint& ckp, MemoryLevel level) = 0;

  /**
   * @brief Return factory registration key (e.g., "vertex_table").
   */
  virtual std::string ModuleTypeName() const = 0;
};

}  // namespace neug
