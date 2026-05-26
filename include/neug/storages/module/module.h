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
#include "neug/storages/module_descriptor.h"

namespace neug {

class Checkpoint;

/**
 * @brief Abstract interface for persistent graph-storage modules.
 *
 * Provides four lifecycle operations: Open (restore), Dump (persist),
 * Close (release), Fork (copy). Both Dump and Fork generate a UUID
 * sub-directory under ckp.runtime_dir().
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
   * @brief Persist to UUID sub-directory, return descriptor.
   */
  virtual ModuleDescriptor Dump(Checkpoint& ckp) = 0;

  /**
   * @brief Release all in-memory buffers and file handles.
   */
  virtual void Close() = 0;

  /**
   * @brief Create independent copy in UUID sub-directory.
   */
  virtual std::unique_ptr<Module> Fork(Checkpoint& ckp, MemoryLevel level) = 0;

  /**
   * @brief Return factory registration key (e.g., "vertex_table").
   */
  virtual std::string ModuleTypeName() const { return ""; }
};

}  // namespace neug
