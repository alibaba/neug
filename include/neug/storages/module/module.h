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

// Forward-declare Checkpoint to avoid circular includes
// (workspace.h → snapshot_meta.h → module/module.h).
class Checkpoint;

/**
 * @brief Unified lifecycle interface for all persistent graph-storage modules.
 *
 * Module is an abstract base class that every top-level storage
 * component (VertexTable, EdgeTable, Column, CsrBase, …) implements.  It
 * provides four orthogonal lifecycle operations driven by a Checkpoint context:
 *
 *   - **Open**  – restore the module from a Checkpoint + descriptor.
 *   - **Dump**  – persist the current state to the Checkpoint's runtime
 *                 directory under a freshly generated UUID sub-path, and
 *                 return the resulting descriptor.
 *   - **Close** – release all in-memory buffers and file handles.
 *   - **Fork**  – create an independent sibling module written to the runtime
 *                 directory, respecting the requested MemoryLevel.
 *
 * ## Dump / Fork path convention
 *
 * Both `Dump` and `Fork` generate a UUID-named sub-directory under
 * `ckp.runtime_dir()`.  The returned `ModuleDescriptor::path` always points
 * to that location so that callers can find the written data.
 *
 * ## Fork semantics
 *
 * | MemoryLevel          | Typical implementation                          |
 * |----------------------|-------------------------------------------------|
 * | kSyncToFile          | dump files to runtime UUID path, mmap them      |
 * | kInMemory            | dump + open via MAP_PRIVATE (anonymous memory)  |
 * | kHugePagePreferred    | same as kInMemory but with hugepage allocation  |
 */
class Module {
 public:
  virtual ~Module() = default;

  /**
   * @brief Open the module from state described by @p descriptor, using the
   * Checkpoint for directory context.
   *
   * @param ckp        The checkpoint that provides snapshot / runtime paths.
   * @param descriptor Metadata produced by a prior Dump call.
   */
  virtual void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                    MemoryLevel level) = 0;

  /**
   * @brief Persist the current module state to a UUID directory under
   * @p ckp.runtime_dir(), returning an updated descriptor.
   */
  virtual ModuleDescriptor Dump(Checkpoint& ckp) = 0;

  /**
   * @brief Release all in-memory buffers and file handles.
   */
  virtual void Close() = 0;

  /**
   * @brief Create an independent copy of this module in a UUID sub-directory
   * of @p ckp.runtime_dir(), using the requested MemoryLevel strategy.
   *
   * @return A newly allocated Module owning its forked state.
   */
  virtual std::unique_ptr<Module> Fork(Checkpoint& ckp, MemoryLevel level) = 0;

  /**
   * @brief Return the factory registration key for this module type.
   *
   * Concrete subclasses MUST override this to return a non-empty string
   * that matches the key used in NEUG_REGISTER_MODULE.  The default
   * returns "" which prevents accidental factory registration of abstract
   * or intermediate classes.
   *
   * This value is written to ModuleDescriptor::module_type by each Dump()
   * implementation so that the factory can reconstruct the right type.
   */
  virtual std::string ModuleTypeName() const { return ""; }
};

}  // namespace neug
