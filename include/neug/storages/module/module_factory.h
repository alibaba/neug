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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/storages/module/module.h"

namespace neug {

/**
 * @brief Global registry for storage Module implementations.
 *
 * ModuleFactory is a singleton that maps `module_type` strings (as stored in
 * ModuleDescriptor::module_type) to creator functions.  This enables
 * type-erased deserialization: given a ModuleDescriptor loaded from a
 * checkpoint file, the runtime can instantiate the correct Module subclass
 * without compile-time coupling.
 *
 * ## Registration
 *
 * Use the NEUG_REGISTER_MODULE macro at namespace scope to register a module
 * type.  Registration happens before main() via the global-constructor
 * mechanism:
 *
 * @code{.cpp}
 * // In vertex_table.cc (or a dedicated *_module.cc translation unit):
 * NEUG_REGISTER_MODULE("vertex_table", VertexTable);
 * @endcode
 *
 * ## Retrieval
 *
 * @code{.cpp}
 * auto mod = ModuleFactory::instance().Create(descriptor.module_type);
 * if (!mod) { return; }  // nullptr means unknown module_type
 * mod->Open(ckp, descriptor);
 * @endcode
 *
 * ## Enumeration
 *
 * @code{.cpp}
 * for (const auto& name : ModuleFactory::instance().List()) {
 *   LOG(INFO) << "registered module: " << name;
 * }
 * @endcode
 */
class ModuleFactory {
 public:
  /// Creator function: returns a default-constructed Module instance.
  using Creator = std::function<std::unique_ptr<Module>()>;

  // Non-copyable, non-movable singleton.
  ModuleFactory(const ModuleFactory&) = delete;
  ModuleFactory& operator=(const ModuleFactory&) = delete;

  /**
   * @brief Access the singleton ModuleFactory instance.
   */
  static ModuleFactory& instance();

  /**
   * @brief Register a creator under @p module_type.
   *
   * If @p module_type is already registered the previous entry is overwritten.
   * Returns `true` to allow use inside static variable initializers.
   */
  bool Register(const std::string& module_type, Creator creator);

  /**
   * @brief Create a fresh Module instance for @p module_type.
   *
   * @return A newly constructed Module, or nullptr if @p module_type was never
   *         registered.
   */
  std::unique_ptr<Module> Create(const std::string& module_type) const;

  /**
   * @brief Return a sorted list of all registered module type strings.
   */
  std::vector<std::string> List() const;

  /**
   * @brief Return true if @p module_type is registered.
   */
  bool Has(const std::string& module_type) const;

 private:
  ModuleFactory() = default;
  std::unordered_map<std::string, Creator> creators_;
};

// ---------------------------------------------------------------------------
// Static-registration helper
// ---------------------------------------------------------------------------

/**
 * @brief RAII helper that registers a Module creator on construction.
 *
 * Use indirectly via the NEUG_REGISTER_MODULE macro.
 */
struct ModuleRegistrar {
  ModuleRegistrar(const std::string& module_type,
                  ModuleFactory::Creator creator) {
    ModuleFactory::instance().Register(module_type, std::move(creator));
  }
};

// ---------------------------------------------------------------------------
// Convenience macro
// ---------------------------------------------------------------------------

/**
 * @brief Register @p ClassName as the implementation for the given
 * @p type_name string.
 *
 * Place this macro at namespace scope in a .cc file that is linked into the
 * binary.  It generates a translation-unit-local static ModuleRegistrar whose
 * constructor fires before main().
 *
 * Example:
 * @code{.cpp}
 * NEUG_REGISTER_MODULE("vertex_table", VertexTable);
 * @endcode
 */
#define NEUG_REGISTER_MODULE(type_name, ClassName)                  \
  static ::neug::ModuleRegistrar NEUG_MODULE_REGISTRAR_##ClassName( \
      type_name, []() -> std::unique_ptr<::neug::Module> {          \
        return std::make_unique<ClassName>();                       \
      })

}  // namespace neug
