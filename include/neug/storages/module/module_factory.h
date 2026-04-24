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
 * @brief Singleton registry for Module implementations.
 *
 * Maps module_type strings to creator functions for type-erased
 * deserialization from checkpoint files. Use NEUG_REGISTER_MODULE macro
 * to register module types.
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
   * @brief Register creator for module_type.
   */
  bool Register(const std::string& module_type, Creator creator);

  template <typename T>
  void Register() {
    Register(T::type_name(), [] { return std::make_unique<T>(); });
  }

  /**
   * @brief Create Module instance, or nullptr if not registered.
   */
  std::unique_ptr<Module> Create(const std::string& module_type) const;

  /**
   * @brief List all registered module types.
   */
  std::vector<std::string> List() const;

  /**
   * @brief Check if module_type is registered.
   */
  bool Has(const std::string& module_type) const;

 private:
  ModuleFactory() = default;
  std::unordered_map<std::string, Creator> creators_;
};

/**
 * @brief Registers Module creator via static initialization.
 */
struct ModuleRegistrar {
  ModuleRegistrar(const std::string& module_type,
                  ModuleFactory::Creator creator) {
    ModuleFactory::instance().Register(module_type, std::move(creator));
  }
};

/**
 * @brief Macro for automatic module registration at static initialization.
 *
 * Usage: Place REGISTER_MODULE(MyModule) in the source file after class
 * definition.
 */
#define NEUG_REGISTER_MODULE(Class)                              \
  __attribute__((constructor)) static void _register_##Class() { \
    ModuleFactory::instance().Register<Class>();                 \
  }

/**
 * @brief Macro for registering template module instantiations.
 *
 * Usage: REGISTER_TEMPLATE_MODULE(Column, int32_t)
 * Works with types containing '::' (e.g. std::string_view) by using
 * __COUNTER__ for a stable, type-name-independent function identifier.
 */
#define NEUG_REGISTER_TEMPLATE_MODULE_IMPL(TemplateClass, T, Counter)      \
  __attribute__(                                                           \
      (constructor)) static void _register_##TemplateClass##_##Counter() { \
    ModuleFactory::instance().Register<TemplateClass<T>>();                \
  }

// Indirection layer: N is not adjacent to ##, so __COUNTER__ is fully expanded
// here before being passed to IMPL (where it would otherwise be pasted raw).
#define NEUG_REGISTER_TEMPLATE_MODULE_N(TemplateClass, T, N) \
  NEUG_REGISTER_TEMPLATE_MODULE_IMPL(TemplateClass, T, N)

#define NEUG_REGISTER_TEMPLATE_MODULE(TemplateClass, T) \
  NEUG_REGISTER_TEMPLATE_MODULE_N(TemplateClass, T, __COUNTER__)

}  // namespace neug
