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

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include <rapidjson/document.h>

#include "neug/config.h"

namespace neug {

/**
 * @brief Metadata descriptor for a storage module.
 *
 * Carries the metadata required to open or restore a Module instance.
 * Supports JSON serialization for persistence and inter-component
 * communication.
 */
struct ModuleDescriptor {
  ModuleDescriptor();
  ~ModuleDescriptor();
  ModuleDescriptor(const ModuleDescriptor&);
  ModuleDescriptor& operator=(const ModuleDescriptor&);
  ModuleDescriptor(ModuleDescriptor&&) noexcept;
  ModuleDescriptor& operator=(ModuleDescriptor&&) noexcept;

  /// Must be the absolute path to the file or directory where the module's
  /// snapshot data resides.  This is the only path field that ModuleDescriptor
  /// itself defines, but sub-modules can also use the same convention for their
  /// own path fields.
  std::string path;

  /// Logical element count (vertices, edges, rows, …) at the time of the last
  /// Dump.  Zero means "unknown".
  uint64_t size = 0;

  /// Module type identifier for factory registration.
  std::string module_type;

  /**
   * @brief Set an extra key-value pair.  Returns *this for chaining.
   */
  ModuleDescriptor& set(const std::string& key, const std::string& value) {
    extra_[key] = value;
    return *this;
  }

  /**
   * @brief Retrieve an extra value by key.  Returns std::nullopt if absent.
   */
  std::optional<std::string> get(const std::string& key) const {
    auto it = extra_.find(key);
    if (it == extra_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  /**
   * @brief Returns true if the key exists in the extra map.
   */
  bool has(const std::string& key) const { return extra_.count(key) > 0; }

  /**
   * @brief Read-only access to all extra key-value pairs.
   */
  const std::unordered_map<std::string, std::string>& extra() const {
    return extra_;
  }

  const std::unordered_map<std::string, ModuleDescriptor>& sub_modules() const;
  void set_sub_module(const std::string& key, ModuleDescriptor desc);
  bool has_sub_module(const std::string& key) const;

  /**
   * @brief Return the sub-module descriptor for @p key, or std::nullopt if
   * absent.  Mirrors get() for extra KV pairs — prefer this when the caller
   * must handle the missing case explicitly (e.g. before reading .path).
   */
  std::optional<ModuleDescriptor> get_sub_module(const std::string& key) const;

  /**
   * @brief Return the sub-module descriptor for @p key, or an empty
   * ModuleDescriptor if absent.  Use this when passing the result directly
   * to Module::Open(), where an empty descriptor means "open from scratch".
   */
  ModuleDescriptor get_sub_module_or_default(
      const std::string& key,
      const ModuleDescriptor& default_desc = ModuleDescriptor()) const;

  /**
   * @brief Serialize this descriptor to a rapidjson Value (object).
   *
   * The returned Value borrows from @p alloc – keep the allocator alive for as
   * long as the Value is in use.
   */
  rapidjson::Value ToJson(rapidjson::Document::AllocatorType& alloc) const;

  /**
   * @brief Deserialize a ModuleDescriptor from a rapidjson Value (object).
   *
   * Missing fields keep their default values.
   */
  static ModuleDescriptor FromJson(const rapidjson::Value& obj);

  /**
   * @brief Serialize to a self-contained JSON string.
   */
  std::string ToJsonString() const;

  /**
   * @brief Deserialize from a JSON string.
   */
  static ModuleDescriptor FromJsonString(const std::string& json);

 private:
  /// Optional free-form key-value pairs for module-specific metadata.
  std::unordered_map<std::string, std::string> extra_;

 public:
  // Pimpl for sub_modules to avoid incomplete-type issues with self-referential
  // unordered_map instantiation inside the struct definition.
  struct SubModulesImpl;

 private:
  std::unique_ptr<SubModulesImpl> sub_modules_impl_;
};

}  // namespace neug
