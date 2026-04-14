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
 * ModuleDescriptor carries the essential metadata required to open, fork, or
 * restore a Module instance.  It is serializable to/from a rapidjson
 * JSON object so it can be persisted alongside snapshot data or passed between
 * components.
 *
 * JSON schema example:
 * @code{.json}
 * {
 *   "path":         "snapshots/v1/vertex_person",
 *   "size":         420000,
 *   "type":         "vertex_table",
 *   "version":      1,
 *   "memory_level": 2,
 *   "extra": {
 *     "label": "Person"
 *   },
 *   "sub_modules": {
 *     "indexer": { "path": "...", "type": "lf_indexer", ... },
 *     "table":   { "path": "...", "type": "table", ... }
 *   }
 * }
 * @endcode
 */
struct ModuleDescriptor {
  // Rule-of-5: explicitly declared because of the pimpl unique_ptr member
  // (SubModulesImpl is an incomplete type in the header).
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

  /// Free-form type tag, e.g. "vertex_table", "edge_table", "csr".
  std::string type;

  /// Module type identifier for factory registration.
  std::string module_type;

  /// Schema / format version.  Increment when the on-disk layout changes.
  uint32_t version = 1;

  // ---------------------------------------------------------------------------
  // Extra key-value pairs
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Sub-module descriptors
  // ---------------------------------------------------------------------------

  /**
   * @brief Nested descriptors for sub-components, keyed by role name
   * (e.g. "indexer", "table", "in_csr", "out_csr").
   *
   * Each sub-module's @c path field holds the **absolute** path to the
   * directory where that sub-component's snapshot files reside.
   *
   * Stored as a unique_ptr to allow self-referential definition (an
   * unordered_map instantiated inside a struct requires the value type to be
   * complete, which is impossible if value == the same struct).
   */
  const std::unordered_map<std::string, ModuleDescriptor>& sub_modules() const;
  std::unordered_map<std::string, ModuleDescriptor>& sub_modules();
  void set_sub_module(const std::string& key, ModuleDescriptor desc);
  bool has_sub_module(const std::string& key) const;
  ModuleDescriptor get_sub_module(const std::string& key) const;

  // ---------------------------------------------------------------------------
  // Serialization
  // ---------------------------------------------------------------------------

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
