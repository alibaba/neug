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
 * @brief Metadata descriptor for a single Module instance.
 *
 * Holds the path / size / module_type / extras a Module needs to open or dump
 * itself.  Descriptors are leaves: composite structures (VertexTable,
 * EdgeTable, …) hold their children as separate top-level entries in
 * SnapshotMeta::modules_, keyed by the orchestration layer.
 */
struct ModuleDescriptor {
  // High-frequency path key constants
  static constexpr const char* kDataPath = "data";
  static constexpr const char* kItemsPath = "items";
  static constexpr const char* kNbrListPath = "nbr_list";
  static constexpr const char* kDegreeListPath = "degree_list";
  static constexpr const char* kCapacityListPath = "capacity_list";

  ModuleDescriptor() = default;
  ~ModuleDescriptor() = default;
  ModuleDescriptor(const ModuleDescriptor&) = default;
  ModuleDescriptor& operator=(const ModuleDescriptor&) = default;
  ModuleDescriptor(ModuleDescriptor&&) noexcept = default;
  ModuleDescriptor& operator=(ModuleDescriptor&&) noexcept = default;

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

  /**
   * @brief Record a filesystem path under @p name.  Paths live in a separate
   * map from `extra_` so Checkpoint::UpdateMeta / Checkpoint::Checkpoint can
   * relativize / absolutize them on persistence without scanning every extras
   * key by name.  Mirrors set() in chainability.
   */
  ModuleDescriptor& set_path(const std::string& name, std::string path) {
    paths_[name] = std::move(path);
    return *this;
  }

  /// Look up a path by name.  Returns std::nullopt when absent.
  std::optional<std::string> get_path(const std::string& name) const {
    auto it = paths_.find(name);
    if (it == paths_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  bool has_path(const std::string& name) const {
    return paths_.count(name) > 0;
  }

  /// Read-only access to all named paths.
  const std::unordered_map<std::string, std::string>& paths() const {
    return paths_;
  }

  /// Mutable access — useful for in-place rewrites (e.g. absolute ↔ relative
  /// conversion in Checkpoint).
  std::unordered_map<std::string, std::string>& mutable_paths() {
    return paths_;
  }

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

 private:
  /// Optional free-form key-value pairs for module-specific metadata.
  std::unordered_map<std::string, std::string> extra_;

  /// Filesystem paths a Module owns, keyed by an arbitrary local name (e.g.
  /// "nbr_list", "items").  Carved out of `extra_` so checkpoint code can
  /// rewrite paths without scanning every extras key by suffix.
  std::unordered_map<std::string, std::string> paths_;
};

}  // namespace neug
