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
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "neug/storages/graph/schema.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

class Checkpoint;

/**
 * @brief In-memory representation of a checkpoint's module inventory.
 *
 * Maps canonical string keys to ModuleDescriptors for all modules in a
 * checkpoint. On disk every descriptor references immutable object IDs; the
 * Checkpoint root resolves them to local paths after loading.
 */
class CheckpointManifest {
 public:
  /// Current on-disk format version for the manifest JSON.
  ///
  /// Bump only on breaking changes to the JSON layout (renamed/removed
  /// fields, changed value semantics).  Additive changes (new optional
  /// fields) do not require a bump.  Readers must reject unknown versions.
  ///
  /// Version 1 belongs to the legacy checkpoint-N meta format. This immutable
  /// objects + complete manifest layout starts at version 2.
  static constexpr int kFormatVersion = 2;

  CheckpointManifest() = default;

  /// Return the descriptor for @p key, or nullptr if absent.
  const ModuleDescriptor* FindModule(const std::string& key) const;

  /// Return the mutable descriptor for @p key, or nullptr if absent.
  ModuleDescriptor* FindMutableModule(const std::string& key);

  /**
   * @brief Insert or replace the descriptor for @p key.
   */
  void SetModule(const std::string& key, ModuleDescriptor desc);

  /**
   * @brief Returns true if @p key is present in the module map.
   */
  bool HasModule(const std::string& key) const;

  /// Copy @p key and its referenced-module dependency closure from @p prev.
  /// Object references are immutable and therefore reused without file I/O.
  void ReuseModuleClosureFrom(const CheckpointManifest& prev,
                              const std::string& key);

  /**
   * @brief Read-only access to the full module map.
   */
  const std::unordered_map<std::string, ModuleDescriptor>& Modules() const;

  /// Insert or replace a scalar entry.
  void SetScalar(std::string key, std::string value);

  /**
   * @brief If @p key exists in @p prev, copy its scalar value into this
   * manifest. Scalars have no filesystem payload, so this is a plain copy
   * (unlike ReuseModuleFrom).
   */
  void CopyScalarFrom(const CheckpointManifest& prev, const std::string& key);

  /// Typed scalar accessor; performs std::istringstream parsing on the raw
  /// string value.  Returns std::nullopt if the key is absent or parsing fails.
  template <typename T>
  std::optional<T> GetScalarAs(const std::string& key) const {
    auto raw = GetScalar(key);
    if (!raw) {
      return std::nullopt;
    }
    std::istringstream iss(*raw);
    T value;
    if (!(iss >> value)) {
      return std::nullopt;
    }
    return value;
  }

  void Load(const std::string& file_path);

  void Save(const std::string& file_path) const;

  const Schema& GetSchema() const;

  void SetSchema(const Schema& schema);

  /// Returns true if the manifest contains a schema. A committed manifest
  /// always carries a schema, even when the graph has no tables.
  bool has_schema() const { return has_schema_; }

  /// Greatest transaction timestamp represented by this manifest. Recovery
  /// starts WAL replay at the next timestamp.
  uint64_t base_timestamp() const { return base_timestamp_; }

 private:
  std::optional<std::string> GetScalar(const std::string& key) const;

  void ReuseModuleClosureFromImpl(const CheckpointManifest& prev,
                                  const std::string& key,
                                  std::unordered_set<std::string>& visited);

  Schema schema_;
  bool has_schema_ = false;
  uint64_t base_timestamp_ = 0;
  std::unordered_map<std::string, ModuleDescriptor> modules_;
  std::unordered_map<std::string, std::string> scalars_;
};

}  // namespace neug
