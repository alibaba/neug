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

#include "neug/storages/graph/schema.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

/**
 * @brief In-memory representation of a checkpoint's module inventory.
 *
 * Maps canonical string keys to ModuleDescriptors for all modules in a
 * checkpoint. Serialized as JSON for persistence inside checkpoint directories.
 */
class CheckpointManifest {
 public:
  /// Name of the meta file written inside the checkpoint directory.
  static constexpr const char* kMetaFileName = "meta";

  /// Current on-disk format version for the meta JSON.
  ///
  /// Bump only on breaking changes to the JSON layout (renamed/removed
  /// fields, changed value semantics).  Additive changes (new optional
  /// fields) do not require a bump.  Readers must reject unknown versions.
  static constexpr int kFormatVersion = 1;

  CheckpointManifest() = default;

  /**
   * @brief Return the descriptor for @p key, or std::nullopt if absent.
   */
  std::optional<ModuleDescriptor> module(const std::string& key) const;

  /**
   * @brief Insert or replace the descriptor for @p key.
   */
  void set_module(const std::string& key, ModuleDescriptor desc);

  /**
   * @brief Remove the descriptor for @p key (no-op if absent).
   */
  void remove_module(const std::string& key);

  /**
   * @brief Returns true if @p key is present in the module map.
   */
  bool has_module(const std::string& key) const;

  /**
   * @brief Read-only access to the full module map.
   */
  const std::unordered_map<std::string, ModuleDescriptor>& modules() const;

  /**
   * @brief Mutable access to the module map.  Useful for in-place traversal
   * (e.g. recursive path rewriting in Checkpoint::UpdateMeta) that would
   * otherwise require copy-modify-replace cycles.
   */
  std::unordered_map<std::string, ModuleDescriptor>& mutable_modules();

  /**
   * @brief Retrieve a scalar value by key.  Returns std::nullopt if absent.
   *
   * Scalars hold shell-owned state that has no Module home of its own —
   * LFIndexer's num_elements, EdgeTable's row_count, hash policy index, etc.
   * Convention: namespace the key by owner using `/`, e.g.
   * "indexer_person/num_elements".
   */
  std::optional<std::string> GetScalar(const std::string& key) const;

  /// Insert or replace a scalar entry.
  void SetScalar(std::string key, std::string value);

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

  /// Read-only access to the full scalar map.
  const std::unordered_map<std::string, std::string>& scalars() const;

  void Load(const std::string& file_path);

  void Save(const std::string& file_path) const;

  static void GenerateEmptyMeta(const std::string& file_path);

  const Schema& GetSchema() const;

  void SetSchema(const Schema& schema);

  /// Returns true if the meta file contained a "schema" field when loaded.
  /// GenerateEmptyMeta() does not write a schema, so this returns false for
  /// stub checkpoints created during initial DB creation.  A fully committed
  /// checkpoint (via Save()) always writes a schema, even if the graph has
  /// no tables.
  bool has_schema() const { return has_schema_; }

 private:
  Schema schema_;
  bool has_schema_ = false;
  std::unordered_map<std::string, ModuleDescriptor> modules_;
  std::unordered_map<std::string, std::string> scalars_;
};

}  // namespace neug
