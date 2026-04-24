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
#include <string>
#include <unordered_map>

#include "neug/storages/graph/schema.h"
#include "neug/storages/module/module.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

/**
 * @brief In-memory representation of a checkpoint's module inventory.
 *
 * Maps canonical string keys to ModuleDescriptors for all modules in a
 * checkpoint. Serialized as JSON for persistence inside checkpoint directories.
 */
class SnapshotMeta {
 public:
  /// Name of the meta file written inside the checkpoint directory.
  static constexpr const char* kMetaFileName = "meta";

  SnapshotMeta() = default;

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

  void Open(const std::string& file_path);

  void Dump(const std::string& file_path) const;

  static void GenerateEmptyMeta(const std::string& file_path);

  const Schema& GetSchema() const;

  void SetSchema(const Schema& schema);

 private:
  Schema schema_;
  std::unordered_map<std::string, ModuleDescriptor> modules_;
};

}  // namespace neug
