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

#include "neug/storages/module/module.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

/**
 * @brief In-memory representation of a checkpoint's module inventory.
 *
 * SnapshotMeta is serialized as a single JSON file (kMetaFileName) inside a
 * checkpoint directory.  It maps canonical string keys to ModuleDescriptors for
 * every Module that belongs to the checkpoint (schema, vertex tables,
 * edge tables, …).
 *
 * **Key naming follows file_names.h conventions:**
 * | Module              | Key                               |
 * |---------------------|-----------------------------------|
 * | Schema              | `"schema"`                        |
 * | VertexTable `LABEL` | `"vertex_table_LABEL"`            |
 * | EdgeTable src→edge→dst | `"edge_SRC_EDGE_DST"`        |
 *
 * JSON on-disk format:
 * @code{.json}
 * {
 *   "version": 1,
 *   "modules": {
 *     "schema":                { "path": "...", "size": 0, ... },
 *     "vertex_table_Person":   { "path": "...", "size": 100, ... },
 *     "edge_Person_KNOWS_Person": { "path": "...", "size": 500, ... }
 *   }
 * }
 * @endcode
 */
class SnapshotMeta {
 public:
  /// Name of the meta file written inside the checkpoint directory.
  static constexpr const char* kMetaFileName = "meta";

  SnapshotMeta() = default;

  // ---------------------------------------------------------------------------
  // Module map access
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Canonical key helpers (mirror file_names.h naming)
  // ---------------------------------------------------------------------------

  /// Key for the schema module: "schema"
  static std::string schema_key() { return "schema"; }

  /// Key for a vertex table: "vertex_table_<LABEL>"
  static std::string vertex_table_key(const std::string& label) {
    return "vertex_table_" + label;
  }

  /// Key for an edge table: "edge_<SRC>_<EDGE>_<DST>"
  static std::string edge_table_key(const std::string& src_label,
                                    const std::string& edge_label,
                                    const std::string& dst_label) {
    return "edge_" + src_label + "_" + edge_label + "_" + dst_label;
  }

  // ---------------------------------------------------------------------------
  // Module overrides
  // ---------------------------------------------------------------------------

  /**
   * @brief Load the meta JSON from ckp.path()/meta.
   * @p descriptor is currently unused (meta is self-describing).
   */
  void Open(const std::string& file_path);

  void Dump(const std::string& file_path) const;

  static void GenerateEmptyMeta(const std::string& file_path);

 private:
  std::unordered_map<std::string, ModuleDescriptor> modules_;
};

}  // namespace neug
