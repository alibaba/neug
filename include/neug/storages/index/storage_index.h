/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <rapidjson/document.h>

#include "neug/common/types.h"
#include "neug/common/types/value.h"
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

class ColumnBase;
class VertexSet;

// --- Index metadata types ---

struct IndexBindSchema {
  label_t label_id = 0;
  // Only single-property indexes are supported.
  std::string property_name;
  DataType property_type;

  rapidjson::Value ToJson(rapidjson::Document::AllocatorType& alloc) const;
  static IndexBindSchema FromJson(const rapidjson::Value& obj);
};

struct IndexMeta {
  std::string name;
  std::string type;
  IndexBindSchema schema;
  common::case_insensitive_map_t<std::string> options;

  std::string ToJsonString() const;
  static IndexMeta FromJsonString(const std::string& json_str);
};

// --- Query parameter types ---

// Stores the index query parameters, i.e., target value, comparison type, etc.
struct IndexQueryParams {
  virtual ~IndexQueryParams() = default;
};

struct IndexBindContext {
  const ColumnBase* column = nullptr;
};

// --- Index base class ---

/**
 * @brief Abstract base class for all index implementations.
 *
 * Index inherits Module for lifecycle (Open/Dump) and provides the
 * data operations (Search/Upsert/Delete) plus COW support (Fork/LazyFork).
 *
 * Extensions (e.g. zvec) provide concrete implementations by subclassing
 * Index and registering them through the ModuleFactory.
 */
class StorageIndex : public Module {
 public:
  StorageIndex() = default;

  /**
   * @brief Initialize the index object during CreateIndex.
   *
   * Init configures the in-memory index object with its metadata and ID
   * accessor. It does not open persistent storage, bind graph columns, or
   * populate index data.
   */
  virtual Status Init(std::unique_ptr<IndexMeta> meta,
                      std::unique_ptr<IndexIDAccessor> index_id_accessor);

  ~StorageIndex() override = default;

  // --- Module interface ---
  /**
   * @brief Open the index's persistent resources from a checkpoint.
   *
   * Open restores or creates the backing storage. It does not bind the index
   * to a graph column or build index entries from existing vertices.
   */
  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  std::string ModuleTypeName() const override;

  /**
   * @brief Bind the index to its non-owning property column dependency.
   *
   * Rebind is called when an index is initially created and after
   * PropertyGraph::Open or PropertyGraph::Clone so the index refers to the
   * column owned by the current graph version.
   */
  virtual Status Rebind(const IndexBindContext&) { return Status::OK(); }

  // --- Data operations ---

  /**
   * @brief Populate a newly created index from all visible vertices.
   *
   * BulkBuild is used during CreateIndex to populate the index from existing
   * graph data. Unlike Init (object configuration) and Open (persistent
   * resource setup), it builds index records; subsequent changes are maintained
   * through Upsert and Delete.
   *
   * Contract: Init -> Open -> Rebind -> BulkBuild.
   * Implementations must not retain a reference to vertices.
   */
  virtual Status BulkBuild(const VertexSet& vertices) = 0;

  /**
   * @brief Search the index and translate internal index ids to vertex ids.
   * @param params Query parameters (subclass-specific).
   * @return Vertex ids corresponding to valid internal index ids.
   */
  result<std::vector<vid_t>> Search(const IndexQueryParams& params);

  /**
   * @brief Insert or replace the index record for a vertex id.
   *
   * Non-virtual: allocates a new internal index id via IndexIDAccessor, then
   * delegates to AppendImpl.
   *
   * @param vid The vertex id.
   * @param new_value The new property value for the indexed column.
   */
  Status Upsert(vid_t vid, const Value& new_value);

  /**
   * @brief Mark a vertex as deleted using tombstone semantics.
   *
   * This removes the accessor mapping but deliberately leaves the underlying
   * index entry in place; searches filter it through the missing mapping.
   * TODO: Add Compact/GC support to periodically reclaim tombstoned entries.
   */
  virtual Status Delete(vid_t vid);

  // --- Metadata ---
  const IndexMeta& GetMeta() const { return *meta_; }

 protected:
  virtual result<std::vector<index_id_t>> SearchImpl(
      const IndexQueryParams& params) = 0;
  virtual Status AppendImpl(index_id_t index_id, const Value& value) = 0;

  std::unique_ptr<IndexMeta> meta_;
  std::unique_ptr<IndexIDAccessor> index_id_accessor_;
};

}  // namespace neug
