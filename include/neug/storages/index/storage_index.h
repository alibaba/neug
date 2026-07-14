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
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/doc_id_map.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

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

// --- Query/filter parameter types ---

// Stores the index query parameters, i.e., target value, comparison type, etc.
struct IndexQueryParams {
  virtual ~IndexQueryParams() = default;
};

// perform scalar filtering during the index lookup process
struct IndexFilterParams {
  virtual ~IndexFilterParams() = default;
};

// --- Index base class ---

/**
 * @brief Abstract base class for all index implementations.
 *
 * Index inherits Module for lifecycle (Open/Dump) and provides the
 * data operations (Search/Append/Delete) plus COW support (Fork/LazyFork).
 *
 * Extensions (e.g. zvec) provide concrete implementations by subclassing
 * Index and registering them through the ModuleFactory.
 */
class StorageIndex : public Module {
 public:
  StorageIndex()
      : meta_(std::make_unique<IndexMeta>()),
        doc_id_map_(std::make_unique<DocIDMap>()) {}

  StorageIndex(std::string name, std::unique_ptr<IndexMeta> meta)
      : meta_(std::move(meta)), doc_id_map_(std::make_unique<DocIDMap>()) {
    meta_->name = std::move(name);
  }

  ~StorageIndex() override = default;

  // --- Module interface ---
  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::string ModuleTypeName() const override;

  // --- Data operations ---

  /**
   * @brief Search for nearest neighbors.
   * @param params Query parameters (subclass-specific).
   * @param filter_params MVCC filter parameters.
   * @param graph Indexes may need to access graph data. For example, when
   * vector data is stored in the NeuG graph, ZVec can access this NeuG vector
   * data through this interface.
   * @param results Output: vid_t results after doc_id -> vid translation.
   */
  virtual result<std::vector<vid_t>> Search(
      const IndexQueryParams& params, const IndexFilterParams& filter_params,
      const StorageReadInterface& graph) = 0;

  /**
   * @brief Append a record for the given vertex id.
   *
   * Non-virtual: allocates doc_id via DocIDMap, then delegates to AppendImpl.
   *
   * @param vid The vertex id.
   * @param values Property values for the indexed columns.
   * @param graph Indexes may need to access graph data.
   */
  Status Append(vid_t vid, const Value& value,
                const StorageReadInterface& graph) {
    if (!doc_id_map_) {
      return Status::RuntimeError("DocIDMap not initialized");
    }
    doc_id_t doc_id = doc_id_map_->Insert(vid);
    return AppendImpl(vid, doc_id, value, graph);
  }

  /**
   * @brief Mark a vertex as deleted in the index.
   */
  virtual Status Delete(vid_t vid) = 0;

  // --- Metadata ---
  const IndexMeta& GetMeta() const { return *meta_; }
  void SetMeta(std::unique_ptr<IndexMeta> meta) { meta_ = std::move(meta); }

 protected:
  /**
   * @brief Subclass-specific append implementation.
   * @param vid The vertex id.
   * @param doc_id The doc_id allocated by DocIDMap.
   * @param values Property values for the indexed columns.
   * @param graph Indexes may need to access graph data.
   */
  virtual Status AppendImpl(vid_t vid, doc_id_t doc_id,
                            const Value& value,
                            const StorageReadInterface& graph) = 0;

  std::unique_ptr<IndexMeta> meta_;
  std::unique_ptr<DocIDMap> doc_id_map_;
};

}  // namespace neug
