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

  Status Init(std::unique_ptr<IndexMeta> meta,
              std::unique_ptr<IndexIDAccessor> index_id_accessor);

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
   * @brief Mark a vertex as deleted in the index.
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
