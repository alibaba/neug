/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/storages/index/index_utils.h"

#include <vector>

#include "neug/compiler/common/string_utils.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"

namespace neug {

bool IsHNSWIndex(const IndexMeta& meta) {
  auto type = meta.type;
  common::StringUtils::toLower(type);
  return type == "hnsw";
}

Status AddBatchVertexIndexData(
    PropertyGraph& graph, label_t label, const std::vector<vid_t>& vids,
    const std::function<Status(StorageIndex&)>& prepare_index) {
  const auto& vtable = graph.get_vertex_table(label);
  auto& index_manager = graph.mutable_index_manager();
  auto indexes = index_manager.GetIndexesForUpdate(label);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    if (prepare_index) {
      RETURN_IF_NOT_OK(prepare_index(*index));
    }
    std::vector<const ColumnBase*> columns;
    columns.reserve(index->GetMeta().schema.columns.size());
    for (const auto& column : index->GetMeta().schema.columns) {
      const auto* property_column =
          vtable.GetPropertyColumnBase(column.property_name);
      if (!property_column) {
        return Status::InternalError("Indexed property column does not exist");
      }
      columns.push_back(property_column);
    }
    for (vid_t vid : vids) {
      IndexValues values;
      values.reserve(columns.size());
      for (const auto* column : columns) {
        values.push_back(column->get_any(vid));
      }
      RETURN_IF_NOT_OK(index->Upsert(vid, values));
    }
  }
  return Status::OK();
}

}  // namespace neug
