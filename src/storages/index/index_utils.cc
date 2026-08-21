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

#include <tuple>

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
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  const auto& vtable = graph.get_vertex_table(label);
  auto& index_manager = graph.mutable_index_manager();

  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndexForUpdate(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  if (!pk_indexes->empty()) {
    auto pk_col = vtable.GetPropertyColumn(pk_name);
    if (!pk_col) {
      return Status::InternalError("Primary key column does not exist");
    }
    for (auto* index : pk_indexes.value()) {
      if (prepare_index) {
        RETURN_IF_NOT_OK(prepare_index(*index));
      }
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Upsert(vid, pk_col->get_any(vid)));
      }
    }
  }

  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx]) {
      continue;
    }
    auto indexes = index_manager.GetIndexForUpdate(
        label, v_schema->property_names[prop_idx]);
    if (!indexes) {
      return indexes.error();
    }
    if (indexes->empty()) {
      continue;
    }
    auto col = vtable.GetPropertyColumn(static_cast<int32_t>(prop_idx));
    if (!col) {
      continue;
    }
    for (auto* index : indexes.value()) {
      if (prepare_index) {
        RETURN_IF_NOT_OK(prepare_index(*index));
      }
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Upsert(vid, col->get_any(vid)));
      }
    }
  }
  return Status::OK();
}

}  // namespace neug
