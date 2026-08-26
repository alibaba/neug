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

#include "neug/storages/graph/graph_interface.h"

#include <cstring>
#include <limits>

#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/index/index_utils.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/vec_column.h"
#include "neug/utils/result.h"

namespace neug {

namespace {

std::unique_ptr<ColumnBase> FromArrayColumn(const ArrayColumn& array,
                                            size_t vid_size,
                                            const Value& default_value,
                                            Checkpoint& ckp,
                                            MemoryLevel level) {
  if (vid_size > array.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromArrayColumn: vid size exceeds array column size");
  }
  if (vid_size != 0 && vid_size - 1 > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromArrayColumn: vid size exceeds the VID range");
  }
  auto offset_accessor = std::make_unique<DefaultIndexIDAccessor>();
  offset_accessor->Open(ckp, ModuleDescriptor{}, level);
  for (size_t vid = 0; vid < vid_size; ++vid) {
    offset_accessor->UpsertVID(static_cast<vid_t>(vid));
  }

  const auto child_type = ArrayType::GetChildType(array.array_type()).id();
  switch (child_type) {
  case DataTypeId::kFloat:
    return std::make_unique<VecColumn>(
        array.shared_buffer<float>(), std::move(offset_accessor),
        array.array_type(), array.size(), default_value, ckp, level);
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW index supports only FLOAT array properties");
  }
}

std::unique_ptr<ArrayColumn> FromVecColumn(VecColumn& vec, size_t vid_size,
                                           size_t size,
                                           const Value& default_value,
                                           Checkpoint& ckp, MemoryLevel level) {
  if (vid_size > size) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromVecColumn: vid size exceeds array column size");
  }
  if (vid_size != 0 && vid_size - 1 > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromVecColumn: vid size exceeds the VID range");
  }

  auto array_column = std::make_unique<ArrayColumn>(vec.array_type());
  array_column->Open(ckp, ModuleDescriptor{}, level);
  array_column->resize(size, default_value);

  const auto* src = static_cast<const float*>(vec.get_buffer_ptr());
  auto* dst =
      static_cast<float*>(array_column->shared_buffer<float>()->GetData());
  const auto array_size = vec.array_size();
  const auto* offset_accessor = vec.get_offset_accessor();
  for (size_t vid = 0; vid < vid_size; ++vid) {
    const auto offset =
        offset_accessor->GetIndexIDByVID(static_cast<vid_t>(vid));
    if (offset == INVALID_OFFSET) {
      continue;
    }
    if (offset >= vec.size()) {
      THROW_RUNTIME_ERROR("FromVecColumn: offset out of range");
    }
    std::memcpy(dst + vid * array_size, src + offset * array_size,
                array_size * sizeof(float));
  }
  return array_column;
}

}  // namespace

result<std::vector<SearchResult>> StorageReadInterface::IndexSearch(
    const std::string& unique_index_name,
    const IndexQueryParams& params) const {
  GS_AUTO(index, view_.GetIndexByName(unique_index_name));
  return index->Search(params);
}

/**
 * Creates an index for a vertex property.
 *
 * When creating an HNSW index, this method converts an ArrayColumn to a
 * VecColumn. The VecColumn reuses the ArrayColumn's underlying vector buffer
 * without copying its data. Subsequent incremental vector updates avoid
 * copy-on-write by letting the VecColumn maintain separate buffer versions.
 */
neug::result<CreatedIndex> CreateStorageIndex(
    PropertyGraph& graph, GraphView& view, timestamp_t timestamp,
    std::unique_ptr<IndexMeta> meta,
    IndexPlanningChangedCallback on_planning_changed, bool required) {
  if (!meta) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null metadata");
  }
  auto label_id = meta->schema.label_id;
  if (!graph.schema().is_vertex_label_valid(label_id)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Index label id is out of range");
  }

  auto& vertex_table = graph.get_vertex_table(label_id);
  const auto schema = vertex_table.get_vertex_schema_ptr();
  const auto& property_name = meta->schema.property_name;
  const bool is_primary_key =
      property_name == std::get<1>(schema->primary_keys[0]);
  const ColumnBase* column = vertex_table.GetPropertyColumnBase(property_name);
  if (!column) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Indexed property column does not exist: " + property_name);
  }

  int32_t property_col = -1;
  std::unique_ptr<ColumnBase> vec_column;
  std::unique_ptr<IndexIDAccessor> index_id_accessor;

  auto& index_manager = graph.mutable_index_manager();
  GS_AUTO(existing_indexes, index_manager.GetAllIndexes());
  GS_AUTO(pending_indexes,
          index_manager.GetPendingIndex(label_id, property_name));
  const auto active_matches = [&](StorageIndex* index) {
    const auto& existing_meta = index->GetMeta();
    return existing_meta.schema.label_id == label_id &&
           existing_meta.schema.property_name == property_name;
  };

  if (IsHNSWIndex(*meta)) {
    const bool has_non_hnsw =
        std::any_of(existing_indexes.begin(), existing_indexes.end(),
                    [&](StorageIndex* index) {
                      return active_matches(index) &&
                             !IsHNSWIndex(index->GetMeta());
                    }) ||
        std::any_of(pending_indexes.begin(), pending_indexes.end(),
                    [](const StorageIndexManager::PendingIndex* index) {
                      return !IsHNSWIndex(index->meta);
                    });
    if (has_non_hnsw) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "HNSW index cannot coexist with non-HNSW indexes on the same "
          "property");
    }
    if (is_primary_key) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "HNSW index cannot be created on a primary key");
    }
    property_col = schema->get_property_index(property_name);
    if (property_col < 0) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Indexed property does not exist: " + property_name);
    }

    if (const auto* array = dynamic_cast<const ArrayColumn*>(column)) {
      const auto& default_value = schema->default_property_values[property_col];
      vec_column = FromArrayColumn(*array, vertex_table.Size(), default_value,
                                   graph.checkpoint(), graph.memory_level());
      column = vec_column.get();
    }

    auto* candidate_column =
        vec_column ? vec_column.get()
                   : vertex_table.get_table().get_column_by_id(property_col);
    if (auto* vec = dynamic_cast<VecColumn*>(candidate_column)) {
      index_id_accessor = std::make_unique<VecColumnBackedIndexIDAccessor>(
          *vec->get_offset_accessor());
    } else {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "CreateIndex: HNSW index can only be created on VecColumn");
    }
  } else {
    const bool has_hnsw =
        std::any_of(existing_indexes.begin(), existing_indexes.end(),
                    [&](StorageIndex* index) {
                      return active_matches(index) &&
                             IsHNSWIndex(index->GetMeta());
                    }) ||
        std::any_of(pending_indexes.begin(), pending_indexes.end(),
                    [](const StorageIndexManager::PendingIndex* index) {
                      return IsHNSWIndex(index->meta);
                    });
    if (has_hnsw) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "Non-HNSW index cannot coexist with HNSW indexes on the same "
          "property");
    }
    if (dynamic_cast<const VecColumn*>(column)) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Non-HNSW index cannot be created on VecColumn");
    }
    index_id_accessor = std::make_unique<DefaultIndexIDAccessor>();
  }

  GS_AUTO(index, index_manager.CreateIndex(
                     std::move(meta), std::move(index_id_accessor), column,
                     graph.GetVertexSet(label_id, timestamp), required));

  if (on_planning_changed) {
    on_planning_changed();
  }
  if (vec_column) {
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(vec_column));
    graph.MarkVertexTableDirty(label_id);
    view.Rebuild(graph);
  }
  return index;
}

/**
 * Drops an index.
 *
 * When dropping the last HNSW index on a VecColumn, this method converts the
 * VecColumn back to an ArrayColumn. It creates a new ArrayColumn and copies
 * vectors from the VecColumn by vertex ID. This is equivalent to compaction:
 * obsolete vector versions addressed by previous index IDs are discarded.
 */
Status DropStorageIndex(PropertyGraph& graph, GraphView& view,
                        const std::string& name,
                        IndexPlanningChangedCallback on_planning_changed) {
  auto& index_manager = graph.mutable_index_manager();
  auto pending = index_manager.GetPendingIndexByName(name);
  const bool is_pending = pending.has_value();
  IndexMeta meta;
  if (is_pending) {
    meta = pending.value()->meta;
  } else {
    auto target = index_manager.GetIndexByName(name);
    if (!target) {
      return target.error();
    }
    meta = target.value()->GetMeta();
  }
  std::unique_ptr<ArrayColumn> array_column;
  int32_t property_col = -1;

  if (IsHNSWIndex(meta)) {
    bool has_other_hnsw = false;
    auto pending_indexes = index_manager.GetPendingIndex(
        meta.schema.label_id, meta.schema.property_name);
    if (!pending_indexes) {
      return pending_indexes.error();
    }
    has_other_hnsw = std::any_of(
        pending_indexes->begin(), pending_indexes->end(),
        [&](const StorageIndexManager::PendingIndex* index) {
          return index->meta.name != name && IsHNSWIndex(index->meta);
        });
    if (!has_other_hnsw) {
      auto indexes = index_manager.GetAllIndexes();
      if (!indexes) {
        return indexes.error();
      }
      has_other_hnsw = std::any_of(
          indexes->begin(), indexes->end(), [&](StorageIndex* index) {
            const auto& other_meta = index->GetMeta();
            return other_meta.name != name &&
                   other_meta.schema.label_id == meta.schema.label_id &&
                   other_meta.schema.property_name ==
                       meta.schema.property_name &&
                   IsHNSWIndex(other_meta);
          });
    }
    if (!has_other_hnsw) {
      auto& vertex_table = graph.get_vertex_table(meta.schema.label_id);
      const auto schema = vertex_table.get_vertex_schema_ptr();
      property_col = schema->get_property_index(meta.schema.property_name);
      if (property_col < 0) {
        return Status(
            StatusCode::ERR_INVALID_ARGUMENT,
            "Indexed property does not exist: " + meta.schema.property_name);
      }

      const auto& default_value = schema->default_property_values[property_col];
      auto* column = vertex_table.get_table().get_column_by_id(property_col);
      if (auto* vec = dynamic_cast<VecColumn*>(column)) {
        array_column = FromVecColumn(*vec, vertex_table.Size(),
                                     vertex_table.Capacity(), default_value,
                                     graph.checkpoint(), graph.memory_level());
      } else {
        return Status(StatusCode::ERR_INVALID_ARGUMENT,
                      "DropIndex: HNSW index can only be created on VecColumn");
      }
    }
  }

  RETURN_IF_NOT_OK(index_manager.DropIndex(name));
  if (on_planning_changed) {
    on_planning_changed();
  }
  if (array_column) {
    auto& vertex_table = graph.get_vertex_table(meta.schema.label_id);
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(array_column));
    graph.MarkVertexTableDirty(meta.schema.label_id);
    view.Rebuild(graph);
  }
  return Status::OK();
}

result<size_t> ActivateStorageIndexes(
    PropertyGraph& graph, GraphView& view,
    IndexPlanningChangedCallback on_planning_changed) {
  auto activated = graph.ActivateIndexes();
  if (!activated) {
    return activated.error();
  }
  if (activated.value() == 0) {
    return size_t{0};
  }
  view.Rebuild(graph);
  if (on_planning_changed) {
    on_planning_changed();
  }
  return activated.value();
}

}  // namespace neug
