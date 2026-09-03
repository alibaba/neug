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
#include <string_view>

#include <glog/logging.h>

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
    if (offset >= vec.buffer_size()) {
      THROW_RUNTIME_ERROR("FromVecColumn: offset out of range");
    }
    std::memcpy(dst + vid * array_size, src + offset * array_size,
                array_size * sizeof(float));
  }
  return array_column;
}

// Drops every index whose metadata references the given vertex label/property.
static Status dropVertexIndex(PropertyGraph& graph, label_t label,
                              const std::string& prop_name) {
  auto& index_manager = graph.mutable_index_manager();
  auto indexes = index_manager.GetIndexesContainingProperty(label, prop_name);
  if (!indexes) {
    return indexes.error();
  }

  std::vector<std::string> index_names;
  index_names.reserve(indexes->size());
  for (const auto& binding : indexes.value()) {
    index_names.push_back(binding.index->GetMeta().name);
  }
  for (const auto& index_name : index_names) {
    RETURN_IF_NOT_OK(index_manager.DropIndex(index_name));
  }
  return Status::OK();
}

// Updates metadata for every index bound to a renamed vertex property.
static Status renameVertexIndex(PropertyGraph& graph, label_t label,
                                const std::string& old_name,
                                const std::string& new_name) {
  auto indexes =
      graph.mutable_index_manager().GetIndexesContainingPropertyForUpdate(
          label, old_name);
  if (!indexes) {
    return indexes.error();
  }
  for (const auto& binding : indexes.value()) {
    binding.index->RenameProperty(old_name, new_name);
  }
  return Status::OK();
}

// Appends index entries for one newly inserted vertex row.
static Status addVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                                 const Value& id,
                                 const std::vector<Value>& props) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();

  // Build every indexed tuple in metadata order. Primary keys are stored
  // separately from regular properties, so read the supplied id explicitly.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto indexes = index_manager.GetIndexesForUpdate(label);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    IndexValues values;
    for (const auto& column : index->GetMeta().schema.columns) {
      if (column.property_name == pk_name) {
        values.push_back(id);
        continue;
      }
      auto it = std::find(v_schema->property_names.begin(),
                          v_schema->property_names.end(), column.property_name);
      if (it == v_schema->property_names.end()) {
        return Status::InternalError("Indexed property does not exist");
      }
      auto pos = static_cast<size_t>(
          std::distance(v_schema->property_names.begin(), it));
      values.push_back(pos < props.size() ? props[pos] : Value());
    }
    RETURN_IF_NOT_OK(index->Upsert(lid, values));
  }
  return Status::OK();
}

// Appends index entries for a batch of newly inserted vertex rows.
static Status batchAddVertexIndexData(PropertyGraph& graph, label_t label,
                                      const std::vector<vid_t>& vids) {
  const auto& vtable = graph.get_vertex_table(label);
  auto& index_manager = graph.mutable_index_manager();

  // Resolve all columns by name so the same path handles regular properties
  // and the primary-key column maintained by the vertex table.
  auto indexes = index_manager.GetIndexesForUpdate(label);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    std::vector<const ColumnBase*> columns;
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
      for (const auto* column : columns) {
        values.push_back(column->get_any(vid));
      }
      RETURN_IF_NOT_OK(index->Upsert(vid, values));
    }
  }
  return Status::OK();
}

// Updates index entries for one changed vertex property. Primary keys are not
// handled here because PropertyGraph does not allow modifying the primary key
// of an existing vertex.
static Status updateVertexIndexData(PropertyGraph& graph, label_t label,
                                    vid_t lid, int32_t col_id,
                                    const Value& value) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= v_schema->property_names.size() ||
      v_schema->vprop_soft_deleted[col_id]) {
    return Status::OK();
  }

  auto& index_manager = graph.mutable_index_manager();
  auto indexes = index_manager.GetIndexesContainingPropertyForUpdate(
      label, v_schema->property_names[col_id]);
  if (!indexes) {
    return indexes.error();
  }
  for (const auto& binding : indexes.value()) {
    RETURN_IF_NOT_OK(
        binding.index->Upsert(lid, IndexValue{binding.column_id, value}));
  }
  return Status::OK();
}

// Deletes index entries for one or more removed vertex rows.
static Status deleteVertexIndexData(PropertyGraph& graph, label_t label,
                                    const std::vector<vid_t>& vids) {
  auto& index_manager = graph.mutable_index_manager();

  auto indexes = index_manager.GetIndexesForUpdate(label);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    for (vid_t vid : vids) {
      RETURN_IF_NOT_OK(index->Delete(vid));
    }
  }
  return Status::OK();
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
  if (meta->schema.columns.empty()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Index must bind at least one property");
  }
  std::vector<const ColumnBase*> columns;
  std::vector<std::string> property_names;
  columns.reserve(meta->schema.columns.size());
  property_names.reserve(meta->schema.columns.size());
  for (const auto& index_column : meta->schema.columns) {
    const auto* column =
        vertex_table.GetPropertyColumnBase(index_column.property_name);
    if (!column) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Indexed property column does not exist: " +
                              index_column.property_name);
    }
    columns.push_back(column);
    property_names.push_back(index_column.property_name);
  }

  int32_t property_col = -1;
  std::unique_ptr<ColumnBase> vec_column;
  std::unique_ptr<IndexIDAccessor> index_id_accessor;

  auto& index_manager = graph.mutable_index_manager();

  if (IsHNSWIndex(*meta)) {
    if (meta->schema.columns.size() != 1) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "HNSW index requires exactly one property");
    }
    const auto& property_name = meta->schema.columns[0].property_name;
    const bool is_primary_key =
        property_name == std::get<1>(schema->primary_keys[0]);
    GS_AUTO(existing_indexes, index_manager.GetIndexesContainingProperty(
                                  label_id, property_name));
    GS_AUTO(pending_indexes, index_manager.GetPendingIndexContainingProperty(
                                 label_id, property_name));
    const bool cosine_normalize = ParseCosineNormalizeOption(*meta);
    const bool has_non_hnsw =
        std::any_of(existing_indexes.begin(), existing_indexes.end(),
                    [](const BoundIndexRef& binding) {
                      return !IsHNSWIndex(binding.index->GetMeta());
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

    if (const auto* array = dynamic_cast<const ArrayColumn*>(columns[0])) {
      const auto& default_value = schema->default_property_values[property_col];
      vec_column = FromArrayColumn(*array, vertex_table.Size(), default_value,
                                   graph.checkpoint(), graph.memory_level());
      columns[0] = vec_column.get();
    }

    auto* candidate_column =
        vec_column ? vec_column.get()
                   : vertex_table.get_table().get_column_by_id(property_col);
    if (auto* vec = dynamic_cast<VecColumn*>(candidate_column)) {
      if (cosine_normalize && !vec->is_l2_normalized()) {
        const bool has_raw_hnsw =
            std::any_of(
                existing_indexes.begin(), existing_indexes.end(),
                [](const BoundIndexRef& binding) {
                  return IsHNSWIndex(binding.index->GetMeta()) &&
                         !UsesCosineNormalization(binding.index->GetMeta());
                }) ||
            std::any_of(pending_indexes.begin(), pending_indexes.end(),
                        [](const StorageIndexManager::PendingIndex* index) {
                          return IsHNSWIndex(index->meta) &&
                                 !UsesCosineNormalization(index->meta);
                        });
        if (has_raw_hnsw) {
          RETURN_STATUS_ERROR(
              StatusCode::ERR_INVALID_ARGUMENT,
              "Cannot normalize a vector property used by an existing "
              "HNSW index built from raw vectors");
        }
        auto status = vec->EnsureL2Normalized();
        if (!status.ok()) {
          RETURN_ERROR(status);
        }
      } else if (!cosine_normalize && IsCosineMetric(*meta) &&
                 !vec->SampleIsL2Normalized()) {
        RETURN_STATUS_ERROR(
            StatusCode::ERR_INVALID_ARGUMENT,
            "Cosine HNSW requires L2-normalized vectors; specify "
            "cosine_normalize = true or normalize the property data before "
            "creating the index");
      }
      index_id_accessor = std::make_unique<VecColumnBackedIndexIDAccessor>(
          *vec->get_offset_accessor());
    } else {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "CreateIndex: HNSW index can only be created on VecColumn");
    }
  } else {
    for (size_t i = 0; i < columns.size(); ++i) {
      const auto* column = columns[i];
      GS_AUTO(existing_indexes,
              index_manager.GetIndex(label_id, {property_names[i]}));
      GS_AUTO(pending_indexes,
              index_manager.GetPendingIndex(label_id, {property_names[i]}));
      const bool has_hnsw =
          std::any_of(existing_indexes.begin(), existing_indexes.end(),
                      [](const StorageIndex* index) {
                        return IsHNSWIndex(index->GetMeta());
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
    }
    index_id_accessor = std::make_unique<DefaultIndexIDAccessor>();
  }

  GS_AUTO(index,
          index_manager.CreateIndex(
              std::move(meta), std::move(index_id_accessor), std::move(columns),
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
    const auto& property_name = meta.schema.columns[0].property_name;
    bool has_other_hnsw = false;
    auto pending_indexes =
        index_manager.GetPendingIndex(meta.schema.label_id, {property_name});
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
                   other_meta.schema.columns.size() == 1 &&
                   other_meta.schema.columns[0].property_name ==
                       property_name &&
                   IsHNSWIndex(other_meta);
          });
    }
    if (!has_other_hnsw) {
      auto& vertex_table = graph.get_vertex_table(meta.schema.label_id);
      const auto schema = vertex_table.get_vertex_schema_ptr();
      property_col = schema->get_property_index(property_name);
      if (property_col < 0) {
        return Status(StatusCode::ERR_INVALID_ARGUMENT,
                      "Indexed property does not exist: " + property_name);
      }

      const auto& default_value = schema->default_property_values[property_col];
      auto* column = vertex_table.get_table().get_column_by_id(property_col);
      if (auto* vec = dynamic_cast<VecColumn*>(column)) {
        if (vec->is_l2_normalized()) {
          LOG(WARNING)
              << "Dropping the last HNSW index on L2-normalized property '"
              << property_name
              << "' converts it back to an ArrayColumn. Existing normalized "
                 "values remain irreversible, while subsequent writes will "
                 "not be normalized automatically.";
        }
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
