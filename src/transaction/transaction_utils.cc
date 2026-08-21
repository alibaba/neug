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

#include "neug/transaction/transaction_utils.h"

#include <glog/logging.h>

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/graph/cow_detach_state.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {

Status dropVertexIndex(PropertyGraph& graph, label_t label,
                       const std::string& prop_name,
                       CowDetachState* detach_state) {
  auto& index_manager = graph.mutable_index_manager();
  auto indexes = index_manager.GetIndexForUpdate(label, prop_name);
  if (!indexes) {
    return indexes.error();
  }

  std::vector<std::string> index_names;
  index_names.reserve(indexes->size());
  for (auto* index : indexes.value()) {
    index_names.push_back(index->GetMeta().name);
  }
  for (const auto& index_name : index_names) {
    RETURN_IF_NOT_OK(index_manager.DropIndex(index_name));
    if (detach_state) {
      detach_state->index_detached.erase(index_name);
    }
  }
  return Status::OK();
}

Status renameVertexIndex(PropertyGraph& graph, label_t label,
                         const std::string& old_name,
                         const std::string& new_name) {
  auto indexes =
      graph.mutable_index_manager().GetIndexForUpdate(label, old_name);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    index->RenameProperty(new_name);
  }
  return Status::OK();
}

Status addVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                          const Value& id, const std::vector<Value>& props,
                          const IndexDetachFn& detach_index) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();

  // Primary keys are stored separately from property_names, so maintain their
  // indexes explicitly.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndexForUpdate(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  for (auto* index : pk_indexes.value()) {
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
    RETURN_IF_NOT_OK(index->Upsert(lid, id));
  }

  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx] || prop_idx >= props.size()) {
      continue;
    }
    auto indexes = index_manager.GetIndexForUpdate(
        label, v_schema->property_names[prop_idx]);
    if (!indexes) {
      return indexes.error();
    }
    for (auto* index : indexes.value()) {
      if (detach_index) {
        RETURN_IF_NOT_OK(detach_index(*index));
      }
      RETURN_IF_NOT_OK(index->Upsert(lid, props[prop_idx]));
    }
  }
  return Status::OK();
}

Status updateVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                             int32_t col_id, const Value& value,
                             const IndexDetachFn& detach_index) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= v_schema->property_names.size() ||
      v_schema->vprop_soft_deleted[col_id]) {
    return Status::OK();
  }

  auto indexes = graph.mutable_index_manager().GetIndexForUpdate(
      label, v_schema->property_names[col_id]);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
    RETURN_IF_NOT_OK(index->Upsert(lid, value));
  }
  return Status::OK();
}

Status deleteVertexIndexData(PropertyGraph& graph, label_t label,
                             const std::vector<vid_t>& vids,
                             const IndexDetachFn& detach_index) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();

  // Primary keys are excluded from property_names, so delete their index
  // entries explicitly.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndexForUpdate(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  for (auto* index : pk_indexes.value()) {
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
    for (vid_t vid : vids) {
      RETURN_IF_NOT_OK(index->Delete(vid));
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
    for (auto* index : indexes.value()) {
      if (detach_index) {
        RETURN_IF_NOT_OK(detach_index(*index));
      }
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Delete(vid));
      }
    }
  }
  return Status::OK();
}

void ReplayCowGraphWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                       size_t length, Allocator& alloc) {
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op_type;
    arc >> op_type;
    if (op_type == OpType::kCreateVertexType) {
      CreateVertexTypeParam redo = CreateVertexTypeRedo::Deserialize(arc);
      graph.MarkSchemaDirty();
      auto ret = graph.CreateVertexType(redo);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to create vertex type in redo: ",
                                     ret);
    } else if (op_type == OpType::kCreateEdgeType) {
      const auto& redo = CreateEdgeTypeRedo::Deserialize(arc);
      graph.MarkSchemaDirty();
      auto ret = graph.CreateEdgeType(redo);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to create edge type in redo: ",
                                     ret);
    } else if (op_type == OpType::kInsertVertex) {
      InsertVertexRedo redo;
      arc >> redo;
      vid_t vid;
      bool inserted = false;
      auto& v_table = graph.get_vertex_table(redo.label);
      if (!graph.get_lid(redo.label, redo.oid, vid, timestamp) ||
          !graph.IsValidLid(redo.label, vid, timestamp)) {
        if (v_table.Size() >= v_table.Capacity()) {
          auto new_capacity = v_table.Size() < 4096
                                  ? 4096
                                  : v_table.Size() + v_table.Size() / 4;
          graph.EnsureCapacity(redo.label, new_capacity);
        }
        graph.MarkVertexTableDirty(redo.label);
        auto ret = graph.AddVertex(redo.label, redo.oid, redo.props, vid,
                                   timestamp, true);
        THROW_STORAGE_EXCEPTION_STATUS("Failed to add vertex in redo: ", ret);
        inserted = true;
      }
      if (inserted) {
        auto& index_manager = graph.mutable_index_manager();
        Status ret;
        if (index_manager.HasPendingIndex(redo.label)) {
          const auto& v_schema = graph.schema().get_vertex_schema(redo.label);
          std::vector<std::pair<std::string, Value>> properties;
          properties.emplace_back(std::get<1>(v_schema->primary_keys[0]),
                                  redo.oid);
          for (size_t prop_idx = 0;
               prop_idx < v_schema->property_names.size() &&
               prop_idx < redo.props.size();
               ++prop_idx) {
            if (!v_schema->vprop_soft_deleted[prop_idx]) {
              properties.emplace_back(v_schema->property_names[prop_idx],
                                      redo.props[prop_idx]);
            }
          }
          index_manager.RecordPendingInsert(redo.label, vid,
                                            std::move(properties));
        } else {
          ret =
              addVertexIndexData(graph, redo.label, vid, redo.oid, redo.props);
        }
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to append vertex indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kInsertEdge) {
      InsertEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      int32_t oe_offset_unused = 0;
      const void* prop_unused = nullptr;
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.AddEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                               redo.edge_label, redo.properties, timestamp,
                               alloc, oe_offset_unused, prop_unused, true);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to add edge in redo: ", ret);
    } else if (op_type == OpType::kUpdateVertexProp) {
      UpdateVertexPropRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.MarkVertexTableDirty(redo.label);
      auto ret = graph.UpdateVertexProperty(redo.label, vid, redo.prop_id,
                                            redo.value, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to update vertex property in redo: ", ret);
      const auto& v_schema = graph.schema().get_vertex_schema(redo.label);
      if (redo.prop_id >= 0 &&
          static_cast<size_t>(redo.prop_id) < v_schema->property_names.size() &&
          !v_schema->vprop_soft_deleted[redo.prop_id] &&
          graph.mutable_index_manager().HasPendingIndex(
              redo.label, v_schema->property_names[redo.prop_id])) {
        graph.mutable_index_manager().RecordPendingUpdate(
            redo.label, vid, v_schema->property_names[redo.prop_id],
            redo.value);
      } else {
        ret = updateVertexIndexData(graph, redo.label, vid, redo.prop_id,
                                    redo.value);
      }
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to update vertex property indexes in redo: ", ret);
    } else if (op_type == OpType::kUpdateEdgeProp) {
      UpdateEdgePropRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.UpdateEdgeProperty(
          redo.src_label, src_vid, redo.dst_label, dst_vid, redo.edge_label,
          redo.oe_offset, redo.ie_offset, redo.prop_id, redo.value, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to update edge property in redo: ",
                                     ret);
    } else if (op_type == OpType::kRemoveVertex) {
      RemoveVertexRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.MarkVertexTableDirty(redo.label);
      // Cascade: DeleteVertex physically writes incident edge tables.
      for (const auto& [_, es] : graph.schema().get_all_edge_schemas()) {
        if (es->src_label_id == redo.label || es->dst_label_id == redo.label) {
          graph.MarkEdgeTableDirty(es->src_label_id, es->dst_label_id,
                                   es->edge_label_id);
        }
      }
      auto ret = graph.DeleteVertex(redo.label, vid, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete vertex in redo: ", ret);
      if (graph.mutable_index_manager().HasPendingIndex(redo.label)) {
        graph.mutable_index_manager().RecordPendingDelete(redo.label, vid);
      } else {
        ret = deleteVertexIndexData(graph, redo.label, {vid});
      }
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete vertex indexes in redo: ", ret);
    } else if (op_type == OpType::kRemoveEdge) {
      RemoveEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.DeleteEdge(redo.src_label, src_vid, redo.dst_label,
                                  dst_vid, redo.edge_label, redo.oe_offset,
                                  redo.ie_offset, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete edge in redo: ", ret);
    } else if (op_type == OpType::kAddVertexProp) {
      auto redo = AddVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.AddVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to add vertex properties in redo: ", ret);
    } else if (op_type == OpType::kAddEdgeProp) {
      auto redo = AddEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.AddEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to add edge properties in redo: ",
                                     ret);
    } else if (op_type == OpType::kRenameVertexProp) {
      auto redo = RenameVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.RenameVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to rename vertex properties in redo: ", ret);
      for (const auto& [old_name, new_name] :
           redo.config.GetRenameProperties()) {
        if (old_name == new_name) {
          continue;
        }
        ret = renameVertexIndex(graph, label, old_name, new_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to rename vertex index metadata in redo: ", ret);
      }
    } else if (op_type == OpType::kRenameEdgeProp) {
      auto redo = RenameEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.RenameEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to rename edge properties in redo: ", ret);
    } else if (op_type == OpType::kDeleteVertexProp) {
      auto redo = DeleteVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.DeleteVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete vertex properties in redo: ", ret);
      for (const auto& prop_name : redo.config.GetDeleteProperties()) {
        ret = dropVertexIndex(graph, label, prop_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to drop deleted-property indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kDeleteEdgeProp) {
      auto redo = DeleteEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.DeleteEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete edge properties in redo: ", ret);
    } else if (op_type == OpType::kDeleteVertexType) {
      DeleteVertexTypeRedo redo;
      arc >> redo;
      graph.MarkSchemaDirty();
      auto v_label = graph.schema().get_vertex_label_id(redo.vertex_type);
      const auto& v_schema = graph.schema().get_vertex_schema(v_label);
      std::vector<std::string> indexed_properties;
      indexed_properties.reserve(v_schema->property_names.size() + 1);
      indexed_properties.push_back(std::get<1>(v_schema->primary_keys[0]));
      for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
           ++prop_idx) {
        if (v_schema->vprop_soft_deleted[prop_idx]) {
          continue;
        }
        indexed_properties.push_back(v_schema->property_names[prop_idx]);
      }
      auto ret = graph.DeleteVertexType(redo.vertex_type);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete vertex type in redo: ",
                                     ret);
      for (const auto& property_name : indexed_properties) {
        ret = dropVertexIndex(graph, v_label, property_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to drop deleted-vertex-type indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kDeleteEdgeType) {
      DeleteEdgeTypeRedo redo;
      arc >> redo;
      graph.MarkSchemaDirty();
      auto ret =
          graph.DeleteEdgeType(redo.src_type, redo.dst_type, redo.edge_type);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete edge type in redo: ",
                                     ret);
    } else if (op_type == OpType::kAddGraphEntry) {
      auto redo = AddGraphEntryRedo::Deserialize(arc);
      auto ret = graph.mutable_schema().AddGraphEntry(redo.name, redo.entry);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to replay projected graph add: ",
                                     ret);
      graph.MarkSchemaDirty();
    } else if (op_type == OpType::kDropGraphEntry) {
      auto redo = DropGraphEntryRedo::Deserialize(arc);
      auto ret = graph.mutable_schema().DropGraphEntry(redo.name);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to replay projected graph drop: ",
                                     ret);
      graph.MarkSchemaDirty();
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unexpected op_type: " +
                                    std::to_string(static_cast<int>(op_type)));
    }
  }
}

}  // namespace neug
