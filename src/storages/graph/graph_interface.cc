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

#include "neug/storages/index/index_manager.h"

namespace neug {

namespace {

static LabelEntry make_vertex_label(label_t label_id) {
  return LabelEntry{EntryType::VERTEX, label_id, 0, 0};
}

static LabelEntry make_edge_label(label_t edge_label_id) {
  return LabelEntry{EntryType::EDGE, edge_label_id, 0, 0};
}

static Status drop_indexes_for_property(IndexManager& index_manager,
                                        label_t label_id,
                                        const std::string& property_name) {
  std::vector<Index*> indexes;
  RETURN_IF_NOT_OK(index_manager.GetIndex(make_vertex_label(label_id),
                                          {property_name}, indexes));
  for (auto* idx : indexes) {
    RETURN_IF_NOT_OK(index_manager.DropIndex(idx->GetMeta().name));
  }
  return Status::OK();
}

}  // namespace

Status StorageAPUpdateInterface::UpdateVertexProperty(
    label_t label, vid_t lid, int col_id, const execution::Value& value) {
  RETURN_IF_NOT_OK(
      graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_));

  const auto& v_schema = graph_.schema().get_vertex_schema(label);
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= v_schema->property_names.size()) {
    return Status::OK();
  }
  if (v_schema->vprop_soft_deleted[col_id]) {
    return Status::OK();
  }
  const auto& prop_name = v_schema->property_names[col_id];
  std::vector<Index*> indexes;
  index_manager_.GetIndex(make_vertex_label(label), {prop_name}, indexes);
  for (auto* idx : indexes) {
    idx->Delete(lid);
    idx->Append(lid, {value});
  }
  return Status::OK();
}

Status StorageAPUpdateInterface::UpdateEdgeProperty(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const execution::Value& value) {
  const auto& e_schema =
      graph_.schema().get_edge_schema(src_label, dst_label, edge_label);
  for (const auto& prop_name : e_schema->property_names) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " +
                          graph_.schema().get_edge_label_name(edge_label));
      }
    }
  }
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   neug::timestamp_t(0));
}

Status StorageAPUpdateInterface::AddVertex(
    label_t label, const execution::Value& id,
    const std::vector<execution::Value>& props, vid_t& vid) {
  const auto& vertex_table = graph_.get_vertex_table(label);
  if (vertex_table.Size() >= vertex_table.Capacity()) {
    auto new_cap = vertex_table.Size() < 4096
                       ? 4096
                       : vertex_table.Size() + vertex_table.Size() / 4;
    auto status = graph_.EnsureCapacity(label, new_cap);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for vertex of label "
                 << graph_.schema().get_vertex_label_name(label) << ": "
                 << status.ToString();
      return status;
    }
  }

  auto status =
      graph_.AddVertex(label, id, props, vid, neug::timestamp_t(0), true);
  if (!status.ok()) {
    LOG(ERROR) << "AddVertex failed: " << status.ToString();
    return status;
  }

  auto label_entry = make_vertex_label(label);
  const auto& v_schema = graph_.schema().get_vertex_schema(label);
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx])
      continue;
    if (prop_idx >= props.size())
      continue;
    const auto& prop_name = v_schema->property_names[prop_idx];
    std::vector<Index*> indexes;
    index_manager_.GetIndex(label_entry, {prop_name}, indexes);
    for (auto* idx : indexes) {
      idx->Append(vid, {props[prop_idx]});
    }
  }
  return status;
}

Status StorageAPUpdateInterface::AddEdge(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, const std::vector<execution::Value>& properties,
    const void*& prop) {
  const auto& edge_table =
      graph_.get_edge_table(src_label, dst_label, edge_label);
  if (edge_table.PropTableSize() >= edge_table.Capacity()) {
    size_t cur_size = edge_table.PropTableSize();
    auto new_cap = cur_size < 4096 ? 4096 : cur_size + cur_size / 4;
    auto status =
        graph_.EnsureCapacity(src_label, dst_label, edge_label, new_cap);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for edge of label "
                 << graph_.schema().get_edge_label_name(edge_label) << ": "
                 << status.ToString();
      return status;
    }
  }
  int32_t oe_offset = 0;
  auto status =
      graph_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                     neug::timestamp_t(0), alloc_, oe_offset, prop, true);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add edge: " << status.ToString();
  }
  return status;
}

void StorageAPUpdateInterface::CreateCheckpoint() {
  graph_.Dump();
  // Dump(reopen=true) clears and re-opens the graph, replacing all vertex/edge
  // tables.  Rebuild the view so cached pointers stay valid.
  mut_view_.Rebuild(graph_);
}

Status StorageAPUpdateInterface::DeleteVertex(label_t label, vid_t lid) {
  return graph_.DeleteVertex(label, lid, timestamp_);
}

Status StorageAPUpdateInterface::DeleteEdge(label_t src_label, vid_t src,
                                            label_t dst_label, vid_t dst,
                                            label_t edge_label,
                                            int32_t oe_offset,
                                            int32_t ie_offset) {
  return graph_.DeleteEdge(src_label, src, dst_label, dst, edge_label,
                           oe_offset, ie_offset, timestamp_);
}

Status StorageAPUpdateInterface::DeleteEdges(label_t src_label, vid_t src,
                                             label_t dst_label, vid_t dst,
                                             label_t edge_label) {
  // AP mode: delegate to batch version with single pair
  std::vector<std::tuple<vid_t, vid_t>> edges = {{src, dst}};
  return graph_.BatchDeleteEdges(src_label, dst_label, edge_label, edges);
}

Status StorageAPUpdateInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
  std::vector<vid_t> new_vids;
  RETURN_IF_NOT_OK(
      graph_.BatchAddVertices(v_label_id, std::move(supplier), new_vids));

  if (new_vids.empty()) {
    return Status::OK();
  }

  auto label_entry = make_vertex_label(v_label_id);
  const auto& v_schema = graph_.schema().get_vertex_schema(v_label_id);
  const auto& vtable = graph_.get_vertex_table(v_label_id);

  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx]) {
      continue;
    }
    const auto& prop_name = v_schema->property_names[prop_idx];
    std::vector<Index*> indexes;
    index_manager_.GetIndex(label_entry, {prop_name}, indexes);
    if (indexes.empty()) {
      continue;
    }
    auto col = vtable.GetPropertyColumn(static_cast<int32_t>(prop_idx));
    if (!col) {
      continue;
    }
    for (auto* idx : indexes) {
      for (vid_t vid : new_vids) {
        auto prop_value = col->get_any(vid);
        idx->Append(vid, {prop_value});
      }
    }
  }

  return Status::OK();
}

Status StorageAPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  const auto& e_schema =
      graph_.schema().get_edge_schema(src_label, dst_label, edge_label);
  for (const auto& prop_name : e_schema->property_names) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " +
                          graph_.schema().get_edge_label_name(edge_label));
      }
    }
  }
  return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                              std::move(supplier));
}

Status StorageAPUpdateInterface::BatchDeleteVertices(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  auto label_entry = make_vertex_label(v_label_id);
  const auto& v_schema = graph_.schema().get_vertex_schema(v_label_id);
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx])
      continue;
    const auto& prop_name = v_schema->property_names[prop_idx];
    std::vector<Index*> indexes;
    index_manager_.GetIndex(label_entry, {prop_name}, indexes);
    for (auto* idx : indexes) {
      for (vid_t vid : vids) {
        idx->Delete(vid);
      }
    }
  }
  return graph_.BatchDeleteVertices(v_label_id, vids);
}

Status StorageAPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  const auto& e_schema = graph_.schema().get_edge_schema(
      src_v_label_id, dst_v_label_id, edge_label_id);
  for (const auto& prop_name : e_schema->property_names) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label_id), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " +
                          graph_.schema().get_edge_label_name(edge_label_id));
      }
    }
  }
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 edges);
}

Status StorageAPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  const auto& e_schema = graph_.schema().get_edge_schema(
      src_v_label_id, dst_v_label_id, edge_label_id);
  for (const auto& prop_name : e_schema->property_names) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label_id), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " +
                          graph_.schema().get_edge_label_name(edge_label_id));
      }
    }
  }
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 oe_edges, ie_edges);
}

Status StorageAPUpdateInterface::CreateVertexType(
    const CreateVertexTypeParam& config) {
  auto status = graph_.CreateVertexType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::CreateEdgeType(
    const CreateEdgeTypeParam& config) {
  auto status = graph_.CreateEdgeType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddVertexProperties(
    const AddVertexPropertiesParam& config) {
  auto status = graph_.AddVertexProperties(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddEdgeProperties(
    const AddEdgePropertiesParam& config) {
  auto status = graph_.AddEdgeProperties(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::RenameVertexProperties(
    const RenameVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    RETURN_IF_NOT_OK(
        drop_indexes_for_property(index_manager_, v_label, old_name));
  }
  return graph_.RenameVertexProperties(config);
}

Status StorageAPUpdateInterface::RenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  const auto& edge_label_name = config.GetEdgeLabel();
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_label_name);
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label_id), {old_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " + edge_label_name);
      }
    }
  }
  return graph_.RenameEdgeProperties(config);
}

Status StorageAPUpdateInterface::DeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  for (const auto& prop_name : config.GetDeleteProperties()) {
    RETURN_IF_NOT_OK(
        drop_indexes_for_property(index_manager_, v_label, prop_name));
  }
  auto status = graph_.DeleteVertexProperties(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteEdgeProperties(
    const DeleteEdgePropertiesParam& config) {
  const auto& edge_label_name = config.GetEdgeLabel();
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_label_name);
  for (const auto& prop_name : config.GetDeleteProperties()) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label_id), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " + edge_label_name);
      }
    }
  }
  auto status = graph_.DeleteEdgeProperties(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteVertexType(
    const std::string& vertex_type_name) {
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  const auto& v_schema = graph_.schema().get_vertex_schema(v_label);
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx])
      continue;
    RETURN_IF_NOT_OK(drop_indexes_for_property(
        index_manager_, v_label, v_schema->property_names[prop_idx]));
  }
  auto status = graph_.DeleteVertexType(vertex_type_name);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteEdgeType(const std::string& src_type,
                                                const std::string& dst_type,
                                                const std::string& edge_type) {
  const auto& prop_names =
      graph_.schema().get_edge_property_names(src_type, dst_type, edge_type);
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  for (const auto& prop_name : prop_names) {
    std::vector<Index*> edge_indexes;
    index_manager_.GetIndex(make_edge_label(edge_label_id), {prop_name},
                            edge_indexes);
    for (auto* idx : edge_indexes) {
      if (idx->GetMeta().schema.label.type == EntryType::EDGE) {
        return Status(StatusCode::ERR_NOT_SUPPORTED,
                      "Edge indexes are not supported: " + edge_type);
      }
    }
  }
  auto status = graph_.DeleteEdgeType(src_type, dst_type, edge_type);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageReadInterface::GetIndex(
    const LabelEntry& label_entry,
    const std::vector<std::string>& property_names,
    std::vector<Index*>& target_indexes) const {
  return view_.index_manager().GetIndex(label_entry, property_names,
                                        target_indexes);
}

Index* StorageReadInterface::GetIndexByName(const std::string& name) const {
  return view_.index_manager().GetIndexByName(name);
}

Status StorageReadInterface::GetAllIndexes(
    std::vector<Index*>& target_indexes) const {
  return view_.index_manager().GetAllIndexes(target_indexes);
}

neug::result<Index*> StorageAPUpdateInterface::CreateIndex(
    const std::string& name, std::unique_ptr<IndexMeta> meta) {
  return index_manager_.CreateIndex(name, std::move(meta));
}

Status StorageAPUpdateInterface::DropIndex(const std::string& name) {
  return index_manager_.DropIndex(name);
}

}  // namespace neug
