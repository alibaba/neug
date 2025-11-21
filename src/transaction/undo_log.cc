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

#include "neug/transaction/undo_log.h"

namespace gs {
void CreateVertexTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  graph.DeleteVertexType(vertex_type);
};

void CreateEdgeTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  graph.DeleteEdgeType(src_type, dst_type, edge_type);
};

void InsertVertexUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.DeleteVertex(v_label, vid, ts);
};

void InsertEdgeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for InsertEdge is not implemented yet.");
};

void UpdateVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.UpdateVertexProperty(v_label, vid, col_id, value, ts);
};

void UpdateEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for UpdateEdgeProp is not implemented yet.");
};

void RemoveVertexUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.get_vertex_table(v_label).RevertDeleteVertex(lid, ts);
};

void RemoveEdgeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RemoveEdge is not implemented yet.");
};

void AddVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for AddVertexProp is not implemented yet.");
};

void AddEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for AddEdgeProp is not implemented yet.");
};

void RenameVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RenameVertexProp is not implemented yet.");
};

void RenameEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RenameEdgeProp is not implemented yet.");
};

void DeleteVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteVertexProp is not implemented yet.");
};

void DeleteEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteEdgeProp is not implemented yet.");
};

void DeleteVertexTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (graph.schema().contains_vertex_label(v_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteVertexType for vertex label " +
                        v_label + " since it does  exist.");
  }
  if (!graph.schema().IsVertexLabelSoftDeleted(v_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteVertexType for vertex label " +
                        v_label + " since it is not soft deleted.");
  }
  graph.mutable_schema().RevertDeleteVertexLabel(v_label);
  // No need to do with the vertex tables and edge tables, they are only marked
  // as deleted in txn.
};

void DeleteEdgeTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (graph.schema().exist(src_label, dst_label, edge_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteEdgeType for edge label " +
                        edge_label + " since it exists.");
  }
  if (!graph.schema().IsEdgeLabelSoftDeleted(src_label, dst_label,
                                             edge_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteEdgeType for edge label " +
                        edge_label + " since it is not soft deleted.");
  }
  graph.mutable_schema().RevertDeleteEdgeLabel(src_label, dst_label,
                                               edge_label);
  // No need to do with the vertex tables and edge tables, they are only marked
  // as deleted in txn.
};

}  // namespace gs