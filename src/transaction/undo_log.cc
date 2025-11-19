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
void CreateVertexTypeUndo::Undo(PropertyGraph& graph) const {
  graph.DeleteVertexType(vertex_type);
};

void CreateEdgeTypeUndo::Undo(PropertyGraph& graph) const {
  graph.DeleteEdgeType(src_type, dst_type, edge_type);
};

void InsertVertexUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for InsertVertex is not implemented yet.");
};

void InsertEdgeUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for InsertEdge is not implemented yet.");
};

void UpdateVertexPropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for UpdateVertexProp is not implemented yet.");
};

void UpdateEdgePropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for UpdateEdgeProp is not implemented yet.");
};

void RemoveVertexUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RemoveVertex is not implemented yet.");
};

void RemoveEdgeUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RemoveEdge is not implemented yet.");
};

void AddVertexPropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for AddVertexProp is not implemented yet.");
};

void AddEdgePropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for AddEdgeProp is not implemented yet.");
};

void RenameVertexPropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RenameVertexProp is not implemented yet.");
};

void RenameEdgePropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for RenameEdgeProp is not implemented yet.");
};

void DeleteVertexPropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteVertexProp is not implemented yet.");
};

void DeleteEdgePropUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteEdgeProp is not implemented yet.");
};

void DeleteVertexTypeUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteVertexType is not implemented yet.");
};

void DeleteEdgeTypeUndo::Undo(PropertyGraph& graph) const {
  THROW_NOT_IMPLEMENTED_EXCEPTION(
      "Undo for DeleteEdgeType is not implemented yet.");
};

}  // namespace gs