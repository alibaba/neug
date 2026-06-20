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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/function/gds/gds_algo_function.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

struct PlainPredecessorAccessor {
  const vid_t* data;
  vid_t get(vid_t v) const { return data[v]; }
};

// Configure path encoding mode based on path_properties option
// - "lightweight" (default): only _ID, _LABEL, PK for vertices; _ID, _LABEL, _SRC_ID, _DST_ID for edges
// - "full": all properties encoded
inline void configure_path_encoding(const std::string& path_properties) {
  if (path_properties == "full") {
    execution::set_path_full_encoding(true);
  } else {
    // Default to lightweight mode
    execution::set_path_full_encoding(false);
  }
}

// Build the proper DataType for a GDS path output column.  NeuG's type
// converter expects kPath to carry a StructTypeInfo with _NODES (list of
// vertex) and _RELS (list of edge) fields, matching what the Cypher pattern
// matcher produces.  A bare DataTypeId::kPath crashes the converter.
inline common::DataType buildPathDataType() {
  auto nodeType = common::DataType(common::DataTypeId::kVertex);
  auto relType = common::DataType(common::DataTypeId::kEdge);

  auto nodesListType = common::DataType::List(nodeType.copy());
  auto relsListType = common::DataType::List(relType.copy());

  std::vector<std::string> fieldNames;
  fieldNames.push_back(common::InternalKeyword::NODES);
  fieldNames.push_back(common::InternalKeyword::RELS);

  std::vector<common::DataType> fieldTypes;
  fieldTypes.push_back(std::move(nodesListType));
  fieldTypes.push_back(std::move(relsListType));

  return common::DataType(
      common::DataTypeId::kPath,
      std::make_shared<common::StructTypeInfo>(std::move(fieldNames),
                                                std::move(fieldTypes)));
}

// Wrap the TableFunction::bindFunc to patch path column expressions with
// the proper Struct-wrapped DataType.  Without this, the type converter
// crashes on bare kPath during physical plan generation.
inline void wrapTableBindFuncWithPathFix(function::GDSAlgoFunction* func) {
  auto* tableFunc = static_cast<function::TableFunction*>(func);
  auto originalBind = tableFunc->bindFunc;
  tableFunc->bindFunc = [originalBind](main::ClientContext* ctx,
                                       const function::TableFuncBindInput* input)
      -> std::unique_ptr<function::TableFuncBindData> {
    auto result = originalBind(ctx, input);
    auto pathType = buildPathDataType();
    for (auto& col : result->columns) {
      if (col->getDataType().id() == common::DataTypeId::kPath) {
        col->dataType = pathType.copy();
      }
    }
    return result;
  };
}

// Build a Path object from a predecessor chain, looking up real edge data
// pointers from the CSR graph view.  The caller provides the vertex chain in
// source-to-target order.
inline execution::Path build_path_from_chain(
    const std::vector<vid_t>& chain, label_t vertex_label, label_t edge_label,
    bool directed, const StorageReadInterface& graph) {
  if (chain.size() <= 1) {
    return execution::Path(vertex_label, chain[0]);
  }

  auto oe_view = graph.GetGenericOutgoingGraphView(vertex_label, vertex_label,
                                                    edge_label);
  auto ie_view = graph.GetGenericIncomingGraphView(vertex_label, vertex_label,
                                                    edge_label);

  std::vector<std::pair<execution::Direction, const void*>> edge_datas;
  edge_datas.reserve(chain.size() - 1);

  for (size_t i = 0; i + 1 < chain.size(); ++i) {
    vid_t from = chain[i];
    vid_t to = chain[i + 1];
    const void* prop = nullptr;
    execution::Direction dir = execution::Direction::kOut;

    // Try outgoing edges first
    auto oe_edges = oe_view.get_edges(from);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      if (*it == to) {
        prop = it.get_data_ptr();
        break;
      }
    }

    // For undirected graphs, try incoming edges if not found in outgoing
    if (prop == nullptr && !directed) {
      auto ie_edges = ie_view.get_edges(from);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        if (*it == to) {
          prop = it.get_data_ptr();
          dir = execution::Direction::kIn;
          break;
        }
      }
    }

    edge_datas.push_back({dir, prop});
  }

  return execution::Path(vertex_label, edge_label, chain, edge_datas);
}

// Reconstruct a path from the predecessor array by walking backward from
// `target` to `source`, then build a Path with real edge data.
template <typename PredAccessor>
inline execution::Path reconstruct_path(vid_t target, vid_t source,
                                        const PredAccessor& pred,
                                        label_t vertex_label,
                                        label_t edge_label, bool directed,
                                        const StorageReadInterface& graph) {
  std::vector<vid_t> chain;
  vid_t cur = target;
  while (cur != source) {
    chain.push_back(cur);
    cur = pred.get(cur);
  }
  chain.push_back(source);
  std::reverse(chain.begin(), chain.end());
  return build_path_from_chain(chain, vertex_label, edge_label, directed,
                               graph);
}

}  // namespace gds
}  // namespace neug
