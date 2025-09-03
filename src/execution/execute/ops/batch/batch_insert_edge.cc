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

#include <glog/logging.h>
#include <stddef.h>

#include <ostream>
#include <string_view>
#include <tuple>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/execute/ops/batch/batch_insert_edge.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/abstract_arrow_fragment_loader.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/pb_utils.h"

namespace gs {
class IRecordBatchSupplier;
class PropertyGraph;

namespace runtime {
class OprTimer;

namespace ops {

gs::result<Context> BatchInsertEdgeOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  auto& frag = graph.GetTransaction().GetGraph();
  std::vector<std::pair<int32_t, std::string>>
      total_mappings;  // include prop_mappings and src/dst vertex bindings
  for (const auto& mapping : src_vertex_bindings_) {
    total_mappings.emplace_back(mapping);
  }
  for (const auto& mapping : dst_vertex_bindings_) {
    total_mappings.emplace_back(mapping);
  }
  for (const auto& mapping : prop_mappings_) {
    total_mappings.emplace_back(mapping);
  }
  auto suppliers = create_record_batch_supplier(ctx, total_mappings);

  AbstractArrowFragmentLoader::batch_load_edges(
      frag, src_label_id_, dst_label_id_, edge_label_id_, suppliers);

  return gs::result<Context>(std::move(ctx));
}

bool get_edge_triplet_label_ids(const Schema& schema,
                                const physical::EdgeType& edge_type,
                                label_t& edge_label, label_t& src_type,
                                label_t& dst_type) {
  switch (edge_type.type_name().item_case()) {
  case common::NameOrId::kId: {
    edge_label = edge_type.type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    edge_label = schema.get_edge_label_id(edge_type.type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  switch (edge_type.src_type_name().item_case()) {
  case common::NameOrId::kId: {
    src_type = edge_type.src_type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    src_type = schema.get_vertex_label_id(edge_type.src_type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  switch (edge_type.dst_type_name().item_case()) {
  case common::NameOrId::kId: {
    dst_type = edge_type.dst_type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    dst_type = schema.get_vertex_label_id(edge_type.dst_type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  return true;
}

std::unique_ptr<IUpdateOperator> BatchInsertEdgeOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().load_edge();
  // before BatchInsertEdgeOpr, we assume the raw data has already been loaded
  // into memory, with each tag points to a column.

  if (!opr.has_edge_type()) {
    LOG(FATAL) << "BatchInsertEdgeOprBuilder::Build: edge type is not set";
  }
  label_t edge_type, src_type, dst_type;
  get_edge_triplet_label_ids(schema, opr.edge_type(), edge_type, src_type,
                             dst_type);

  // parse property mappings
  std::vector<std::pair<int32_t, std::string>> prop_mappings,
      src_vertex_bindings, dst_vertex_binds;
  parse_property_mappings(opr.property_mappings(), prop_mappings);
  parse_property_mappings(opr.source_vertex_binding(), src_vertex_bindings);
  parse_property_mappings(opr.destination_vertex_binding(), dst_vertex_binds);

  PropertyType edge_prop_type, src_pk_type, dst_pk_type;
  auto& edge_props = schema.get_edge_properties(src_type, dst_type, edge_type);
  if (edge_props.empty()) {
    edge_prop_type = PropertyType::Empty();
  } else if (edge_props.size() == 1) {
    edge_prop_type = edge_props[0];
  } else {
    edge_prop_type = PropertyType::RecordView();
  }
  src_pk_type = get_the_pk_type_from_schema(schema, src_type);
  dst_pk_type = get_the_pk_type_from_schema(schema, dst_type);

  std::vector<std::tuple<PropertyType, std::string, size_t>> src_primary_key,
      dst_primary_key;

  return std::make_unique<BatchInsertEdgeOpr>(
      edge_type, src_type, dst_type, edge_prop_type, src_pk_type, dst_pk_type,
      prop_mappings, src_vertex_bindings, dst_vertex_binds);
}

template <typename GraphInterface>
gs::result<Context> InsertEdgeOpr::eval_impl(
    GraphInterface& graph, const std::map<std::string, std::string>& params,
    Context&& ctx, gs::runtime::OprTimer* timer) {
  for (auto& entry : edge_data_) {
    auto& [edge_triplet, src_vertex_mapping, dst_vertex_mapping,
           edge_properties] = entry;
    auto& [edge_label_id, src_label_id, dst_label_id] = edge_triplet;
    VLOG(10) << "InsertEdgeOpr::eval_impl: edge_label_id = "
             << (int32_t) edge_label_id
             << ", src_label_id = " << (int32_t) src_label_id
             << ", dst_label_id = " << (int32_t) dst_label_id
             << ", src_vertex_mapping = " << src_vertex_mapping
             << ", dst_vertex_mapping = " << dst_vertex_mapping;
    auto src_col = ctx.get(src_vertex_mapping);
    auto dst_col = ctx.get(dst_vertex_mapping);
    if (!src_col || !dst_col) {
      LOG(ERROR)
          << "InsertEdgeOpr::eval_impl: source or destination column is null";
      RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                          "Source or destination column is null");
    }
    auto src_size = src_col->size();
    auto dst_size = dst_col->size();
    // src_size should be equal to dst_size.
    if (src_size != dst_size) {
      LOG(ERROR) << "InsertEdgeOpr::eval_impl: source and destination column "
                    "size mismatch";
      RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                          "Source and destination column size mismatch");
    }
    // We also expect both columns are of vertex type.
    if (src_col->column_type() != ContextColumnType::kVertex ||
        dst_col->column_type() != ContextColumnType::kVertex) {
      LOG(ERROR) << "InsertEdgeOpr::eval_impl: source or destination column is "
                    "not of vertex type";
      RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                          "Source or destination column is not of vertex type");
    }
    auto src_v_column = std::dynamic_pointer_cast<IVertexColumn>(src_col);
    auto dst_v_column = std::dynamic_pointer_cast<IVertexColumn>(dst_col);
    if (!src_v_column || !dst_v_column) {
      LOG(ERROR) << "InsertEdgeOpr::eval_impl: source or destination column is "
                    "not of vertex column type";
      RETURN_STATUS_ERROR(
          gs::StatusCode::ERR_INTERNAL_ERROR,
          "Source or destination column is not of vertex column type");
    }
    VLOG(10) << "src_v " << src_v_column->column_info() << ", dst_v "
             << dst_v_column->column_info();
    const auto& src_label_set = src_v_column->get_labels_set();
    const auto& dst_label_set = dst_v_column->get_labels_set();
    if (src_label_set.find(src_label_id) == src_label_set.end()) {
      LOG(ERROR) << "InsertEdgeOpr::eval_impl: source vertex label "
                    "mismatch, expected "
                 << (int32_t) src_label_id << ", got "
                 << src_v_column->column_info()
                 << ", src label set: " << src_label_set.size();
      return gs::result<Context>(std::move(ctx));
    }
    if (dst_label_set.find(dst_label_id) == dst_label_set.end()) {
      LOG(ERROR) << "InsertEdgeOpr::eval_impl: destination vertex label "
                    "mismatch, expected "
                 << (int32_t) dst_label_id << ", got "
                 << dst_v_column->column_info();
      RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                          "dst label not found");
    }

    auto edge_properties_name = graph.schema().get_edge_property_names(
        src_label_id, dst_label_id, edge_label_id);
    std::vector<Any> edge_properties_ordered;
    edge_properties_ordered.resize(edge_properties.size());
    for (const auto& prop : edge_properties) {
      auto it = std::find(edge_properties_name.begin(),
                          edge_properties_name.end(), prop.first);
      if (it == edge_properties_name.end()) {
        LOG(ERROR) << "InsertEdgeOpr::eval_impl: edge property " << prop.first
                   << " not found in edge type " << edge_label_id << " from "
                   << src_label_id << " to " << dst_label_id;
        RETURN_STATUS_ERROR(
            gs::StatusCode::ERR_INTERNAL_ERROR,
            "Edge property not found in edge type from source to destination");
      }
      auto idx = std::distance(edge_properties_name.begin(), it);
      if (idx < 0 || idx >= static_cast<int>(edge_properties_ordered.size())) {
        LOG(ERROR) << "InsertEdgeOpr::eval_impl: edge property index out of "
                      "range: "
                   << idx << ", edge properties size: "
                   << edge_properties_ordered.size();
        RETURN_STATUS_ERROR(
            gs::StatusCode::ERR_INTERNAL_ERROR,
            "Edge property index out of range in edge type from source to ");
      }
      if (edge_properties_ordered[idx].type != PropertyType::Empty()) {
        THROW_RUNTIME_ERROR(
            "InsertEdgeOpr::eval_impl: edge property already set at index " +
            std::to_string(idx) + ", property name: " + prop.first);
      }
      edge_properties_ordered[idx] = prop.second;
    }
    // Now we have all the information we need to insert the edges.
    for (size_t i = 0; i < src_size; ++i) {
      auto src_vertex_record = src_v_column->get_vertex(i);
      auto dst_vertex_record = dst_v_column->get_vertex(i);
      if (src_vertex_record.label() != src_label_id ||
          dst_vertex_record.label() != dst_label_id) {
        LOG(ERROR) << "InsertEdgeOpr::eval_impl: source or destination vertex "
                      "label mismatch, expected "
                   << (int32_t) src_label_id << " and "
                   << (int32_t) dst_label_id << ", got "
                   << src_vertex_record.label() << " and "
                   << dst_vertex_record.label();
        RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                            "Vertex label mismatch");
      }
      auto src_vertex = src_vertex_record.vid();
      auto dst_vertex = dst_vertex_record.vid();
      if (src_vertex == std::numeric_limits<vid_t>::max() ||
          dst_vertex == std::numeric_limits<vid_t>::max()) {
        LOG(WARNING)
            << "InsertEdgeOpr::eval_impl: source or destination vertex "
               "id is invalid";
        continue;
      }
      bool insert_res = false;
      if (edge_properties_ordered.size() == 1) {
        insert_res =
            graph.AddEdge(src_label_id, src_vertex, dst_label_id, dst_vertex,
                          edge_label_id, edge_properties_ordered[0]);
      } else {
        Record record(edge_properties_ordered);
        insert_res =
            graph.AddEdge(src_label_id, src_vertex, dst_label_id, dst_vertex,
                          edge_label_id, Any::From(record));
      }
      if (!insert_res) {
        THROW_RUNTIME_ERROR(
            "InsertEdgeOpr::eval_impl: failed to add edge from " +
            std::to_string(src_vertex) + " to " + std::to_string(dst_vertex) +
            " with edge label " + std::to_string(edge_label_id));
      }
    }
  }
  return gs::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> InsertEdgeOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto& opr = plan.query_plan().plan(op_idx).opr().create_edge();
  LOG(INFO) << "InsertEdgeOprBuilder::Build: opr = " << opr.DebugString();
  std::vector<InsertEdgeOpr::edge_data_t> edge_data;
  for (const auto& edge : opr.entries()) {
    auto edge_triplet = std::make_tuple(edge.edge_type().type_name().id(),
                                        edge.edge_type().src_type_name().id(),
                                        edge.edge_type().dst_type_name().id());
    auto src_vertex_mapping = edge.source_vertex_binding().id();
    auto dst_vertex_mapping = edge.destination_vertex_binding().id();
    std::vector<std::pair<std::string, Any>> edge_properties;
    for (const auto& prop : edge.property_mappings()) {
      if (prop.data().operators_size() != 1) {
        THROW_RUNTIME_ERROR(
            "InsertEdgeOprBuilder::Build: property value must have exactly "
            "one operator");
      }
      edge_properties.emplace_back(
          prop.property().key().name(),
          expr_opr_value_to_any(prop.data().operators(0)));
    }
    edge_data.emplace_back(edge_triplet, src_vertex_mapping, dst_vertex_mapping,
                           edge_properties);
  }
  return std::make_unique<InsertEdgeOpr>(std::move(edge_data));
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
