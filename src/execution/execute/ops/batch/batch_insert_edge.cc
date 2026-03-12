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

#include "neug/execution/execute/ops/batch/batch_insert_edge.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/pb_utils.h"

namespace neug {
class IRecordBatchSupplier;
class PropertyGraph;

namespace execution {
class OprTimer;

namespace ops {

class BatchInsertEdgeOpr : public IOperator {
 public:
  BatchInsertEdgeOpr(
      const label_t& edge_label_id, const label_t& src_label_id,
      const label_t& dst_label_id, const std::vector<DataTypeId>& e_prop,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings,
      const std::vector<std::pair<int32_t, std::string>>& src_vertex_bindings,
      const std::vector<std::pair<int32_t, std::string>>& dst_vertex_bindings)
      : edge_label_id_(edge_label_id),
        src_label_id_(src_label_id),
        dst_label_id_(dst_label_id),
        e_prop_(e_prop),
        prop_mappings_(prop_mappings),
        src_vertex_bindings_(src_vertex_bindings),
        dst_vertex_bindings_(dst_vertex_bindings) {}

  std::string get_operator_name() const override {
    return "BatchInsertEdgeOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  label_t edge_label_id_, src_label_id_, dst_label_id_;
  std::vector<DataTypeId> e_prop_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_,
      src_vertex_bindings_, dst_vertex_bindings_;
};

neug::result<Context> BatchInsertEdgeOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
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

  for (auto& supplier : suppliers) {
    RETURN_STATUS_ERROR_IF_NOT_OK(graph.BatchAddEdges(
        src_label_id_, dst_label_id_, edge_label_id_, supplier));
  }
  return neug::result<Context>(std::move(ctx));
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

neug::result<OpBuildResultT> BatchInsertEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_edge();
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

  std::vector<DataTypeId> edge_prop_types;
  auto edge_props =
      schema.get_edge_properties_id(src_type, dst_type, edge_type);
  if (edge_props.empty()) {
    edge_prop_types.emplace_back(DataTypeId::kEmpty);
  } else {
    for (const auto& prop : edge_props) {
      edge_prop_types.emplace_back(prop);
    }
  }

  return std::make_pair(
      std::make_unique<BatchInsertEdgeOpr>(
          edge_type, src_type, dst_type, edge_prop_types, prop_mappings,
          src_vertex_bindings, dst_vertex_binds),
      ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
