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
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

#include <glog/logging.h>
#include <string>
#include <utility>

namespace neug {
class IRecordBatchSupplier;
class PropertyGraph;

namespace execution {
class OprTimer;

namespace ops {

namespace {

bool resolve_vertex_label_id(const Schema& schema, const common::NameOrId& ni,
                             label_t& out) {
  switch (ni.item_case()) {
  case common::NameOrId::kId: {
    out = ni.id();
    return true;
  }
  case common::NameOrId::kName: {
    if (!schema.contains_vertex_label(ni.name())) {
      LOG(ERROR) << "Unknown vertex type: " << ni.DebugString();
      return false;
    }
    out = schema.get_vertex_label_id(ni.name());
    return true;
  }
  default:
    LOG(ERROR) << "Unknown vertex type: " << ni.DebugString();
    return false;
  }
}

/** Resolve edge + src/dst vertex labels from schema at execution time. */
bool resolve_edge_triplet(const Schema& schema,
                          const physical::EdgeType& edge_type,
                          label_t& edge_label, label_t& src_type,
                          label_t& dst_type) {
  switch (edge_type.type_name().item_case()) {
  case common::NameOrId::kId:
    edge_label = edge_type.type_name().id();
    break;
  case common::NameOrId::kName: {
    const auto& name = edge_type.type_name().name();
    if (!schema.contains_edge_label(name)) {
      LOG(ERROR) << "Unknown edge type: "
                 << edge_type.type_name().DebugString();
      return false;
    }
    edge_label = schema.get_edge_label_id(name);
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  if (!resolve_vertex_label_id(schema, edge_type.src_type_name(), src_type)) {
    return false;
  }
  if (!resolve_vertex_label_id(schema, edge_type.dst_type_name(), dst_type)) {
    return false;
  }
  return true;
}

}  // namespace

class BatchInsertEdgeOpr : public IOperator {
 public:
  BatchInsertEdgeOpr(
      physical::EdgeType edge_type,
      std::vector<std::pair<int32_t, std::string>> prop_mappings,
      std::vector<std::pair<int32_t, std::string>> src_vertex_bindings,
      std::vector<std::pair<int32_t, std::string>> dst_vertex_bindings)
      : edge_type_(std::move(edge_type)),
        prop_mappings_(std::move(prop_mappings)),
        src_vertex_bindings_(std::move(src_vertex_bindings)),
        dst_vertex_bindings_(std::move(dst_vertex_bindings)) {}

  std::string get_operator_name() const override {
    return "BatchInsertEdgeOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  physical::EdgeType edge_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_,
      src_vertex_bindings_, dst_vertex_bindings_;
};

neug::result<Context> BatchInsertEdgeOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  (void) params;
  (void) timer;
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  label_t edge_label_id = 0;
  label_t src_label_id = 0;
  label_t dst_label_id = 0;
  if (!resolve_edge_triplet(graph.schema(), edge_type_, edge_label_id,
                            src_label_id, dst_label_id)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Failed to resolve edge type or vertex endpoints for "
                        "BatchInsertEdge");
  }

  std::vector<std::pair<int32_t, std::string>> total_mappings;
  total_mappings.reserve(src_vertex_bindings_.size() +
                         dst_vertex_bindings_.size() + prop_mappings_.size());
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
        src_label_id, dst_label_id, edge_label_id, supplier));
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> BatchInsertEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_edge();

  if (!opr.has_edge_type()) {
    LOG(FATAL) << "BatchInsertEdgeOprBuilder::Build: edge type is not set";
  }

  std::vector<std::pair<int32_t, std::string>> prop_mappings,
      src_vertex_bindings, dst_vertex_binds;
  parse_property_mappings(opr.property_mappings(), prop_mappings);
  parse_property_mappings(opr.source_vertex_binding(), src_vertex_bindings);
  parse_property_mappings(opr.destination_vertex_binding(), dst_vertex_binds);

  physical::EdgeType edge_type;
  edge_type.CopyFrom(opr.edge_type());
  return std::make_pair(
      std::make_unique<BatchInsertEdgeOpr>(
          std::move(edge_type), std::move(prop_mappings),
          std::move(src_vertex_bindings), std::move(dst_vertex_binds)),
      ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
