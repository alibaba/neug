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
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/execution/execute/ops/batch/data_source.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

#include <glog/logging.h>
#include <string>
#include <utility>

namespace neug {
class IDataChunkSupplier;
class PropertyGraph;

namespace execution {
class OprTimer;

namespace ops {

namespace {

bool resolve_vertex_label_id(const Schema& schema, const ::common::NameOrId& ni,
                             label_t& out) {
  switch (ni.item_case()) {
  case ::common::NameOrId::kId: {
    out = ni.id();
    return true;
  }
  case ::common::NameOrId::kName: {
    if (!schema.is_vertex_label_valid(ni.name())) {
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
  case ::common::NameOrId::kId:
    edge_label = edge_type.type_name().id();
    break;
  case ::common::NameOrId::kName: {
    const auto& name = edge_type.type_name().name();
    if (!schema.is_edge_label_valid(name)) {
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

std::vector<std::pair<int32_t, std::string>> build_total_edge_mappings(
    const std::vector<std::pair<int32_t, std::string>>& source_mappings,
    const std::vector<std::pair<int32_t, std::string>>& destination_mappings,
    const std::vector<std::pair<int32_t, std::string>>& property_mappings) {
  std::vector<std::pair<int32_t, std::string>> mappings;
  mappings.reserve(source_mappings.size() + destination_mappings.size() +
                   property_mappings.size());
  mappings.insert(mappings.end(), source_mappings.begin(),
                  source_mappings.end());
  mappings.insert(mappings.end(), destination_mappings.begin(),
                  destination_mappings.end());
  mappings.insert(mappings.end(), property_mappings.begin(),
                  property_mappings.end());
  return mappings;
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

class BatchInsertEdgeFromSourceOpr : public IOperator {
 public:
  BatchInsertEdgeFromSourceOpr(
      std::shared_ptr<reader::ReadSharedState> shared_state,
      function::ReadFunction* read_function, physical::EdgeType edge_type,
      std::vector<std::pair<int32_t, std::string>> property_mappings,
      std::vector<std::pair<int32_t, std::string>> source_mappings,
      std::vector<std::pair<int32_t, std::string>> destination_mappings)
      : shared_state_(std::move(shared_state)),
        read_function_(read_function),
        edge_type_(std::move(edge_type)),
        property_mappings_(std::move(property_mappings)),
        source_mappings_(std::move(source_mappings)),
        destination_mappings_(std::move(destination_mappings)) {}

  std::string get_operator_name() const override {
    return "BatchInsertEdgeFromSourceOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  std::shared_ptr<reader::ReadSharedState> shared_state_;
  function::ReadFunction* read_function_;
  physical::EdgeType edge_type_;
  std::vector<std::pair<int32_t, std::string>> property_mappings_;
  std::vector<std::pair<int32_t, std::string>> source_mappings_;
  std::vector<std::pair<int32_t, std::string>> destination_mappings_;
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

  auto total_mappings = build_total_edge_mappings(
      src_vertex_bindings_, dst_vertex_bindings_, prop_mappings_);
  auto supplier = create_data_chunk_supplier(ctx, total_mappings);

  RETURN_STATUS_ERROR_IF_NOT_OK(
      graph.BatchAddEdges(src_label_id, dst_label_id, edge_label_id, supplier));
  return neug::result<Context>(std::move(ctx));
}

neug::result<Context> BatchInsertEdgeFromSourceOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  (void) params;
  (void) ctx;
  (void) timer;
  CHECK(read_function_ != nullptr);
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  label_t edge_label_id = 0;
  label_t src_label_id = 0;
  label_t dst_label_id = 0;
  if (!resolve_edge_triplet(graph.schema(), edge_type_, edge_label_id,
                            src_label_id, dst_label_id)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Failed to resolve edge type for "
                        "BatchInsertEdgeFromSource");
  }

  auto mappings = build_total_edge_mappings(
      source_mappings_, destination_mappings_, property_mappings_);
  auto* ap_graph = dynamic_cast<StorageAPUpdateInterface*>(&graph_interface);
  if (ap_graph &&
      ap_graph->CanBatchBuildEdges(src_label_id, dst_label_id, edge_label_id) &&
      read_function_->sourceFunc) {
    auto raw_source = read_function_->sourceFunc(shared_state_);
    auto source = create_data_chunk_source(std::move(raw_source), mappings);
    if (source && should_use_copy_bulk_build(*source)) {
      RETURN_STATUS_ERROR_IF_NOT_OK(ap_graph->BatchBuildEdges(
          src_label_id, dst_label_id, edge_label_id, std::move(source)));
      return Context{};
    }
  }

  auto materialized = read_function_->execFunc(shared_state_);
  auto supplier = create_data_chunk_supplier(materialized, mappings);
  RETURN_STATUS_ERROR_IF_NOT_OK(graph.BatchAddEdges(
      src_label_id, dst_label_id, edge_label_id, std::move(supplier)));
  // Match the empty terminal sink consumed by the fused plan.
  materialized.tag_ids.clear();
  return neug::result<Context>(std::move(materialized));
}

neug::result<OpBuildResultT> BatchInsertEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_edge();

  if (!opr.has_edge_type()) {
    THROW_INTERNAL_EXCEPTION(
        "BatchInsertEdgeOprBuilder::Build: edge type is not set");
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

neug::result<OpBuildResultT> BatchInsertEdgeFromSourceOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta result_meta = ctx_meta;
  if (op_idx + 3 != plan.plan_size() ||
      plan.plan(op_idx + 2).opr().sink().tags_size() != 0) {
    return std::make_pair(nullptr, result_meta);
  }
  const auto& source_pb = plan.plan(op_idx).opr().source();
  const auto& edge_pb = plan.plan(op_idx + 1).opr().load_edge();
  if (!edge_pb.has_edge_type()) {
    THROW_INTERNAL_EXCEPTION(
        "BatchInsertEdgeFromSourceOprBuilder: edge type is not set");
  }

  ReadStateBuilder state_builder;
  auto state = state_builder.build(source_pb);
  auto catalog = neug::main::MetadataRegistry::getCatalog();
  auto function = catalog->getFunctionWithSignature(source_pb.extension_name());
  auto read_function = function->ptrCast<function::ReadFunction>();

  std::vector<std::pair<int32_t, std::string>> property_mappings;
  std::vector<std::pair<int32_t, std::string>> source_mappings;
  std::vector<std::pair<int32_t, std::string>> destination_mappings;
  parse_property_mappings(edge_pb.property_mappings(), property_mappings);
  parse_property_mappings(edge_pb.source_vertex_binding(), source_mappings);
  parse_property_mappings(edge_pb.destination_vertex_binding(),
                          destination_mappings);
  physical::EdgeType edge_type;
  edge_type.CopyFrom(edge_pb.edge_type());
  return std::make_pair(
      std::make_unique<BatchInsertEdgeFromSourceOpr>(
          std::move(state), read_function, std::move(edge_type),
          std::move(property_mappings), std::move(source_mappings),
          std::move(destination_mappings)),
      result_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
