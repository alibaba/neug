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

#include "neug/execution/execute/ops/batch/batch_insert_vertex.h"
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

namespace execution {
class OprTimer;

namespace ops {

namespace {

bool resolve_vertex_label_id(const Schema& schema,
                             const ::common::NameOrId& type,
                             label_t& label_id) {
  switch (type.item_case()) {
  case ::common::NameOrId::kId:
    label_id = type.id();
    return true;
  case ::common::NameOrId::kName:
    if (!schema.is_vertex_label_valid(type.name())) {
      LOG(ERROR) << "Unknown vertex type: " << type.DebugString();
      return false;
    }
    label_id = schema.get_vertex_label_id(type.name());
    return true;
  default:
    LOG(ERROR) << "Invalid vertex type: " << type.DebugString();
    return false;
  }
}

}  // namespace

class BatchInsertVertexOpr : public IOperator {
 public:
  BatchInsertVertexOpr(
      ::common::NameOrId vertex_type,
      std::vector<std::pair<int32_t, std::string>> prop_mappings)
      : vertex_type_(std::move(vertex_type)),
        prop_mappings_(std::move(prop_mappings)) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  ::common::NameOrId vertex_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

class BatchInsertVertexFromSourceOpr : public IOperator {
 public:
  BatchInsertVertexFromSourceOpr(
      std::shared_ptr<reader::ReadSharedState> shared_state,
      function::ReadFunction* read_function, ::common::NameOrId vertex_type,
      std::vector<std::pair<int32_t, std::string>> property_mappings)
      : shared_state_(std::move(shared_state)),
        read_function_(read_function),
        vertex_type_(std::move(vertex_type)),
        property_mappings_(std::move(property_mappings)) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexFromSourceOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  std::shared_ptr<reader::ReadSharedState> shared_state_;
  function::ReadFunction* read_function_;
  ::common::NameOrId vertex_type_;
  std::vector<std::pair<int32_t, std::string>> property_mappings_;
};

neug::result<Context> BatchInsertVertexOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  (void) params;
  (void) timer;
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  label_t vertex_label_id = 0;
  if (!resolve_vertex_label_id(graph.schema(), vertex_type_, vertex_label_id)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Failed to resolve vertex type for BatchInsertVertex");
  }
  auto supplier = create_data_chunk_supplier(ctx, prop_mappings_);
  RETURN_STATUS_ERROR_IF_NOT_OK(
      graph.BatchAddVertices(vertex_label_id, supplier));
  return neug::result<Context>(std::move(ctx));
}

neug::result<Context> BatchInsertVertexFromSourceOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  (void) params;
  (void) ctx;
  (void) timer;
  CHECK(read_function_ != nullptr);
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  label_t vertex_label_id = 0;
  if (!resolve_vertex_label_id(graph.schema(), vertex_type_, vertex_label_id)) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Failed to resolve vertex type for BatchInsertVertexFromSource");
  }

  auto* ap_graph = dynamic_cast<StorageAPUpdateInterface*>(&graph_interface);
  if (ap_graph && ap_graph->CanBatchBuildVertices(vertex_label_id) &&
      read_function_->sourceFunc) {
    auto raw_source = read_function_->sourceFunc(shared_state_);
    auto source =
        create_data_chunk_source(std::move(raw_source), property_mappings_);
    if (source && should_use_copy_bulk_build(*source)) {
      RETURN_STATUS_ERROR_IF_NOT_OK(
          ap_graph->BatchBuildVertices(vertex_label_id, std::move(source)));
      // The fused builder only accepts a terminal COPY with an empty sink.
      return Context{};
    }
  }

  auto materialized = read_function_->execFunc(shared_state_);
  auto supplier = create_data_chunk_supplier(materialized, property_mappings_);
  RETURN_STATUS_ERROR_IF_NOT_OK(
      graph.BatchAddVertices(vertex_label_id, std::move(supplier)));
  // Match the empty terminal sink consumed by the fused plan.
  materialized.tag_ids.clear();
  return neug::result<Context>(std::move(materialized));
}

neug::result<OpBuildResultT> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_vertex();

  if (!opr.has_vertex_type()) {
    THROW_INTERNAL_EXCEPTION("BatchInsertVertexOpr must have vertex type");
  }
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  ::common::NameOrId vertex_type;
  vertex_type.CopyFrom(opr.vertex_type());
  return std::make_pair(std::make_unique<BatchInsertVertexOpr>(
                            std::move(vertex_type), std::move(prop_mappings)),
                        ret_meta);
}

neug::result<OpBuildResultT> BatchInsertVertexFromSourceOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta result_meta = ctx_meta;
  if (op_idx + 3 != plan.plan_size() ||
      plan.plan(op_idx + 2).opr().sink().tags_size() != 0) {
    return std::make_pair(nullptr, result_meta);
  }
  const auto& source_pb = plan.plan(op_idx).opr().source();
  const auto& vertex_pb = plan.plan(op_idx + 1).opr().load_vertex();
  if (!vertex_pb.has_vertex_type()) {
    THROW_INTERNAL_EXCEPTION(
        "BatchInsertVertexFromSourceOprBuilder: vertex type is not set");
  }

  ReadStateBuilder state_builder;
  auto state = state_builder.build(source_pb);
  auto catalog = neug::main::MetadataRegistry::getCatalog();
  auto function = catalog->getFunctionWithSignature(source_pb.extension_name());
  auto read_function = function->ptrCast<function::ReadFunction>();

  std::vector<std::pair<int32_t, std::string>> property_mappings;
  parse_property_mappings(vertex_pb.property_mappings(), property_mappings);
  ::common::NameOrId vertex_type;
  vertex_type.CopyFrom(vertex_pb.vertex_type());
  return std::make_pair(
      std::make_unique<BatchInsertVertexFromSourceOpr>(
          std::move(state), read_function, std::move(vertex_type),
          std::move(property_mappings)),
      result_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
