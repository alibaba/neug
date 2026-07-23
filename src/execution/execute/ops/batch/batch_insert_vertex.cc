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
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

#include <optional>
#include <string>
#include <utility>

namespace neug {

namespace execution {
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IOperator {
 public:
  BatchInsertVertexOpr(
      ::common::NameOrId vertex_type,
      std::vector<std::pair<int32_t, std::string>> property_mappings,
      std::optional<BatchInsertSource> source = std::nullopt)
      : vertex_type_(std::move(vertex_type)),
        property_mappings_(std::move(property_mappings)),
        source_(std::move(source)) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  ::common::NameOrId vertex_type_;
  std::vector<std::pair<int32_t, std::string>> property_mappings_;
  std::optional<BatchInsertSource> source_;
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
                        "Failed to resolve vertex type " +
                            vertex_type_.ShortDebugString() +
                            " for BatchInsertVertex");
  }
  BatchInsertInput input;
  if (source_) {
    input = create_batch_insert_input(source_->state, *source_->read_function,
                                      property_mappings_);
  } else {
    input.data = make_data_chunk_source(
        create_data_chunk_supplier(ctx, property_mappings_));
    input.output = std::move(ctx);
  }
  RETURN_STATUS_ERROR_IF_NOT_OK(
      graph.BatchAddVertices(vertex_label_id, std::move(input.data)));
  return neug::result<Context>(std::move(input.output));
}

neug::result<OpBuildResultT> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
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
                        ctx_meta);
}

neug::result<OpBuildResultT> BatchInsertVertexFromSourceOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  if (!is_terminal_batch_insert(plan, op_idx)) {
    return std::make_pair(nullptr, ctx_meta);
  }
  const auto& vertex_pb = plan.plan(op_idx + 1).opr().load_vertex();
  if (!vertex_pb.has_vertex_type()) {
    THROW_INTERNAL_EXCEPTION(
        "BatchInsertVertexFromSourceOprBuilder: vertex type is not set");
  }

  auto source = build_batch_insert_source(plan, op_idx);

  std::vector<std::pair<int32_t, std::string>> property_mappings;
  parse_property_mappings(vertex_pb.property_mappings(), property_mappings);
  ::common::NameOrId vertex_type;
  vertex_type.CopyFrom(vertex_pb.vertex_type());
  return std::make_pair(std::make_unique<BatchInsertVertexOpr>(
                            std::move(vertex_type),
                            std::move(property_mappings), std::move(source)),
                        ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
