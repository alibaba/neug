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
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

#include <glog/logging.h>
#include <string>
#include <utility>

namespace neug {

namespace execution {
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IOperator {
 public:
  BatchInsertVertexOpr(
      common::NameOrId vertex_type,
      std::vector<std::pair<int32_t, std::string>> prop_mappings)
      : vertex_type_(std::move(vertex_type)),
        prop_mappings_(std::move(prop_mappings)) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  common::NameOrId vertex_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

neug::result<Context> BatchInsertVertexOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  (void) params;
  (void) timer;
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  label_t vertex_label_id = 0;
  switch (vertex_type_.item_case()) {
  case common::NameOrId::kId:
    vertex_label_id = vertex_type_.id();
    break;
  case common::NameOrId::kName: {
    const auto& name = vertex_type_.name();
    if (!graph.schema().contains_vertex_label(name)) {
      LOG(ERROR) << "Unknown vertex type: " << vertex_type_.DebugString();
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Unknown vertex type: " + name);
    }
    vertex_label_id = graph.schema().get_vertex_label_id(name);
    break;
  }
  default:
    LOG(FATAL) << "BatchInsertVertexOpr: invalid vertex_type: "
               << vertex_type_.DebugString();
  }
  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  for (auto supplier : suppliers) {
    RETURN_STATUS_ERROR_IF_NOT_OK(
        graph.BatchAddVertices(vertex_label_id, supplier));
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_vertex();

  if (!opr.has_vertex_type()) {
    LOG(FATAL) << "BatchInsertVertexOpr must have vertex type";
  }
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  common::NameOrId vertex_type;
  vertex_type.CopyFrom(opr.vertex_type());
  return std::make_pair(std::make_unique<BatchInsertVertexOpr>(
                            std::move(vertex_type), std::move(prop_mappings)),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
