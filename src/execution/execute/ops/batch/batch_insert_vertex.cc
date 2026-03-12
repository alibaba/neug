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
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/property.h"

namespace neug {

namespace execution {
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IOperator {
 public:
  BatchInsertVertexOpr(
      const label_t& vertex_label_id,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings)
      : vertex_label_id_(vertex_label_id), prop_mappings_(prop_mappings) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  label_t vertex_label_id_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

neug::result<Context> BatchInsertVertexOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  for (auto supplier : suppliers) {
    RETURN_STATUS_ERROR_IF_NOT_OK(
        graph.BatchAddVertices(vertex_label_id_, supplier));
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().load_vertex();
  // before BatchInsertVertexOpr, we assume the raw data has already been loaded
  // into memory, with each tag points to a column.

  if (!opr.has_vertex_type()) {
    LOG(FATAL) << "BatchInsertVertexOpr must have vertex type";
  }
  label_t vertex_label_id = 0;
  switch (opr.vertex_type().item_case()) {
  case common::NameOrId::kId: {
    vertex_label_id = opr.vertex_type().id();
    break;
  }
  case common::NameOrId::kName: {
    vertex_label_id = schema.get_vertex_label_id(opr.vertex_type().name());
    break;
  }
  default:
    LOG(FATAL) << "Unknown vertex type: " << opr.vertex_type().DebugString();
  }
  // <tag_id, property_name>
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  return std::make_pair(
      std::make_unique<BatchInsertVertexOpr>(vertex_label_id, prop_mappings),
      ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
