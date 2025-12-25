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
#include <ostream>

#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/batch_insert_vertex.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/property.h"

namespace gs {

namespace runtime {
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IOperator {
 public:
  BatchInsertVertexOpr(
      const label_t& vertex_label_id, const DataTypeId& pk_type,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings)
      : vertex_label_id_(vertex_label_id),
        pk_type_(pk_type),
        prop_mappings_(prop_mappings) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  label_t vertex_label_id_;
  DataTypeId pk_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

gs::result<Context> BatchInsertVertexOpr::Eval(
    IStorageInterface& graph_interface,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  for (auto supplier : suppliers) {
    if (!graph.BatchAddVertices(vertex_label_id_, supplier).ok()) {
      THROW_INTERNAL_EXCEPTION("Failed to add vertices");
    }
  }
  return gs::result<Context>(std::move(ctx));
}

gs::result<OpBuildResultT> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.query_plan().plan(op_idx).opr().load_vertex();
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
  LOG(INFO) << "vertex_label_id: " << (int32_t) vertex_label_id;
  // <tag_id, property_name>
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  return std::make_pair(
      std::make_unique<BatchInsertVertexOpr>(
          vertex_label_id, get_the_pk_type_from_schema(schema, vertex_label_id),
          prop_mappings),
      ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
