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

#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_vertex.h"

#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_utils.h"

namespace gs {
namespace runtime {
namespace ops {

bl::result<Context> BatchInsertVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  auto& frag = graph.GetTransaction().GetGraph();

  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  if (pk_type_ == PropertyType::kInt64) {
    frag.template batch_load_vertices<int64_t>(vertex_label_id_, suppliers);
  } else if (pk_type_ == PropertyType::kInt32) {
    frag.template batch_load_vertices<int32_t>(vertex_label_id_, suppliers);
  } else if (pk_type_ == PropertyType::kString) {
    frag.template batch_load_vertices<std::string>(vertex_label_id_, suppliers);
  } else {
    LOG(FATAL) << "Unsupported primary key type: " << pk_type_;
  }
  return bl::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().load_vertex();
  // before BatchInsertVertexOpr, we assume the raw data has already been loaded
  // into memory, with each tag points to a column.

  if (!opr.has_vertex_type()) {
    LOG(FATAL) << "BatchInsertVertexOpr must have vertex type";
  }
  label_t vertex_label_id = 0;
  if (opr.vertex_type().has_id()) {
    vertex_label_id = opr.vertex_type().id();
  } else if (opr.vertex_type().has_name()) {
    vertex_label_id = schema.get_vertex_label_id(opr.vertex_type().name());
  } else {
    LOG(FATAL) << "BatchInsertVertexOpr must have vertex type";
  }
  LOG(INFO) << "vertex_label_id: " << (int32_t) vertex_label_id;
  // <tag_id, property_name>
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  return std::make_unique<BatchInsertVertexOpr>(
      vertex_label_id, get_the_pk_type_from_schema(schema, vertex_label_id),
      prop_mappings);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
