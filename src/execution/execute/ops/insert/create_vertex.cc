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

#include "neug/execution/common/operators/insert/create_vertex.h"
#include "neug/execution/execute/ops/insert/create_vertex.h"
#include "neug/execution/utils/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace runtime {
namespace ops {

class CreateVertexOpr : public IOperator {
 public:
  CreateVertexOpr(
      const std::vector<label_t>& labels, const std::vector<int32_t>& alias,
      const std::vector<
          std::vector<std::pair<std::string, common::Expression>>>& properties)
      : labels_(labels), alias_(alias), properties_(properties) {}

  neug::result<Context> Eval(IStorageInterface& graph_interface,
                             const ParamsMap& params, Context&& ctx,
                             OprTimer* timer) override {
    // Implementation of vertex creation logic goes here.

    const StorageReadInterface* graph_ptr = nullptr;
    if (graph_interface.readable()) {
      graph_ptr = dynamic_cast<const StorageReadInterface*>(&graph_interface);
    }
    std::vector<std::vector<std::pair<std::string, Expr>>> expr_properties;
    for (size_t i = 0; i < labels_.size(); ++i) {
      const auto& props = properties_[i];
      std::vector<std::pair<std::string, Expr>> expr_props;
      for (const auto& [prop, prop_value] : props) {
        Expr e(graph_ptr, ctx, params, prop_value, VarType::kPathVar);
        expr_props.emplace_back(prop, std::move(e));
      }
      expr_properties.emplace_back(std::move(expr_props));
    }
    return CreateVertex::insert_vertex(
        dynamic_cast<StorageInsertInterface&>(graph_interface), std::move(ctx),
        labels_, expr_properties, alias_);
  }
  std::string get_operator_name() const override { return "CreateVertexOpr"; }

 private:
  std::vector<label_t> labels_;
  std::vector<int32_t> alias_;
  std::vector<std::vector<std::pair<std::string, common::Expression>>>
      properties_;
};

neug::result<OpBuildResultT> CreateVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().create_vertex();
  std::vector<label_t> labels;
  std::vector<int32_t> alias;
  std::vector<std::vector<std::pair<std::string, common::Expression>>>
      properties;
  for (const auto& entry : opr.entries()) {
    label_t label;
    switch (entry.vertex_type().item_case()) {
    case common::NameOrId::kId: {
      label = entry.vertex_type().id();
      break;
    }
    case common::NameOrId::kName: {
      label = schema.get_vertex_label_id(entry.vertex_type().name());
      break;
    }
    default:
      LOG(FATAL) << "Unknown vertex type: "
                 << entry.vertex_type().DebugString();
    }
    labels.push_back(label);
    alias.push_back(entry.alias().id());

    std::vector<std::pair<std::string, common::Expression>> props;
    for (const auto& prop : entry.property_mappings()) {
      if (!prop.has_property()) {
        LOG(FATAL) << "PropertyMapping has no property: " << prop.DebugString();
      }
      if (!prop.has_data()) {
        LOG(FATAL) << "PropertyMapping has no data: " << prop.DebugString();
      }
      props.emplace_back(prop.property().key().name(), prop.data());
    }
    properties.push_back(std::move(props));
  }

  return std::make_pair(
      std::make_unique<CreateVertexOpr>(labels, alias, properties), ctx_meta);
}
}  // namespace ops
}  // namespace runtime
}  // namespace neug