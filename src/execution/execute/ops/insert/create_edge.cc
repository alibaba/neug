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

#include "neug/execution/common/operators/insert/create_edge.h"
#include "neug/execution/execute/ops/insert/create_edge.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
namespace ops {

class CreateEdgeOpr : public IOperator {
 public:
  CreateEdgeOpr(
      const std::vector<LabelTriplet>& labels,
      const std::vector<int32_t>& alias,
      const std::vector<std::pair<int32_t, int32_t>>& src_dst_tags,
      std::vector<
          std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>>>&&
          properties)
      : labels_(labels),
        alias_(alias),
        src_dst_tags_(src_dst_tags),
        properties_(std::move(properties)) {}

  neug::result<Context> Eval(IStorageInterface& graph_interface,
                             const ParamsMap& params, Context&& ctx,
                             OprTimer* timer) override {
    const StorageReadInterface* graph_ptr = nullptr;
    if (graph_interface.readable()) {
      graph_ptr = dynamic_cast<const StorageReadInterface*>(&graph_interface);
    }
    std::vector<
        std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>>
        expr_properties;
    for (size_t i = 0; i < labels_.size(); ++i) {
      const auto& props = properties_[i];
      std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
          expr_props;
      for (const auto& [prop, prop_value] : props) {
        auto expr = prop_value->bind(graph_ptr, params);
        expr_props.emplace_back(prop, std::move(expr));
      }
      expr_properties.emplace_back(std::move(expr_props));
    }
    return CreateEdge::insert_edge(
        dynamic_cast<StorageInsertInterface&>(graph_interface), std::move(ctx),
        labels_, src_dst_tags_, std::move(expr_properties), alias_);
  }
  std::string get_operator_name() const override { return "CreateEdgeOpr"; }

 private:
  std::vector<LabelTriplet> labels_;
  std::vector<int32_t> alias_;
  std::vector<std::pair<int32_t, int32_t>> src_dst_tags_;
  std::vector<std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>>>
      properties_;
};
neug::result<OpBuildResultT> CreateEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().create_edge();
  std::vector<LabelTriplet> labels;
  std::vector<int32_t> alias;
  std::vector<std::pair<int32_t, int32_t>> src_dst_tags;
  std::vector<std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>>>
      properties;
  for (const auto& edge : opr.entries()) {
    LabelTriplet triplet;
    triplet.src_label = edge.edge_type().src_type_name().id();
    triplet.dst_label = edge.edge_type().dst_type_name().id();
    triplet.edge_label = edge.edge_type().type_name().id();
    labels.push_back(triplet);
    int32_t src_tag = edge.source_vertex_binding().id();
    int32_t dst_tag = edge.destination_vertex_binding().id();
    src_dst_tags.emplace_back(src_tag, dst_tag);
    alias.push_back(edge.alias().id());
    ret_meta.set(edge.alias().id(), DataType::EDGE);
    std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> props;
    for (const auto& prop : edge.property_mappings()) {
      auto expr = neug::execution::parse_expression(
          prop.data(), ctx_meta, neug::execution::VarType::kRecord);
      props.emplace_back(prop.property().key().name(), std::move(expr));
    }
    properties.push_back(std::move(props));
  }
  return std::make_pair(std::make_unique<CreateEdgeOpr>(
                            labels, alias, src_dst_tags, std::move(properties)),
                        ret_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug