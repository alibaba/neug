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

#include "neug/execution/execute/ops/ddl/add_edge_property.h"
#include "neug/utils/pb_utils.h"

namespace gs {
namespace runtime {
namespace ops {

class AddEdgePropertySchemaOpr : public IOperator {
 public:
  AddEdgePropertySchemaOpr(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      bool error_on_conflict)
      : src_type_(src_type),
        dst_type_(dst_type),
        edge_type_(edge_type),
        properties_(properties),
        error_on_conflict_(error_on_conflict) {}
  std::string get_operator_name() const override {
    return "AddEdgePropertySchemaOpr";
  }
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    auto res = storage.AddEdgeProperties(src_type_, dst_type_, edge_type_,
                                         properties_, error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to add edge property to type: " << edge_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return gs::result<Context>(std::move(ctx));
  }

 private:
  std::string src_type_;
  std::string dst_type_;
  std::string edge_type_;
  std::vector<std::tuple<DataTypeId, std::string, Property>> properties_;
  bool error_on_conflict_;
};

gs::result<OpBuildResultT> AddEdgePropertySchemaOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::DDLPlan& plan, int op_id) {
  const auto& add_edge_property = plan.add_edge_property_schema();
  auto src_name = add_edge_property.edge_type().src_type_name().name();
  auto dst_name = add_edge_property.edge_type().dst_type_name().name();
  auto edge_name = add_edge_property.edge_type().type_name().name();
  auto tuple_res = property_defs_to_tuple(add_edge_property.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  return std::make_pair(
      std::make_unique<AddEdgePropertySchemaOpr>(
          src_name, dst_name, edge_name, tuple_res.value(),
          conflict_action_to_bool(add_edge_property.conflict_action())),
      ctx_meta);
}

}  // namespace ops
}  // namespace runtime

}  // namespace gs