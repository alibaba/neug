
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

#include "neug/execution/execute/ops/ddl/rename_edge_property.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class RenameEdgePropertySchemaOpr : public IOperator {
 public:
  RenameEdgePropertySchemaOpr(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict)
      : src_type_(src_type),
        dst_type_(dst_type),
        edge_type_(edge_type),
        rename_properties_(rename_properties),
        error_on_conflict_(error_on_conflict) {}
  std::string get_operator_name() const override {
    return "RenameEdgePropertySchemaOpr";
  }
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    auto res =
        storage.RenameEdgeProperties(src_type_, dst_type_, edge_type_,
                                     rename_properties_, error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to rename edge property in type: " << edge_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return neug::result<Context>(std::move(ctx));
  }

 private:
  std::string src_type_, dst_type_, edge_type_;
  std::vector<std::pair<std::string, std::string>> rename_properties_;
  bool error_on_conflict_;
};

neug::result<OpBuildResultT> RenameEdgePropertyOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  const auto& rename_edge_property =
      plan.plan(op_id).opr().rename_edge_property_schema();
  std::string src_type =
      rename_edge_property.edge_type().src_type_name().name();
  std::string dst_type =
      rename_edge_property.edge_type().dst_type_name().name();
  std::string edge_type = rename_edge_property.edge_type().type_name().name();
  std::vector<std::pair<std::string, std::string>> rename_properties;
  for (const auto& prop_pair : rename_edge_property.mappings()) {
    rename_properties.emplace_back(
        std::make_pair(prop_pair.first, prop_pair.second));
  }
  return std::make_pair(
      std::make_unique<RenameEdgePropertySchemaOpr>(
          src_type, dst_type, edge_type, rename_properties,
          conflict_action_to_bool(rename_edge_property.conflict_action())),
      ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug