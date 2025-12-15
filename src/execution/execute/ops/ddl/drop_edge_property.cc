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

#include "neug/execution/execute/ops/ddl/drop_edge_property.h"
#include "neug/utils/pb_utils.h"

namespace gs {
namespace runtime {
namespace ops {

class DropEdgePropertySchemaOpr : public IOperator {
 public:
  DropEdgePropertySchemaOpr(const std::string& src_type,
                            const std::string& dst_type,
                            const std::string& edge_type,
                            const std::vector<std::string>& property_names,
                            bool error_on_conflict)
      : src_type_(src_type),
        dst_type_(dst_type),
        edge_type_(edge_type),
        property_names_(property_names),
        error_on_conflict_(error_on_conflict) {}

  std::string get_operator_name() const override {
    return "DropEdgePropertySchemaOpr";
  }
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    auto res = storage.DeleteEdgeProperties(
        src_type_, dst_type_, edge_type_, property_names_, error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to drop edge property from type: " << edge_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return gs::result<Context>(std::move(ctx));
  }

 private:
  std::string src_type_, dst_type_, edge_type_;
  std::vector<std::string> property_names_;
  bool error_on_conflict_;
};

gs::result<OpBuildResultT> DropEdgePropertySchemaOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::DDLPlan& plan, int op_id) {
  const auto& drop_edge_property = plan.drop_edge_property_schema();
  std::vector<std::string> property_names;
  for (const auto& prop_name : drop_edge_property.properties()) {
    property_names.push_back(prop_name);
  }
  bool conflict_action =
      conflict_action_to_bool(drop_edge_property.conflict_action());
  const auto& edge_type = drop_edge_property.edge_type();
  return std::make_pair(
      std::make_unique<DropEdgePropertySchemaOpr>(
          edge_type.src_type_name().name(), edge_type.dst_type_name().name(),
          edge_type.type_name().name(), property_names, conflict_action),
      ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs