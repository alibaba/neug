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

#include "neug/execution/execute/ops/ddl/drop_vertex_property.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class DropVertexPropertySchemaOpr : public IOperator {
 public:
  DropVertexPropertySchemaOpr(const std::string& vertex_type,
                              const std::vector<std::string>& property_names,
                              bool error_on_conflict)
      : vertex_type_(vertex_type),
        property_names_(property_names),
        error_on_conflict_(error_on_conflict) {}

  std::string get_operator_name() const override {
    return "DropVertexPropertySchemaOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    DeleteVertexPropertiesParamBuilder builder;
    auto config = builder.VertexLabel(vertex_type_)
                      .DeleteProperties(property_names_)
                      .Build();
    auto res = storage.DeleteVertexProperties(config, error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to drop vertex property from type: " << vertex_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return neug::result<Context>(std::move(ctx));
  }

 private:
  std::string vertex_type_;
  std::vector<std::string> property_names_;
  bool error_on_conflict_;
};

neug::result<OpBuildResultT> DropVertexPropertySchemaOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  const auto& drop_vertex_property =
      plan.plan(op_id).opr().drop_vertex_property_schema();
  std::vector<std::string> property_names;
  for (const auto& prop : drop_vertex_property.properties()) {
    property_names.push_back(prop);
  }
  bool conflict_action =
      conflict_action_to_bool(drop_vertex_property.conflict_action());
  return std::make_pair(std::make_unique<DropVertexPropertySchemaOpr>(
                            drop_vertex_property.vertex_type().name(),
                            property_names, conflict_action),
                        ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug