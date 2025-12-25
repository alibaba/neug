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

#include "neug/execution/execute/ops/ddl/add_vertex_property.h"
#include "neug/utils/pb_utils.h"

namespace gs {
namespace runtime {
namespace ops {

class AddVertexPropertySchemaOpr : public IOperator {
 public:
  AddVertexPropertySchemaOpr(
      const std::string& vertex_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      bool error_on_conflict)
      : vertex_type_(vertex_type),
        properties_(properties),
        error_on_conflict_(error_on_conflict) {}

  std::string get_operator_name() const override {
    return "AddVertexPropertySchemaOpr";
  }
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    auto res = storage.AddVertexProperties(vertex_type_, properties_,
                                           error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to add vertex property to type: " << vertex_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return gs::result<Context>(std::move(ctx));
  }

 private:
  std::string vertex_type_;
  std::vector<std::tuple<DataTypeId, std::string, Property>> properties_;
  bool error_on_conflict_;
};

gs::result<OpBuildResultT> AddVertexPropertySchemaOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::DDLPlan& plan, int op_id) {
  const auto& add_vertex_property = plan.add_vertex_property_schema();
  auto tuple_res = property_defs_to_tuple(add_vertex_property.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  bool conflict_action =
      conflict_action_to_bool(add_vertex_property.conflict_action());
  return std::make_pair(std::make_unique<AddVertexPropertySchemaOpr>(
                            add_vertex_property.vertex_type().name(),
                            tuple_res.value(), conflict_action),
                        ctx_meta);
}

}  // namespace ops
}  // namespace runtime

}  // namespace gs