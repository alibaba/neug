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

#include "neug/execution/execute/ops/ddl/create_vertex_type.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class CreateVertexTypeOpr : public IOperator {
 public:
  CreateVertexTypeOpr(
      const std::string& type_name,
      const std::vector<std::pair<std::string, Value>>& properties,
      const std::vector<std::string>& pks, bool error_on_conflict)
      : type_name_(type_name),
        properties_(properties),
        pks_(pks),
        error_on_conflict_(error_on_conflict) {}

  std::string get_operator_name() const override {
    return "CreateVertexTypeOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    CreateVertexTypeParamBuilder builder;
    builder.VertexLabel(type_name_).PrimaryKeyNames(pks_);
    std::vector<std::tuple<DataType, std::string, Property>> property_tuples;
    for (const auto& [prop_name, prop_value] : properties_) {
      builder.AddProperty(prop_value.type(), prop_name,
                           value_to_property(prop_value));
    }
    auto res = storage.CreateVertexType(builder.Build(), error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to create vertex type: " << type_name_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return neug::result<Context>(std::move(ctx));
  }

 private:
  std::string type_name_;
  std::vector<std::pair<std::string, Value>> properties_;
  std::vector<std::string> pks_;
  bool error_on_conflict_;
};

neug::result<OpBuildResultT> CreateVertexTypeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  ContextMeta meta = ctx_meta;
  const auto& create_vertex = plan.plan(op_id).opr().create_vertex_schema();
  auto vertex_type_name = create_vertex.vertex_type().name();
  auto tuple_res = property_defs_to_value(create_vertex.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  if (create_vertex.primary_key_size() == 0) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Must specify a primary key for vertex type creation"));
  }
  if (create_vertex.primary_key_size() > 1) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Only one primary key is supported"));
  }
  std::vector<std::string> pks;
  for (const auto& pk : create_vertex.primary_key()) {
    pks.push_back(pk);
  }
  bool error_on_conflict_ =
      conflict_action_to_bool(create_vertex.conflict_action());
  return std::make_pair(
      std::make_unique<CreateVertexTypeOpr>(vertex_type_name, tuple_res.value(),
                                            pks, error_on_conflict_),
      meta);
}

}  // namespace ops

}  // namespace execution

}  // namespace neug
