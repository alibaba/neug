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
#include "neug/common/types/value.h"
#include "neug/execution/execute/ops/ddl/ddl_utils.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class AddEdgePropertySchemaOpr : public IOperator {
 public:
  AddEdgePropertySchemaOpr(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, Value>>& properties,
      bool ignore_conflict)
      : src_type_(src_type),
        dst_type_(dst_type),
        edge_type_(edge_type),
        properties_(properties),
        ignore_conflict_(ignore_conflict) {}
  std::string get_operator_name() const override {
    return "AddEdgePropertySchemaOpr";
  }
  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            OprTimer* timer) override {
    return defer_stream(
        std::move(input),
        [this, &graph, params,
         timer](Stream<ContextChunk>&& input) mutable -> Stream<ContextChunk> {
          auto metadata = input.metadata();
          auto before = collect_batches(std::move(input));
          if (!before) {
            return error_stream<ContextChunk>(before.error());
          }
          input = stream_from_batches(std::move(*before), std::move(metadata));

          StorageUpdateInterface& storage =
              dynamic_cast<StorageUpdateInterface&>(graph);
          label_t src, dst, edge;
          auto resolve =
              ResolveEdgeTriplet(storage.schema(), src_type_, dst_type_,
                                 edge_type_, src, dst, edge);
          if (!resolve.ok()) {
            if (ignore_conflict_ && IsSchemaConflictError(resolve)) {
              return std::move(input);
            }
            LOG(ERROR) << "Fail to add edge property to type: " << edge_type_
                       << ", reason: " << resolve.ToString();
            return error_stream<ContextChunk>(resolve);
          }
          AddEdgePropertiesParamBuilder builder;
          for (const auto& [prop_name, prop_value] : properties_) {
            builder.AddProperty(prop_name, prop_value);
          }
          auto config = builder.Build();
          auto res = storage.AddEdgeProperties(src, dst, edge, config);
          if (!res.ok()) {
            if (ignore_conflict_ && IsSchemaConflictError(res)) {
              return std::move(input);
            }
            LOG(ERROR) << "Fail to add edge property to type: " << edge_type_
                       << ", reason: " << res.ToString();
            return error_stream<ContextChunk>(res);
          }
          return std::move(input);
        });
  }

 private:
  std::string src_type_;
  std::string dst_type_;
  std::string edge_type_;
  std::vector<std::pair<std::string, Value>> properties_;
  bool ignore_conflict_;
};

neug::result<OpBuildResultT> AddEdgePropertySchemaOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  const auto& add_edge_property =
      plan.plan(op_id).opr().add_edge_property_schema();
  auto src_name = add_edge_property.edge_type().src_type_name().name();
  auto dst_name = add_edge_property.edge_type().dst_type_name().name();
  auto edge_name = add_edge_property.edge_type().type_name().name();
  auto tuple_res = property_defs_to_value(add_edge_property.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  return std::make_pair(
      std::make_unique<AddEdgePropertySchemaOpr>(
          src_name, dst_name, edge_name, tuple_res.value(),
          !conflict_action_to_bool(add_edge_property.conflict_action())),
      ctx_meta);
}

}  // namespace ops
}  // namespace execution

}  // namespace neug
