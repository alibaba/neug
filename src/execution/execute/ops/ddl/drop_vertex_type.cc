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

#include "neug/execution/execute/ops/ddl/drop_vertex_type.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class DropVertexTypeOpr : public IOperator {
 public:
  DropVertexTypeOpr(const std::string& vertex_type, bool error_on_conflict)
      : vertex_type_(vertex_type), error_on_conflict_(error_on_conflict) {}

  std::string get_operator_name() const override { return "DropVertexTypeOpr"; }
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    auto res = storage.DeleteVertexType(vertex_type_, error_on_conflict_);
    if (!res.ok()) {
      LOG(ERROR) << "Fail to drop vertex type: " << vertex_type_
                 << ", reason: " << res.ToString();
      RETURN_ERROR(res);
    }
    return neug::result<Context>(std::move(ctx));
  }

 private:
  std::string vertex_type_;
  bool error_on_conflict_;
};

neug::result<OpBuildResultT> DropVertexTypeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  const auto& drop_vertex = plan.plan(op_id).opr().drop_vertex_schema();
  auto vertex_type_name = drop_vertex.vertex_type().name();
  bool error_on_conflict =
      conflict_action_to_bool(drop_vertex.conflict_action());
  return std::make_pair(
      std::make_unique<DropVertexTypeOpr>(vertex_type_name, error_on_conflict),
      ctx_meta);
}

}  // namespace ops
}  // namespace execution

}  // namespace neug
