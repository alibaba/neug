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

#include "neug/execution/execute/ops/retrieve/procedure_call.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {
namespace ops {
class ProcedureCallOpr : public IOperator {
 private:
  std::unique_ptr<neug::function::CallFuncInputBase> callInput;
  function::NeugCallFunction* callFunction;

 public:
  ProcedureCallOpr(std::unique_ptr<neug::function::CallFuncInputBase> input,
                   function::NeugCallFunction* callFunction)
      : callInput(std::move(input)), callFunction(callFunction) {}

  ~ProcedureCallOpr() override = default;

  std::string get_operator_name() const override { return "ProcedureCallOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    if (callFunction == nullptr) {
      THROW_RUNTIME_ERROR("ProcedureCallOpr: callFunction is nullptr");
    }
    return callFunction->execFunc(*callInput, graph);
  }  // namespace ops
};   // namespace execution

neug::result<OpBuildResultT> ProcedureCallOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto gCatalog = neug::main::MetadataRegistry::getCatalog();
  auto procedurePB = plan.plan(op_idx).opr().procedure_call();
  auto signatureName = procedurePB.query().query_name().name();
  auto func = gCatalog->getFunctionWithSignature(signatureName);
  auto callFunc = func->ptrCast<function::NeugCallFunction>();
  ContextMeta ret_meta = ctx_meta;

  return std::make_pair(
      std::make_unique<ProcedureCallOpr>(
          callFunc->bindFunc(schema, execution::ContextMeta(), plan, op_idx),
          callFunc),
      ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug