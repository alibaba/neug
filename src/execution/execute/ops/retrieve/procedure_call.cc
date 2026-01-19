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
#include "neug/compiler/gopt/g_catalog_holder.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace runtime {
namespace ops {
class ProcedureCallOpr : public IOperator {
 private:
  std::unique_ptr<gs::function::CallFuncInputBase> callInput;
  function::NeugCallFunction* callFunction;

 public:
  ProcedureCallOpr(std::unique_ptr<gs::function::CallFuncInputBase> input,
                   function::NeugCallFunction* callFunction)
      : callInput(std::move(input)), callFunction(callFunction) {}

  ~ProcedureCallOpr() override = default;

  std::string get_operator_name() const override { return "ProcedureCallOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    if (callFunction == nullptr) {
      THROW_RUNTIME_ERROR("ProcedureCallOpr: callFunction is nullptr");
    }
    return callFunction->execFunc(*callInput);
  }
};

gs::result<OpBuildResultT> ProcedureCallOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto gCatalog = catalog::GCatalogHolder::getGCatalog();
  auto procedurePB = plan.plan(op_idx).opr().procedure_call();
  auto signatureName = procedurePB.query().query_name().name();
  auto func = gCatalog->getFunctionWithSignature(signatureName);
  auto callFunc = func->ptrCast<function::NeugCallFunction>();
  ContextMeta ret_meta = ctx_meta;

  return std::make_pair(
      std::make_unique<ProcedureCallOpr>(
          callFunc->bindFunc(schema, runtime::ContextMeta(), plan, op_idx),
          callFunc),
      ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs