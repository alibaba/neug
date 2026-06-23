/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "neug/compiler/function/gds/gds_algo_function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/execute/operator.h"

namespace neug {

namespace execution {

namespace ops {

class GDSAlgoOpr : public IOperator {
 public:
  GDSAlgoOpr(std::unique_ptr<function::CallFuncInputBase> algo_input,
             function::GDSAlgoFunction* algo_func);

  ~GDSAlgoOpr() override = default;

  std::string get_operator_name() const override { return "GDSAlgoOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override;

 private:
  std::unique_ptr<function::CallFuncInputBase> algo_input_;
  function::GDSAlgoFunction* algo_func_;
};

class GDSAlgoOprBuilder : public IOperatorBuilder {
 public:
  GDSAlgoOprBuilder() = default;
  ~GDSAlgoOprBuilder() = default;

  neug::result<OpBuildResultT> Build(const neug::Schema& schema,
                                     const ContextMeta& ctx_meta,
                                     const physical::PhysicalPlan& plan,
                                     int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kGdsAlgo};
  }
};

}  // namespace ops

}  // namespace execution

}  // namespace neug
