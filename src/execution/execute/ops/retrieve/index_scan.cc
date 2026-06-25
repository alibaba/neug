/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/execute/ops/retrieve/index_scan.h"

#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"

namespace neug::execution::ops {
namespace {

class IndexScanOpr final : public IOperator {
 public:
  IndexScanOpr(std::unique_ptr<function::CallFuncInputBase> input,
               function::NeugCallFunction* function)
      : input{std::move(input)}, function{function} {}

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap&,
                             Context&&, OprTimer*) override {
    if (function == nullptr || function->execFunc == nullptr) {
      THROW_RUNTIME_ERROR(
          "IndexScanOpr: index scan function is not executable");
    }
    return function->execFunc(*input, graph);
  }

  std::string get_operator_name() const override { return "IndexScanOpr"; }

 private:
  std::unique_ptr<function::CallFuncInputBase> input;
  function::NeugCallFunction* function;
};

}  // namespace

neug::result<OpBuildResultT> IndexScanOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctxMeta,
    const physical::PhysicalPlan& plan, int opIdx) {
  const auto& indexScan = plan.plan(opIdx).opr().index_scan();
  auto* catalog = main::MetadataRegistry::getCatalog();
  auto* baseFunction =
      catalog->getFunctionWithSignature(indexScan.index_scan_function());
  auto* function = baseFunction->ptrCast<function::NeugCallFunction>();
  if (function->bindFunc == nullptr) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: bind function is not registered");
  }

  ContextMeta outputMeta = ctxMeta;
  if (plan.plan(opIdx).meta_data_size() > 0) {
    outputMeta.set(plan.plan(opIdx).meta_data(0).alias(), DataType::VERTEX);
  }
  return std::make_pair(
      std::make_unique<IndexScanOpr>(
          function->bindFunc(schema, ctxMeta, plan, opIdx), function),
      std::move(outputMeta));
}

}  // namespace neug::execution::ops
