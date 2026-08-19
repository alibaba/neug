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

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer*) override {
    if (input == nullptr) {
      THROW_RUNTIME_ERROR("IndexScanOpr: index scan input is null");
    }
    if (function == nullptr || function->execFunc == nullptr) {
      THROW_RUNTIME_ERROR(
          "IndexScanOpr: index scan function is not executable");
    }
    auto bound_input = input->bindParams(params);
    if (bound_input == nullptr) {
      THROW_RUNTIME_ERROR(
          "IndexScanOpr: index scan input did not create a per-Eval instance");
    }
    auto context_bound_input = bound_input->bindContext(std::move(ctx));
    if (context_bound_input == nullptr) {
      THROW_RUNTIME_ERROR(
          "IndexScanOpr: index scan input did not bind the input context");
    }
    return function->execFunc(*context_bound_input, graph);
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
  if (catalog == nullptr) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: catalog is not available");
  }
  auto* baseFunction =
      catalog->getFunctionWithSignature(indexScan.index_scan_function());
  if (baseFunction == nullptr) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: function not found: " +
                        indexScan.index_scan_function());
  }
  auto* function = dynamic_cast<function::NeugCallFunction*>(baseFunction);
  if (function == nullptr) {
    THROW_RUNTIME_ERROR(
        "IndexScanOprBuilder: function is not a NeugCallFunction: " +
        indexScan.index_scan_function());
  }
  if (function->bindFunc == nullptr) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: bind function is not registered");
  }

  auto input = function->bindFunc(schema, ctxMeta, plan, opIdx);
  if (input == nullptr) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: index scan input is null");
  }

  ContextMeta outputMeta = ctxMeta;
  const auto& metadata = plan.plan(opIdx).meta_data();
  for (int i = 0; i < metadata.size(); ++i) {
    const auto& meta = metadata.Get(i);
    // An index scan's first output is a vertex column. The logical expression
    // used to carry its alias may be the vertex's internal ID, so the physical
    // metadata can incorrectly describe it as INT64. Keep ContextMeta aligned
    // with the column produced by the index scan executor so downstream
    // property access (for example, n.id) uses a vertex accessor.
    outputMeta.set(meta.alias(), i == 0 ? DataType(DataTypeId::kVertex)
                                        : parse_from_ir_data_type(meta.type()));
  }
  return std::make_pair(
      std::make_unique<IndexScanOpr>(std::move(input), function),
      std::move(outputMeta));
}

}  // namespace neug::execution::ops
