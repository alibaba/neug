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

#include <mutex>

#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

namespace neug::execution::ops {
namespace {

class IndexScanOpr final : public IOperator {
 public:
  IndexScanOpr(std::unique_ptr<function::CallFuncInputBase> unbound_input,
               function::NeugCallFunction* function)
      : unbound_input_(std::move(unbound_input)), function_(function) {}

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer*) override {
    if (!function_ || !function_->execFunc) {
      THROW_RUNTIME_ERROR(
          "IndexScanOpr: index scan function is not executable");
    }
    if (!unbound_input_) {
      THROW_RUNTIME_ERROR("IndexScanOpr: bound input is null");
    }

    // bindContext mutates the per-call input. Protect the immutable template
    // fallback for functions that have no deferred parameters and therefore do
    // not allocate a per-Eval input from bindParams().
    std::lock_guard lock(input_mutex_);
    auto bound_input = unbound_input_->bindParams(params);
    auto& input = bound_input ? *bound_input : *unbound_input_;
    input.bindContext(std::move(ctx));
    return function_->execFunc(input, graph);
  }

  std::string get_operator_name() const override { return "IndexScanOpr"; }

 private:
  std::unique_ptr<function::CallFuncInputBase> unbound_input_;
  function::NeugCallFunction* function_;
  std::mutex input_mutex_;
};

}  // namespace

neug::result<OpBuildResultT> IndexScanOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& physical_op = plan.plan(op_idx);
  const auto& index_scan = physical_op.opr().index_scan();
  if (index_scan.index_scan_function().empty()) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: function name is empty");
  }

  auto* catalog = main::MetadataRegistry::getCatalog();
  auto* base_function =
      catalog->getFunctionWithSignature(index_scan.index_scan_function());
  auto* function = dynamic_cast<function::NeugCallFunction*>(base_function);
  if (!function) {
    THROW_RUNTIME_ERROR(
        "IndexScanOprBuilder: registered function is not a NeugCallFunction");
  }
  if (!function->bindFunc) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: bind function is not registered");
  }
  if (!function->execFunc) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: exec function is not registered");
  }

  auto input = function->bindFunc(schema, ctx_meta, plan, op_idx);
  if (!input) {
    THROW_RUNTIME_ERROR("IndexScanOprBuilder: bind function returned null");
  }

  ContextMeta output_meta = ctx_meta;
  for (int i = 0; i < physical_op.meta_data_size(); ++i) {
    const auto& meta = physical_op.meta_data(i);
    output_meta.set(meta.alias(), parse_from_ir_data_type(meta.type()));
  }

  return std::make_pair(
      std::make_unique<IndexScanOpr>(std::move(input), function),
      std::move(output_meta));
}

}  // namespace neug::execution::ops
