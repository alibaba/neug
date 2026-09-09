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

#include "neug/execution/execute/ops/retrieve/limit.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/limit.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class LimitOpr : public IOperator {
 public:
  explicit LimitOpr(const algebra::Limit& opr) {
    lower_ = 0;
    upper_ = std::numeric_limits<size_t>::max();
    if (opr.has_range()) {
      lower_ = std::max(lower_, static_cast<size_t>(opr.range().lower()));
      upper_ = std::min(upper_, static_cast<size_t>(opr.range().upper()));
    }
  }

  std::string get_operator_name() const override { return "LimitOpr"; }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer) override {
    auto tags = input.tag_ids;
    auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
    return Stream<ContextChunk>(
        [upstream, skip = lower_,
         remaining = upper_ > lower_ ? upper_ - lower_ : 0,
         done = false]() mutable -> Stream<ContextChunk>::NextResult {
          if (done) {
            return std::optional<ContextChunk>{};
          }
          GS_AUTO(next, upstream->Next());
          if (!next) {
            return std::optional<ContextChunk>{};
          }
          ContextChunk chunk = std::move(*next);
          auto rows = chunk.row_num();
          auto begin = std::min(skip, rows);
          skip -= begin;
          auto count = std::min(remaining, rows - begin);
          remaining -= count;
          GS_AUTO(output, Limit::limit(std::move(chunk), begin, begin + count));
          if (remaining == 0) {
            done = true;
            *upstream = Stream<ContextChunk>();
          }
          return std::optional<ContextChunk>(std::move(output));
        },
        std::move(tags));
  }

 private:
  size_t lower_;
  size_t upper_;
};

neug::result<OpBuildResultT> LimitOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(
      std::make_unique<LimitOpr>(plan.plan(op_idx).opr().limit()), ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug