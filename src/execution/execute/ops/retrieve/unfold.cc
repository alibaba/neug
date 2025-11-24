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

#include "neug/execution/execute/ops/retrieve/unfold.h"

#include <google/protobuf/wrappers.pb.h>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/unfold.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {
class UnfoldOpr : public IOperator {
 public:
  explicit UnfoldOpr(const physical::Unfold& opr)
      : opr_(opr), tag_(opr.tag().value()), alias_(opr.alias().value()) {}

  std::string get_operator_name() const override { return "UnfoldOpr"; }

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::IStorageInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    return Unfold::unfold(std::move(ctx), tag_, alias_);
  }

 private:
  physical::Unfold opr_;
  int tag_;
  int alias_;
};

gs::result<OpBuildResultT> UnfoldOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  int alias = plan.query_plan().plan(op_idx).opr().unfold().alias().value();
  ret_meta.set(alias);
  return std::make_pair(std::make_unique<UnfoldOpr>(
                            plan.query_plan().plan(op_idx).opr().unfold()),
                        ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs