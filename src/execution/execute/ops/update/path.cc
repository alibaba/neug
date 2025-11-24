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

#include "neug/execution/execute/ops/update/path.h"

#include "neug/execution/common/operators/update/path_expand.h"
#include "neug/execution/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

class UPathExpandVOpr : public IOperator {
 public:
  explicit UPathExpandVOpr(const PathExpandParams& params) : params_(params) {}
  ~UPathExpandVOpr() override = default;

  std::string get_operator_name() const override { return "UPathExpandVOpr"; }

  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    return UPathExpand::path_expand_v(
        dynamic_cast<StorageUpdateInterface&>(graph), std::move(ctx), params_);
  }

 private:
  PathExpandParams params_;
};

gs::result<OpBuildResultT> UPathExpandVOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  LOG(INFO) << "Building UPathExpandVOpr";
  const auto& opr = plan.query_plan().plan(op_idx).opr().path();
  const auto& next_opr = plan.query_plan().plan(op_idx + 1).opr().vertex();
  if (opr.result_opt() ==
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
      opr.base().edge_expand().expand_opt() ==
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    int alias = -1;
    if (next_opr.has_alias()) {
      alias = next_opr.alias().value();
    }
    int start_tag = opr.has_start_tag() ? opr.start_tag().value() : -1;
    if (opr.path_opt() !=
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY) {
      LOG(ERROR) << "Currently only support arbitrary path expand";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (opr.is_optional()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    Direction dir = parse_direction(opr.base().edge_expand().direction());
    if (opr.base().edge_expand().is_optional()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    const algebra::QueryParams& query_params =
        opr.base().edge_expand().params();
    PathExpandParams pep;
    pep.alias = alias;
    pep.dir = dir;
    pep.hop_lower = opr.hop_range().lower();
    pep.hop_upper = opr.hop_range().upper();
    pep.start_tag = start_tag;
    pep.labels =
        parse_label_triplets(plan.query_plan().plan(op_idx).meta_data(0));
    if (opr.base().edge_expand().expand_opt() !=
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      LOG(ERROR) << "Currently only support vertex expand";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (query_params.has_predicate()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    return std::make_pair(std::make_unique<UPathExpandVOpr>(pep),
                          ContextMeta());
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
