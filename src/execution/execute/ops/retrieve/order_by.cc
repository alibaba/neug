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

#include "neug/execution/execute/ops/retrieve/order_by.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/order_by.h"
#include "neug/execution/execute/ops/retrieve/order_by_utils.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

class OrderByOpr : public IOperator {
 public:
  OrderByOpr(std::vector<std::pair<int32_t, bool>> keys, int lower, int upper)
      : keys_(std::move(keys)), lower_(lower), upper_(upper) {}

  std::string get_operator_name() const override { return "OrderByOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    int keys_num = keys_.size();
    GeneralComparer cmp;
    for (int i = 0; i < keys_num; ++i) {
      cmp.add_keys(ctx.get(keys_[i].first), keys_[i].second);
    }
    std::vector<size_t> indices;
    int32_t tag = keys_[0].first;
    bool order = keys_[0].second;
    if (ctx.get(tag)->order_by_limit(order, upper_, indices)) {
      return OrderBy::staged_order_by_with_limit<GeneralComparer>(
          graph, std::move(ctx), cmp, lower_, upper_, indices);
    }

    return OrderBy::order_by_with_limit<GeneralComparer>(graph, std::move(ctx),
                                                         cmp, lower_, upper_);
  }

 private:
  std::vector<std::pair<int32_t, bool>> keys_;

  int lower_;
  int upper_;
};

neug::result<OpBuildResultT> OrderByOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto opr = plan.plan(op_idx).opr().order_by();
  int lower = 0;
  int upper = std::numeric_limits<int>::max();
  if (opr.has_limit()) {
    lower = std::max(lower, static_cast<int>(opr.limit().lower()));
    upper = std::min(upper, static_cast<int>(opr.limit().upper()));
  }
  int keys_num = opr.pairs_size();
  if (keys_num == 0) {
    LOG(ERROR) << "keys_num should be greater than 0";
    return std::make_pair(nullptr, ret_meta);
  }
  std::vector<std::pair<int32_t, bool>> keys;

  for (int i = 0; i < keys_num; ++i) {
    const auto& pair = opr.pairs(i);
    if (pair.order() != algebra::OrderBy_OrderingPair_Order::
                            OrderBy_OrderingPair_Order_ASC &&
        pair.order() != algebra::OrderBy_OrderingPair_Order::
                            OrderBy_OrderingPair_Order_DESC) {
      LOG(ERROR) << "order should be asc or desc";
      return std::make_pair(nullptr, ret_meta);
    }
    bool asc =
        pair.order() ==
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
    int32_t tag_id = pair.key().has_tag() ? pair.key().tag().id() : -1;
    keys.emplace_back(tag_id, asc);
  }

  return std::make_pair(
      std::make_unique<OrderByOpr>(std::move(keys), lower, upper), ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug