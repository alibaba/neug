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

#include "neug/execution/execute/ops/retrieve/dedup.h"

#include <stddef.h>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/dedup.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class DedupOpr : public IOperator {
 public:
  explicit DedupOpr(const std::vector<size_t>& tag_ids) : tag_ids_(tag_ids) {}
  std::string get_operator_name() const override { return "DedupOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    return Dedup::dedup(std::move(ctx), tag_ids_);
  }

  std::vector<size_t> tag_ids_;
};

neug::result<OpBuildResultT> DedupOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& dedup_opr = plan.plan(op_idx).opr().dedup();
  int keys_num = dedup_opr.keys_size();
  std::vector<size_t> keys;
  ContextMeta ret_meta;
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const auto& key = dedup_opr.keys(k_i);
    int tag = key.has_tag() ? key.tag().id() : -1;
    keys.emplace_back(tag);
    ret_meta.set(tag, ctx_meta.get(tag));
  }

  return std::make_pair(std::make_unique<DedupOpr>(keys), ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug