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

#include "neug/execution/execute/ops/ddl/drop_index.h"

#include <string>

#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"

namespace neug::execution::ops {

class DropIndexOpr : public IOperator {
 public:
  DropIndexOpr(std::string indexName, bool ignore_conflict)
      : indexName_(std::move(indexName)), ignore_conflict_(ignore_conflict) {}

  std::string get_operator_name() const override { return "DropIndexOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap&,
                             Context&& ctx, OprTimer*) override {
    auto* indexInterface = dynamic_cast<StorageIndexDDLInterface*>(&graph);
    if (!indexInterface) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_NOT_SUPPORTED,
          "Current storage interface does not support index DDL");
    }

    auto status = indexInterface->DropIndex(indexName_);
    if (!status.ok()) {
      // The storage layer reports ERR_NOT_FOUND when the target index does
      // not exist; honor IF EXISTS in that case.
      if (ignore_conflict_ &&
          status.error_code() == StatusCode::ERR_NOT_FOUND) {
        return std::move(ctx);
      }
      RETURN_ERROR(status);
    }
    return std::move(ctx);
  }

 private:
  std::string indexName_;
  bool ignore_conflict_;
};

neug::result<OpBuildResultT> DropIndexOprBuilder::Build(
    const Schema&, const ContextMeta& ctxMeta,
    const physical::PhysicalPlan& plan, int opId) {
  const auto& dropIndex = plan.plan(opId).opr().drop_index();
  const bool ignore_conflict =
      !conflict_action_to_bool(dropIndex.conflict_action());
  return std::make_pair(
      std::make_unique<DropIndexOpr>(dropIndex.name(), ignore_conflict),
      ctxMeta);
}

}  // namespace neug::execution::ops
