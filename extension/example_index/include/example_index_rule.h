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

#pragma once

#include <memory>

#include "neug/compiler/optimizer/logical_rule.h"

namespace neug::extension::example_index {

class ExampleIndexRule final : public optimizer::LogicalRule {
 public:
  static constexpr const char* name = "EXAMPLE_INDEX_RULE";

  void rewrite(main::ClientContext* ctx, planner::LogicalPlan* plan) override {
    clientContext_ = ctx;
    LogicalRule::rewrite(ctx, plan);
  }

  std::shared_ptr<planner::LogicalOperator> visitScanNodeTableReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  main::ClientContext* clientContext_ = nullptr;
};

}  // namespace neug::extension::example_index
