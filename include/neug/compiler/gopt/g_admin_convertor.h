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

#pragma once

#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/simple/logical_extension.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {
namespace gopt {
class GAdminConvertor {
 public:
  std::unique_ptr<::physical::AdminPlan> convert(
      const planner::LogicalPlan& plan);

 private:
  void convertOperator(::physical::AdminPlan* plan,
                       const planner::LogicalOperator& op);

  std::unique_ptr<::physical::AdminPlan::Operator> convertCheckpoint(
      const planner::LogicalOperator& op);

  std::unique_ptr<::physical::AdminPlan::Operator> convertExtension(
      const planner::LogicalOperator& op);
};
}  // namespace gopt
}  // namespace gs