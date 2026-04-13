/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/compiler/optimizer/project_into_data_source_optimizer.h"

#include <algorithm>
#include <memory>
#include <string>

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/compiler/gopt/g_result_schema.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"

#include <glog/logging.h>

using namespace neug::planner;

namespace neug {
namespace optimizer {

void ProjectIntoDataSourceOptimizer::rewrite(LogicalPlan* plan) {
  if (!plan || !plan->getLastOperator()) {
    return;
  }
  this->plan = plan;
  plan->setLastOperator(visitOperator(plan->getLastOperator()));
}

planner::LogicalOperator* getTailProject(LogicalPlan* plan) {
  auto op = plan->getLastOperator();
  while (op) {
    if (op->getOperatorType() == LogicalOperatorType::PROJECTION) {
      break;
    }
    if (op->getNumChildren() == 0) {
      break;
    }
    op = op->getChild(0);
  }
  return (op && op->getOperatorType() == LogicalOperatorType::PROJECTION)
             ? op.get()
             : nullptr;
}

std::shared_ptr<planner::LogicalOperator>
ProjectIntoDataSourceOptimizer::visitOperator(
    const std::shared_ptr<planner::LogicalOperator>& op) {
  // bottom-up traversal, so we can handle consecutive projections after data
  // source
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    op->setChild(i, visitOperator(op->getChild(i)));
  }
  // if there is a non-copy operation after LOAD FROM, batch_read=true will not
  // work, batch_read=true will produce a special ArrowStreamContext, which can
  // only be processed by copy from, other operators do not support it.
  if (op->getNumChildren() > 0 &&
      op->getChild(0)->getOperatorType() ==
          LogicalOperatorType::TABLE_FUNCTION_CALL) {
    auto tableFunc = op->getChild(0)->ptrCast<LogicalTableFunctionCall>();
    auto* scanBindData =
        dynamic_cast<function::ScanFileBindData*>(tableFunc->getBindData());
    if (scanBindData != nullptr) {
      auto& options = scanBindData->fileScanInfo.options;
      if (op->getOperatorType() != LogicalOperatorType::COPY_FROM) {
        options.insert_or_assign("BATCH_READ",
                                 common::Value::createValue(false));
      }
    }
  }
  return LogicalOperatorVisitor::visitOperatorReplaceSwitch(op);
}

std::shared_ptr<planner::LogicalOperator>
ProjectIntoDataSourceOptimizer::visitProjectionReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto projection = op->ptrCast<LogicalProjection>();
  if (projection->getNumChildren() == 0) {
    return op;
  }
  auto child = projection->getChild(0);
  if (child->getOperatorType() != LogicalOperatorType::TABLE_FUNCTION_CALL) {
    return op;
  }
  auto funcCall = std::dynamic_pointer_cast<LogicalTableFunctionCall>(child);
  auto scanBindData =
      dynamic_cast<function::ScanFileBindData*>(funcCall->getBindData());
  if (!scanBindData) {
    return op;
  }
  const auto& projectExprs = projection->getExpressionsToProject();
  const auto& allColumns = scanBindData->columns;
  std::vector<std::string> projectColumns;
  projectColumns.reserve(allColumns.size());
  // if expressions in project op are mismatched with all columns in external
  // file, then column remapping is required.
  bool reorder = false;
  for (size_t pos = 0; pos < projectExprs.size(); ++pos) {
    const auto& expr = projectExprs[pos];
    size_t targetPos = binder::ExpressionUtil::find(expr.get(), allColumns);
    // expression not existed in external file, skip column remapping
    if (targetPos == common::INVALID_IDX) {
      return op;
    }
    // mismatch with original column order, need to reorder columns
    if (targetPos != pos) {
      reorder = true;
    }
    projectColumns.push_back(expr->rawName());
  }
  if (reorder) {
    const auto& columnsAfterSkip = scanBindData->getProjectColumns();
    if (!columnsAfterSkip.empty() &&
        std::find_if(projectColumns.begin(), projectColumns.end(),
                     [&columnsAfterSkip](const std::string& column) {
                       return std::find(columnsAfterSkip.begin(),
                                        columnsAfterSkip.end(),
                                        column) == columnsAfterSkip.end();
                     }) != projectColumns.end()) {
      LOG(WARNING) << "columns to project is not a subset of columns after "
                      "skip, skip column remapping";
      return op;
    }
    scanBindData->setProjectColumns(std::move(projectColumns));
    funcCall->computeFlatSchema();
  }
  auto tailProject = getTailProject(plan);
  // keep tail projection if plan need to sink results, otherwise, remove it.
  if (gopt::GResultSchema::inferFromExpr(*plan) && tailProject == op.get()) {
    return op;
  }
  return funcCall;
}

}  // namespace optimizer
}  // namespace neug
