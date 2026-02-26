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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/optimizer/project_join_condition_optimizer.h"

#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

using namespace neug::planner;
using namespace neug::binder;

namespace neug {
namespace optimizer {

void ProjectJoinConditionOptimizer::rewrite(LogicalPlan* plan) {
  visitOperator(plan->getLastOperator().get());
}

void ProjectJoinConditionOptimizer::visitOperator(LogicalOperator* op) {
  visitOperatorSwitch(op);
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    visitOperator(op->getChild(i).get());
  }
}

std::unique_ptr<common::LogicalType> ProjectJoinConditionOptimizer::getDataType(
    const std::string& uniqueVarName, LogicalOperator* op) {
  if (op->getOperatorType() == planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    auto scan = op->cast<planner::LogicalScanNodeTable>();
    if (uniqueVarName == scan.getAliasName()) {
      auto nodeType = scan.getNodeType(ctx->getCatalog());
      auto fields = std::vector<common::StructField>();
      auto extraInfo = std::make_unique<common::GNodeTypeInfo>(
          std::move(fields), std::move(nodeType));
      auto dataType =
          std::make_unique<common::LogicalType>(common::LogicalTypeID::NODE);
      dataType->setExtraTypeInfo(std::move(extraInfo));
      return dataType;
    }
  } else if (op->getOperatorType() == planner::LogicalOperatorType::GET_V) {
    auto& getV = op->cast<planner::LogicalGetV>();
    if (uniqueVarName == getV.getAliasName()) {
      auto nodeType = getV.getNodeType(ctx->getCatalog());
      auto fields = std::vector<common::StructField>();
      auto extraInfo = std::make_unique<common::GNodeTypeInfo>(
          std::move(fields), std::move(nodeType));
      auto dataType =
          std::make_unique<common::LogicalType>(common::LogicalTypeID::NODE);
      dataType->setExtraTypeInfo(std::move(extraInfo));
      return dataType;
    }
  } else if (op->getOperatorType() == planner::LogicalOperatorType::EXTEND) {
    auto& extend = op->cast<planner::LogicalExtend>();
    auto nbrNode = extend.getNbrNode();
    if (extend.getExtendOpt() == planner::ExtendOpt::VERTEX &&
        uniqueVarName == nbrNode->getUniqueName()) {
      return std::make_unique<common::LogicalType>(
          nbrNode->getDataType().copy());
    }
  } else {
    auto schema = op->getSchema();
    if (schema) {
      for (auto& expr : schema->getExpressionsInScope()) {
        if (expr->getUniqueName() == uniqueVarName) {
          return std::make_unique<common::LogicalType>(
              expr->getDataType().copy());
        }
      }
    }
  }
  for (auto child : op->getChildren()) {
    auto dataType = getDataType(uniqueVarName, child.get());
    if (dataType) {
      return dataType;
    }
  }
  return nullptr;
}

bool containsAliasName(const std::vector<gopt::GAliasName>& aliasNames,
                       const std::string& targetAlias) {
  for (auto& aliasName : aliasNames) {
    if (aliasName.uniqueName == targetAlias) {
      return true;
    }
  }
  return false;
}

void ProjectJoinConditionOptimizer::visitHashJoin(LogicalOperator* op) {
  auto& hashJoin = op->cast<LogicalHashJoin>();
  auto& joinConditions = hashJoin.getJoinConditionsRef();

  if (joinConditions.empty()) {
    return;
  }

  auto probeAliasNames = std::vector<gopt::GAliasName>();
  gopt::GAliasManager::extractGAliasNames(*op->getChild(0), probeAliasNames);

  auto buildAliasNames = std::vector<gopt::GAliasName>();
  gopt::GAliasManager::extractGAliasNames(*op->getChild(1), buildAliasNames);

  // convert join conditions: convert internal id to node it self in that NeuG
  // can support join by nodes directly
  for (auto& [probeKey, buildKey] : joinConditions) {
    if (probeKey->getDataType().getLogicalTypeID() ==
            common::LogicalTypeID::INTERNAL_ID &&
        !containsAliasName(probeAliasNames, probeKey->getUniqueName())) {
      // convert internal id to node it self
      auto probeIDExpr = probeKey->ptrCast<binder::PropertyExpression>();
      auto probeVarName = probeIDExpr->getVariableName();
      auto probeType = getDataType(probeVarName, op->getChild(0).get());
      CHECK(probeType) << "Fail to get probe type with variable name: "
                       << probeVarName;
      auto varExpr = std::make_shared<binder::VariableExpression>(
          probeType->copy(), probeVarName, probeVarName);
      probeKey = varExpr;
    }
    if (buildKey->getDataType().getLogicalTypeID() ==
            common::LogicalTypeID::INTERNAL_ID &&
        !containsAliasName(buildAliasNames, buildKey->getUniqueName())) {
      // convert internal id to node it self
      auto buildIDExpr = buildKey->ptrCast<binder::PropertyExpression>();
      auto buildVarName = buildIDExpr->getVariableName();
      auto buildType = getDataType(buildVarName, op->getChild(1).get());
      CHECK(buildType) << "Fail to get build type with variable name: "
                       << buildVarName;
      auto varExpr = std::make_shared<binder::VariableExpression>(
          buildType->copy(), buildVarName, buildVarName);
      buildKey = varExpr;
    }
  }

  auto hasPkJoin =
      gopt::GPhysicalAnalyzer::getScanFromPKJoin(ctx->getCatalog(), op);
  if (hasPkJoin) {
    return;
  }

  // Collect expressions from join conditions that need to be projected
  binder::expression_vector probeExpressions;
  binder::expression_vector buildExpressions;

  for (const auto& [probeKey, buildKey] : joinConditions) {
    if (probeKey->expressionType == common::ExpressionType::PROPERTY &&
        !containsAliasName(probeAliasNames, probeKey->getUniqueName())) {
      probeExpressions.push_back(probeKey);
    }
    if (buildKey->expressionType == common::ExpressionType::PROPERTY &&
        !containsAliasName(buildAliasNames, buildKey->getUniqueName())) {
      buildExpressions.push_back(buildKey);
    }
  }

  // Add projections before join if needed
  // Probe side (left child, index 0)
  if (!probeExpressions.empty()) {
    addProjectionBeforeJoin(op, 0, probeExpressions);
  }

  // Build side (right child, index 1)
  if (!buildExpressions.empty()) {
    addProjectionBeforeJoin(op, 1, buildExpressions);
  }
}

void ProjectJoinConditionOptimizer::collectExpressionsFromJoinConditions(
    const std::vector<binder::expression_pair>& joinConditions,
    binder::expression_vector& expressions) {
  for (const auto& [probeKey, buildKey] : joinConditions) {
    expressions.push_back(probeKey);
    expressions.push_back(buildKey);
  }
}

void ProjectJoinConditionOptimizer::addProjectionBeforeJoin(
    LogicalOperator* op, common::idx_t childIdx,
    const binder::expression_vector& expressions, bool isAppend) {
  if (expressions.empty()) {
    return;
  }
  auto child = op->getChild(childIdx);
  // Create a new projection operator
  auto projection = std::make_shared<LogicalProjection>(expressions, child);
  projection->setAppend(isAppend);
  projection->computeFlatSchema();
  op->setChild(childIdx, projection);
}

}  // namespace optimizer
}  // namespace neug
