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

#include <yaml-cpp/node/node.h>
#include <memory>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/compiler/gopt/g_type_utils.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/utils/exception/exception.h"

#pragma once
namespace neug {
namespace gopt {
class GResultSchema {
 public:
  static inline YAML::Node infer(const planner::LogicalPlan& plan,
                                 std::shared_ptr<GAliasManager> manager,
                                 catalog::Catalog* catalog) {
    auto schema = plan.getSchema();
    if (!schema) {
      THROW_EXCEPTION_WITH_FILE_LINE("Cannot infer schema from logical plan");
    }
    YAML::Node result;
    auto exprScope = schema->getExpressionsInScope();
    YAML::Node columns = YAML::Node(YAML::NodeType::Sequence);
    if (inferFromExpr(plan)) {
      for (auto& expr : exprScope) {
        auto aliasId = manager->getAliasId(expr->getUniqueName());
        auto gAlias = manager->getGAliasName(aliasId);
        std::string columnName;
        if (gAlias.queryName
                .has_value()) {  // return query given alias if exists
          columnName = gAlias.queryName.value();
        } else {  // use system built-in name as alias
          columnName = gAlias.uniqueName;
        }
        YAML::Node column;
        column["name"] = columnName;
        column["type"] = convertType(*expr, catalog);
        columns.push_back(column);
      }
    }
    result["returns"] = columns;
    return result;
  }

  static inline YAML::Node convertType(const binder::Expression& expr,
                                       catalog::Catalog* catalog) {
    auto& type = expr.getDataType();
    if (type.getLogicalTypeID() == common::LogicalTypeID::NODE) {
      auto nodeExpr = dynamic_cast<const binder::NodeExpression*>(&expr);
      if (nodeExpr) {
        GNodeType nodeType{*nodeExpr};
        return nodeType.toYAML();
      } else {
        GNodeType emptyNode({});
        return emptyNode.toYAML();
      }
    } else if (type.getLogicalTypeID() == common::LogicalTypeID::REL) {
      auto relExpr = dynamic_cast<const binder::RelExpression*>(&expr);
      if (relExpr) {
        GRelType relType{*relExpr};
        return relType.toYAML(catalog);
      } else {
        GRelType emptyRel({});
        return emptyRel.toYAML(catalog);
      }
    } else {
      return GTypeUtils::toYAML(type);
    }
  }

  static inline bool inferFromExpr(const planner::LogicalPlan& plan) {
    auto opType = plan.getLastOperator()->getOperatorType();
    if (opType == planner::LogicalOperatorType::COPY_FROM ||
        opType == planner::LogicalOperatorType::INSERT ||
        opType == planner::LogicalOperatorType::SET_PROPERTY ||
        opType == planner::LogicalOperatorType::DELETE ||
        opType == planner::LogicalOperatorType::COPY_TO ||
        opType == planner::LogicalOperatorType::TRANSACTION ||
        opType == planner::LogicalOperatorType::EXTENSION ||
        opType == planner::LogicalOperatorType::CREATE_TABLE ||
        opType == planner::LogicalOperatorType::DROP ||
        opType == planner::LogicalOperatorType::ALTER) {
      return false;
    }
    return true;
  }
};
}  // namespace gopt
}  // namespace neug