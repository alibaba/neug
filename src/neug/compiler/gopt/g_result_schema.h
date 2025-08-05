#include <yaml-cpp/node/node.h>
#include <memory>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"
#include "neug/compiler/gopt/g_type_utils.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/utils/exception/exception.h"

#pragma once
namespace gs {
namespace gopt {
class GResultSchema {
 public:
  static inline YAML::Node infer(const planner::LogicalPlan& plan,
                                 std::shared_ptr<GAliasManager> manager,
                                 catalog::Catalog* catalog) {
    auto schema = plan.getSchema();
    if (!schema) {
      throw exception::Exception("Cannot infer schema from logical plan");
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
      auto& nodeExpr = expr.constCast<binder::NodeExpression>();
      GNodeType nodeType{nodeExpr};
      return nodeType.toYAML();
    } else if (type.getLogicalTypeID() == common::LogicalTypeID::REL) {
      auto& relExpr = expr.constCast<binder::RelExpression>();
      GRelType relType{relExpr};
      return relType.toYAML(catalog);
    } else {
      return GTypeUtils::toYAML(type);
    }
  }

  static inline bool inferFromExpr(const planner::LogicalPlan& plan) {
    GPhysicalAnalyzer analyzer;
    auto mode = analyzer.analyze(plan);
    if (mode == PhysicalMode::DDL) {
      return false;
    } else if (mode == PhysicalMode::READ_WRITE) {
      auto opType = plan.getLastOperator()->getOperatorType();
      if (opType == planner::LogicalOperatorType::COPY_FROM ||
          opType == planner::LogicalOperatorType::INSERT ||
          opType == planner::LogicalOperatorType::SET_PROPERTY ||
          opType == planner::LogicalOperatorType::DELETE) {
        return false;
      }
    } else {
      auto opType = plan.getLastOperator()->getOperatorType();
      return opType == planner::LogicalOperatorType::COPY_TO ? false : true;
    }
    return false;
  }
};
}  // namespace gopt
}  // namespace gs