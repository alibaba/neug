#include <iostream>
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/binder/query/reading_clause/bound_load_from.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"
#include "neug/compiler/binder/query/reading_clause/bound_table_function_call.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

void Planner::planReadingClause(
    const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
  switch (readingClause.getClauseType()) {
  case ClauseType::MATCH: {
    planMatchClause(readingClause, prevPlans);
  } break;
  case ClauseType::UNWIND: {
    planUnwindClause(readingClause, prevPlans);
  } break;
  case ClauseType::TABLE_FUNCTION_CALL: {
    planTableFunctionCall(readingClause, prevPlans);
  } break;
  case ClauseType::LOAD_FROM: {
    planLoadFrom(readingClause, prevPlans);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void Planner::planMatchClause(
    const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
  auto& boundMatchClause = readingClause.constCast<BoundMatchClause>();
  auto queryGraphCollection = boundMatchClause.getQueryGraphCollection();
  auto predicates = boundMatchClause.getConjunctivePredicates();
  switch (boundMatchClause.getMatchClauseType()) {
  case MatchClauseType::MATCH: {
    if (plans.size() == 1 && plans[0]->isEmpty()) {
      auto info = QueryGraphPlanningInfo();
      if (this->preQueryPlan) {
        // set query plan info for each subquery in union, the info contains the
        // common expressions in pre query and the cardinality of the pre query
        // plan
        NEUG_ASSERT(this->preQueryPlan->getSchema());
        auto correlatedExprs = getCorrelatedExprs(
            *queryGraphCollection, {}, this->preQueryPlan->getSchema());
        auto joinNodeIDs = ExpressionUtil::getExpressionsWithDataType(
            correlatedExprs, LogicalTypeID::INTERNAL_ID);
        info.corrExprs = joinNodeIDs;
        info.subqueryType = SubqueryPlanningType::COMMON_PAT_REUSE;
        info.corrExprsCard = this->preQueryPlan->getCardinality();
      }
      info.predicates = predicates;
      info.hint = boundMatchClause.getHint();
      plans = enumerateQueryGraphCollection(*queryGraphCollection, info);
    } else {
      for (auto& plan : plans) {
        planRegularMatch(*queryGraphCollection, predicates, *plan,
                         boundMatchClause.getHint());
      }
    }
  } break;
  case MatchClauseType::OPTIONAL_MATCH: {
    for (auto& plan : plans) {
      planOptionalMatch(*queryGraphCollection, predicates, *plan,
                        boundMatchClause.getHint());
    }
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void Planner::planUnwindClause(
    const BoundReadingClause& boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
  for (auto& plan : plans) {
    if (plan->isEmpty()) {  // UNWIND [1, 2, 3, 4] AS x RETURN x
      appendDummyScan(*plan);
    }
    appendUnwind(boundReadingClause, *plan);
  }
}

class PredicatesDependencyAnalyzer {
 public:
  explicit PredicatesDependencyAnalyzer(
      const expression_vector& outputColumns) {
    for (auto& column : outputColumns) {
      columnNameSet.insert(column->getUniqueName());
    }
  }

  void analyze(const expression_vector& predicates) {
    predicatesDependsOnlyOnOutputColumns.clear();
    predicatesWithOtherDependencies.clear();
    for (auto& predicate : predicates) {
      if (hasExternalDependency(predicate)) {
        predicatesWithOtherDependencies.push_back(predicate);
      } else {
        predicatesDependsOnlyOnOutputColumns.push_back(predicate);
      }
    }
  }

 private:
  bool hasExternalDependency(const std::shared_ptr<Expression>& expression) {
    auto collector = DependentVarNameCollector();
    collector.visit(expression);
    for (auto& name : collector.getVarNames()) {
      if (!columnNameSet.contains(name)) {
        return true;
      }
    }
    return false;
  }

 public:
  expression_vector predicatesDependsOnlyOnOutputColumns;
  expression_vector predicatesWithOtherDependencies;

 private:
  std::unordered_set<std::string> columnNameSet;
};

void Planner::planTableFunctionCall(
    const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
  auto& boundCall = readingClause.constCast<BoundTableFunctionCall>();
  if (boundCall.getBindData()) {
    auto analyzer =
        PredicatesDependencyAnalyzer(boundCall.getBindData()->columns);
    analyzer.analyze(boundCall.getConjunctivePredicates());
    NEUG_ASSERT(boundCall.getTableFunc().getLogicalPlanFunc);
    boundCall.getTableFunc().getLogicalPlanFunc(
        this, readingClause, analyzer.predicatesDependsOnlyOnOutputColumns,
        plans);
    if (!analyzer.predicatesWithOtherDependencies.empty()) {
      for (auto& plan : plans) {
        appendFilters(analyzer.predicatesWithOtherDependencies, *plan);
      }
    }
  } else {
    appendTableFunctionCall(boundCall.getTableScanInfo(), *plans[0]);
  }
}

void Planner::planLoadFrom(const BoundReadingClause& readingClause,
                           std::vector<std::unique_ptr<LogicalPlan>>& plans) {
  auto& loadFrom = readingClause.constCast<BoundLoadFrom>();
  for (auto& plan : plans) {
    auto op = getTableFunctionCall(*loadFrom.getInfo());
    auto predicate = loadFrom.getPredicate();
    if (predicate) {
      auto funcCall = op->ptrCast<planner::LogicalTableFunctionCall>();
      auto bindData = funcCall->getBindData();
      ResetVarUseNameVisitor visitor(true);
      visitor.visit(predicate);
      bindData->setRowSkips(std::move(predicate));
    }
    plan->setLastOperator(std::move(op));
  }
}

void Planner::planReadOp(std::shared_ptr<LogicalOperator> op,
                         const expression_vector& predicates,
                         LogicalPlan& plan) {
  if (!plan.isEmpty()) {
    auto tmpPlan = LogicalPlan();
    tmpPlan.setLastOperator(std::move(op));
    if (!predicates.empty()) {
      appendFilters(predicates, tmpPlan);
    }
    appendCrossProduct(plan, tmpPlan, plan);
  } else {
    plan.setLastOperator(std::move(op));
    if (!predicates.empty()) {
      appendFilters(predicates, plan);
    }
  }
}

}  // namespace planner
}  // namespace neug
