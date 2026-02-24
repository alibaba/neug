#include <iostream>
#include <memory>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/binder/visitor/property_collector.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/planner/operator/logical_alias_map.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_union.h"
#include "neug/compiler/planner/planner.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

static std::vector<std::vector<std::unique_ptr<LogicalPlan>>>
cartesianProductChildrenPlans(
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>>
        childrenLogicalPlans) {
  std::vector<std::vector<std::unique_ptr<LogicalPlan>>> resultChildrenPlans;
  for (auto& childLogicalPlans : childrenLogicalPlans) {
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>>
        curChildResultLogicalPlans;
    for (auto& childLogicalPlan : childLogicalPlans) {
      if (resultChildrenPlans.empty()) {
        std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
        logicalPlans.push_back(childLogicalPlan->shallowCopy());
        curChildResultLogicalPlans.push_back(std::move(logicalPlans));
      } else {
        for (auto& resultChildPlans : resultChildrenPlans) {
          std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
          logicalPlans.reserve(resultChildPlans.size());
          for (auto& resultChildPlan : resultChildPlans) {
            logicalPlans.push_back(resultChildPlan->shallowCopy());
          }
          logicalPlans.push_back(childLogicalPlan->shallowCopy());
          curChildResultLogicalPlans.push_back(std::move(logicalPlans));
        }
      }
    }
    resultChildrenPlans = std::move(curChildResultLogicalPlans);
  }
  return resultChildrenPlans;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::planQuery(
    const BoundStatement& boundStatement) {
  std::vector<std::unique_ptr<LogicalPlan>> resultPlans;
  auto& regularQuery = (BoundRegularQuery&) boundStatement;
  if (regularQuery.getNumSingleQueries() == 1) {
    resultPlans = planSingleQuery(regularQuery.getSingleQuery(0));
  } else {
    auto preQueryPlans = getInitialEmptyPlans();
    for (auto& part : regularQuery.getPreQueryPart()) {
      preQueryPlans = planQueryPart(&part, std::move(preQueryPlans));
    }
    bool preQueryPart = !regularQuery.getPreQueryPart().empty();

    if (preQueryPart) {
      this->preQueryPlan = std::move(preQueryPlans.at(0)->shallowCopy());
    }

    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans(
        regularQuery.getNumSingleQueries());
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
      childrenLogicalPlans[i] = planSingleQuery(regularQuery.getSingleQuery(i));
    }

    this->preQueryPlan = nullptr;

    auto childrenPlans =
        cartesianProductChildrenPlans(std::move(childrenLogicalPlans));
    for (auto& childrenPlan : childrenPlans) {
      resultPlans.push_back(
          createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0)));
    }
    if (preQueryPart) {
      std::vector<std::unique_ptr<LogicalPlan>> cartesianProductPrePlans;
      for (auto& preQueryPlan : preQueryPlans) {
        for (auto& resultPlan : resultPlans) {
          if (resultPlan->getLastOperator() &&
              resultPlan->getLastOperator()->getOperatorType() ==
                  LogicalOperatorType::UNION_ALL) {
            auto unionOp =
                resultPlan->getLastOperator()->ptrCast<LogicalUnion>();
            auto unionCopy = std::make_unique<LogicalUnion>(
                unionOp->getExpressionsToUnion(), unionOp->getChildren());
            unionCopy->computeFactorizedSchema();
            unionCopy->insertChild(0, preQueryPlan->getLastOperator());
            unionCopy->setPreQuery(true);
            auto unionPlan = std::make_unique<LogicalPlan>();
            unionPlan->setLastOperator(std::move(unionCopy));
            unionPlan->setCost(resultPlan->getCost());
            cartesianProductPrePlans.push_back(std::move(unionPlan));
          } else {
            THROW_EXCEPTION_WITH_FILE_LINE("result plan should be union");
          }
        }
      }
      resultPlans = std::move(cartesianProductPrePlans);
    }
    if (regularQuery.getPostSingleQuery()) {
      auto postQuery = regularQuery.getPostSingleQuery();
      resultPlans = planSingleQuery(postQuery, std::move(resultPlans));
    }
  }
  return resultPlans;
}

std::unique_ptr<LogicalPlan> Planner::getBestPlan(
    std::vector<std::unique_ptr<LogicalPlan>> plans) {
  auto bestPlan = std::move(plans[0]);
  for (auto i = 1u; i < plans.size(); ++i) {
    if (plans[i]->getCost() < bestPlan->getCost()) {
      bestPlan = std::move(plans[i]);
    }
  }
  return bestPlan;
}

std::unique_ptr<LogicalPlan> Planner::createUnionPlan(
    std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
  NEUG_ASSERT(!childrenPlans.empty());
  auto plan = std::make_unique<LogicalPlan>();
  std::vector<std::shared_ptr<LogicalOperator>> children;
  children.reserve(childrenPlans.size());
  for (auto& childPlan : childrenPlans) {
    children.push_back(childPlan->getLastOperator());
  }
  // we compute the schema based on first child
  auto union_ = make_shared<LogicalUnion>(
      childrenPlans[0]->getSchema()->getExpressionsInScope(),
      std::move(children));
  for (auto i = 0u; i < childrenPlans.size(); ++i) {
    appendFlattens(union_->getGroupsPosToFlatten(i), *childrenPlans[i]);
    union_->setChild(i, childrenPlans[i]->getLastOperator());
  }
  union_->computeFactorizedSchema();
  plan->setLastOperator(union_);
  if (!isUnionAll) {
    appendDistinct(union_->getExpressionsToUnion(), *plan);
  }
  return plan;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getInitialEmptyPlans() {
  std::vector<std::unique_ptr<LogicalPlan>> plans;
  plans.push_back(std::make_unique<LogicalPlan>());
  return plans;
}

expression_vector Planner::getProperties(const Expression& pattern) const {
  NEUG_ASSERT(pattern.expressionType == ExpressionType::PATTERN);
  return propertyExprCollection.getProperties(pattern);
}

JoinOrderEnumeratorContext Planner::enterNewContext() {
  auto prevContext = std::move(context);
  context = JoinOrderEnumeratorContext();
  return prevContext;
}

void Planner::exitContext(JoinOrderEnumeratorContext prevContext) {
  context = std::move(prevContext);
}

PropertyExprCollection Planner::enterNewPropertyExprCollection() {
  auto prevCollection = std::move(propertyExprCollection);
  propertyExprCollection = PropertyExprCollection();
  return prevCollection;
}

void Planner::exitPropertyExprCollection(PropertyExprCollection collection) {
  propertyExprCollection = std::move(collection);
}

}  // namespace planner
}  // namespace neug
