#include "neug/compiler/planner/operator/schema.h"

#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

f_group_pos Schema::createGroup() {
  auto pos = groups.size();
  groups.push_back(std::make_unique<FactorizationGroup>());
  return pos;
}

void Schema::insertToScope(const std::shared_ptr<Expression>& expression,
                           f_group_pos groupPos) {
  NEUG_ASSERT(!expressionNameToGroupPos.contains(expression->getUniqueName()));
  expressionNameToGroupPos.insert({expression->getUniqueName(), groupPos});
  // NEUG_ASSERT(getGroup(groupPos)->expressionNameToPos.contains(
  //     expression->getUniqueName()));
  expressionsInScope.push_back(expression);
}

void Schema::insertToGroupAndScope(
    const std::shared_ptr<Expression>& expression, f_group_pos groupPos) {
  // NEUG_ASSERT(!expressionNameToGroupPos.contains(expression->getUniqueName()));
  expressionNameToGroupPos.insert({expression->getUniqueName(), groupPos});
  groups[groupPos]->insertExpression(expression);
  expressionsInScope.push_back(expression);
}

void Schema::insertToScopeMayRepeat(
    const std::shared_ptr<Expression>& expression, uint32_t groupPos) {
  if (expressionNameToGroupPos.contains(expression->getUniqueName())) {
    return;
  }
  insertToScope(expression, groupPos);
}

void Schema::insertToGroupAndScopeMayRepeat(
    const std::shared_ptr<Expression>& expression, uint32_t groupPos) {
  if (expressionNameToGroupPos.contains(expression->getUniqueName())) {
    return;
  }
  insertToGroupAndScope(expression, groupPos);
}

void Schema::insertToGroupAndScope(const expression_vector& expressions,
                                   f_group_pos groupPos) {
  for (auto& expression : expressions) {
    insertToGroupAndScope(expression, groupPos);
  }
}

f_group_pos Schema::getGroupPos(const std::string& expressionName) const {
  // NEUG_ASSERT(expressionNameToGroupPos.contains(expressionName));
  if (expressionNameToGroupPos.contains(expressionName)) {
    return expressionNameToGroupPos.at(expressionName);
  } else {
    return 0;
  }
}

bool Schema::isExpressionInScope(const Expression& expression) const {
  for (auto& expressionInScope : expressionsInScope) {
    if (expressionInScope->getUniqueName() == expression.getUniqueName()) {
      return true;
    }

    if (expression.expressionType == common::ExpressionType::PROPERTY) {
      auto propertyExpr = expression.constCast<binder::PropertyExpression>();
      if (expressionInScope->getUniqueName() ==
          propertyExpr.getVariableName()) {
        return true;
      }
    }

    // for query `MATCH (a:person) WHERE a.gender = 1 WITH a AS k MATCH
    // (k)-[e:knows]->(b:person)`, `with a` will project a pattern expression of
    // the query node, but its schema does not contain the internal ID. the
    // following code is to handle this case and identify the join node by
    // comparing internal ID directly.
    if (expressionInScope->expressionType == common::ExpressionType::PATTERN &&
        expressionInScope->getDataType().getLogicalTypeID() ==
            common::LogicalTypeID::NODE &&
        expression.getDataType().getLogicalTypeID() ==
            common::LogicalTypeID::INTERNAL_ID) {
      auto nodeScope = expressionInScope->constPtrCast<NodeExpression>();
      if (nodeScope->getInternalID()->getUniqueName() ==
          expression.getUniqueName()) {
        return true;
      }
    }
  }
  return false;
}

expression_vector Schema::getExpressionsInScope(f_group_pos pos) const {
  expression_vector result;
  for (auto& expression : expressionsInScope) {
    if (getGroupPos(expression->getUniqueName()) == pos) {
      result.push_back(expression);
    }
  }
  return result;
}

bool Schema::evaluable(const Expression& expression) const {
  auto inScope = isExpressionInScope(expression);
  if (expression.expressionType == ExpressionType::LITERAL || inScope) {
    return true;
  }
  auto children = ExpressionChildrenCollector::collectChildren(expression);
  if (children.empty()) {
    return inScope;
  } else {
    for (auto& child : children) {
      if (!evaluable(*child)) {
        return false;
      }
    }
    return true;
  }
}

std::unordered_set<f_group_pos> Schema::getGroupsPosInScope() const {
  std::unordered_set<f_group_pos> result;
  for (auto& expressionInScope : expressionsInScope) {
    result.insert(getGroupPos(expressionInScope->getUniqueName()));
  }
  return result;
}

std::unique_ptr<Schema> Schema::copy() const {
  auto newSchema = std::make_unique<Schema>();
  newSchema->expressionNameToGroupPos = expressionNameToGroupPos;
  for (auto& group : groups) {
    newSchema->groups.push_back(std::make_unique<FactorizationGroup>(*group));
  }
  newSchema->expressionsInScope = expressionsInScope;
  return newSchema;
}

void Schema::clear() {
  groups.clear();
  clearExpressionsInScope();
}

size_t Schema::getNumGroups(bool isFlat) const {
  auto result = 0u;
  for (auto groupPos : getGroupsPosInScope()) {
    result += groups[groupPos]->isFlat() == isFlat;
  }
  return result;
}

f_group_pos SchemaUtils::getLeadingGroupPos(
    const std::unordered_set<f_group_pos>& groupPositions,
    const Schema& schema) {
  auto leadingGroupPos = INVALID_F_GROUP_POS;
  for (auto groupPos : groupPositions) {
    if (!schema.getGroup(groupPos)->isFlat()) {
      return groupPos;
    }
    leadingGroupPos = groupPos;
  }
  return leadingGroupPos;
}

void SchemaUtils::validateAtMostOneUnFlatGroup(
    const std::unordered_set<f_group_pos>& groupPositions,
    const Schema& schema) {
  auto hasUnFlatGroup = false;
  for (auto groupPos : groupPositions) {
    if (!schema.getGroup(groupPos)->isFlat()) {
      if (hasUnFlatGroup) {
        THROW_INTERNAL_EXCEPTION(
            "Unexpected multiple unFlat factorization groups found.");
      }
      hasUnFlatGroup = true;
    }
  }
}

void SchemaUtils::validateNoUnFlatGroup(
    const std::unordered_set<f_group_pos>& groupPositions,
    const Schema& schema) {
  for (auto groupPos : groupPositions) {
    if (!schema.getGroup(groupPos)->isFlat()) {
      THROW_INTERNAL_EXCEPTION("Unexpected unFlat factorization group found.");
    }
  }
}

}  // namespace planner
}  // namespace neug
