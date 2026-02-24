#include "neug/compiler/planner/operator/logical_limit.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace planner {

static uint64_t getLiteralNumber(std::shared_ptr<neug::binder::Expression> expr) {
  uint64_t number = common::INVALID_LIMIT;
  if (expr == nullptr) {
    return number;
  }
  auto value = binder::ExpressionUtil::evaluateAsLiteralValue(*expr);
  auto errorMsg =
      "The number of rows to skip/limit must be a non-negative integer.";
  common::TypeUtils::visit(
      value.getDataType(),
      [&]<common::IntegerTypes T>(T) {
        if (value.getValue<T>() < 0) {
          THROW_RUNTIME_ERROR(errorMsg);
        }
        number = (uint64_t) value.getValue<T>();
      },
      [&](auto) { THROW_RUNTIME_ERROR(errorMsg); });
  return number;
}

bool LogicalLimit::canEvaluateSkipNum() const {
  if (!skipNum) {
    return false;
  }
  return binder::ExpressionUtil::canEvaluateAsLiteral(*skipNum);
}

uint64_t LogicalLimit::evaluateSkipNum() const {
  return getLiteralNumber(skipNum);
}

bool LogicalLimit::canEvaluateLimitNum() const {
  if (!limitNum) {
    return false;
  }
  return binder::ExpressionUtil::canEvaluateAsLiteral(*limitNum);
}

uint64_t LogicalLimit::evaluateLimitNum() const {
  return getLiteralNumber(limitNum);
}

std::string LogicalLimit::getExpressionsForPrinting() const {
  std::string result;
  if (hasSkipNum()) {
    result += "SKIP ";
    if (canEvaluateSkipNum()) {
      result += std::to_string(evaluateSkipNum());
    }
  }
  if (hasLimitNum()) {
    if (!result.empty()) {
      result += ",";
    }
    result += "LIMIT ";
    if (canEvaluateLimitNum()) {
      result += std::to_string(evaluateLimitNum());
    }
  }
  return result;
}

f_group_pos_set LogicalLimit::getGroupsPosToFlatten() {
  auto childSchema = children[0]->getSchema();
  return FlattenAllButOne::getGroupsPosToFlatten(
      childSchema->getGroupsPosInScope(), *childSchema);
}

f_group_pos LogicalLimit::getGroupPosToSelect() const {
  auto childSchema = children[0]->getSchema();
  auto groupsPosInScope = childSchema->getGroupsPosInScope();
  SchemaUtils::validateAtMostOneUnFlatGroup(groupsPosInScope, *childSchema);
  return SchemaUtils::getLeadingGroupPos(groupsPosInScope, *childSchema);
}

}  // namespace planner
}  // namespace neug
