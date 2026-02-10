#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug {
namespace planner {

class LogicalProjection : public LogicalOperator {
 public:
  explicit LogicalProjection(binder::expression_vector expressions,
                             std::shared_ptr<LogicalOperator> child)
      : LogicalOperator{LogicalOperatorType::PROJECTION, std::move(child)},
        expressions{std::move(expressions)},
        isAppend{false} {}

  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  inline std::string getExpressionsForPrinting() const override {
    auto result = binder::ExpressionUtil::toString(expressions);
    // result += ", Cardinality: " + std::to_string(cardinality);
    if (isAppend) {
      result += ", APPEND";
    }
    return result;
  }

  inline binder::expression_vector getExpressionsToProject() const {
    return expressions;
  }

  inline binder::expression_vector& getExpressionsToProjectRef() {
    return expressions;
  }

  std::unordered_set<uint32_t> getDiscardedGroupsPos() const;

  std::unique_ptr<LogicalOperator> copy() override {
    auto projection =
        make_unique<LogicalProjection>(expressions, children[0]->copy());
    projection->setAppend(isAppend);
    return projection;
  }

  void resetExprUniqueNames();

  void setAppend(bool isAppend) { this->isAppend = isAppend; }

  bool append() const { return isAppend; }

 private:
  binder::expression_vector expressions;
  bool isAppend;
};

}  // namespace planner
}  // namespace neug
