#pragma once

#include <memory>
#include "logical_operator.h"

namespace neug {
namespace planner {

class LogicalUnion : public LogicalOperator {
 public:
  LogicalUnion(binder::expression_vector expressions,
               const std::vector<std::shared_ptr<LogicalOperator>>& children)
      : LogicalOperator{LogicalOperatorType::UNION_ALL, children},
        expressionsToUnion{std::move(expressions)},
        preQuery(false) {}

  f_group_pos_set getGroupsPosToFlatten(uint32_t childIdx);

  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  std::string getExpressionsForPrinting() const override {
    return std::string{};
  }

  binder::expression_vector getExpressionsToUnion() const {
    return expressionsToUnion;
  }

  Schema* getSchemaBeforeUnion(uint32_t idx) const {
    return children[idx]->getSchema();
  }

  std::unique_ptr<LogicalOperator> copy() override;

  std::unique_ptr<LogicalOperator> copyWithSchema();

  void setPreQuery(bool preQuery) { this->preQuery = preQuery; }

  bool getPreQuery() const { return preQuery; }

 private:
  // If an expression to union has different flat/unflat state in different
  // child, we need to flatten that expression in all the single queries.
  bool requireFlatExpression(uint32_t expressionIdx);

 private:
  binder::expression_vector expressionsToUnion;
  bool preQuery;
};

}  // namespace planner
}  // namespace neug
