#pragma once

#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug {
namespace planner {

class LogicalDummyScan final : public LogicalOperator {
 public:
  explicit LogicalDummyScan(bool updateClause = false)
      : LogicalOperator{LogicalOperatorType::DUMMY_SCAN},
        updateClause(updateClause) {}

  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  inline std::string getExpressionsForPrinting() const override {
    return std::string();
  }

  inline bool isUpdateClause() const { return updateClause; }

  static std::shared_ptr<binder::Expression> getDummyExpression();

  inline std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalDummyScan>();
  }

 private:
  bool updateClause;
};

}  // namespace planner
}  // namespace neug
