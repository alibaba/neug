#pragma once

#include "neug/compiler/planner/operator/logical_operator.h"

namespace gs {
namespace planner {

class LogicalDummyScan final : public LogicalOperator {
 public:
  explicit LogicalDummyScan()
      : LogicalOperator{LogicalOperatorType::DUMMY_SCAN} {}

  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  inline std::string getExpressionsForPrinting() const override {
    return std::string();
  }

  static std::shared_ptr<binder::Expression> getDummyExpression();

  inline std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalDummyScan>();
  }
};

}  // namespace planner
}  // namespace gs
