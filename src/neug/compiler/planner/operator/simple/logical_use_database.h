#pragma once

#include "logical_database.h"

namespace gs {
namespace planner {

class LogicalUseDatabase final : public LogicalDatabase {
 public:
  explicit LogicalUseDatabase(
      std::string dbName, std::shared_ptr<binder::Expression> outputExpression)
      : LogicalDatabase{LogicalOperatorType::USE_DATABASE, outputExpression,
                        std::move(dbName)} {}

  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalUseDatabase>(dbName, outputExpression);
  }
};

}  // namespace planner
}  // namespace gs
