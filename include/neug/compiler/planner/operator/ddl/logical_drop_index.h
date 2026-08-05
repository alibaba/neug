#pragma once

#include <string>

#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug::planner {

class LogicalDropIndex final : public LogicalOperator {
 public:
  LogicalDropIndex(std::string indexName, bool ifExists)
      : LogicalOperator{LogicalOperatorType::DROP_INDEX},
        indexName{std::move(indexName)},
        ifExists{ifExists} {}

  void computeFactorizedSchema() override { createEmptySchema(); }
  void computeFlatSchema() override { createEmptySchema(); }
  std::string getExpressionsForPrinting() const override { return indexName; }
  const std::string& getIndexName() const { return indexName; }
  bool getIfExists() const { return ifExists; }
  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalDropIndex>(indexName, ifExists);
  }

 private:
  std::string indexName;
  bool ifExists;
};

}  // namespace neug::planner
