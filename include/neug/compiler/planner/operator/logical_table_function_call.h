#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug {
namespace planner {

class NEUG_API LogicalTableFunctionCall final : public LogicalOperator {
  static constexpr LogicalOperatorType operatorType_ =
      LogicalOperatorType::TABLE_FUNCTION_CALL;

 public:
  LogicalTableFunctionCall(
      function::TableFunction tableFunc,
      std::unique_ptr<function::TableFuncBindData> bindData)
      : LogicalOperator{operatorType_},
        tableFunc{std::move(tableFunc)},
        bindData{std::move(bindData)} {
    setCardinality(this->bindData->numRows);
  }

  const function::TableFunction& getTableFunc() const { return tableFunc; }
  function::TableFuncBindData* getBindData() const { return bindData.get(); }

  void setColumnSkips(std::vector<bool> columnSkips) {
    bindData->setColumnSkips(std::move(columnSkips));
  }

  void setNodeMaskRoots(std::vector<std::shared_ptr<LogicalOperator>> roots) {
    nodeMaskRoots = std::move(roots);
  }
  std::vector<std::shared_ptr<LogicalOperator>> getNodeMaskRoots() const {
    return nodeMaskRoots;
  }

  void computeFlatSchema() override;
  void computeFactorizedSchema() override;

  std::string getExpressionsForPrinting() const override {
    auto result = tableFunc.name + "\nColumns: ";
    for (const auto& expr : bindData->columns) {
      result += expr->toString() + ", ";
    }
    if (!bindData->columns.empty()) {
      result = result.substr(0, result.length() - 2);  // Remove trailing ", "
    }
    return result;
  }

  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalTableFunctionCall>(tableFunc,
                                                      bindData->copy());
  }

 private:
  function::TableFunction tableFunc;
  std::unique_ptr<function::TableFuncBindData> bindData;

  std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots;
};

}  // namespace planner
}  // namespace neug
