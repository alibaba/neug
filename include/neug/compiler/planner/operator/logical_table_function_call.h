#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/function/neug_procedure_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/compiler/planner/operator/logical_operator.h"

namespace gs {
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

  LogicalTableFunctionCall(function::NeugCallFunction callFunc,
                           binder::expression_vector callParams,
                           binder::expression_vector callOutput)
      : LogicalOperator{operatorType_},
        callFunc{std::move(callFunc)},
        callParams{std::move(callParams)},
        callOutput{std::move(callOutput)} {}

  const function::TableFunction& getTableFunc() const { return tableFunc; }
  const function::TableFuncBindData* getBindData() const {
    return bindData.get();
  }

  const function::NeugCallFunction& getCallFunc() const { return callFunc; }

  const binder::expression_vector& getCallParams() const { return callParams; }

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
    if (bindData) {
      auto result = tableFunc.name + "\nColumns: ";
      for (const auto& expr : bindData->columns) {
        result += expr->toString() + ", ";
      }
      if (!bindData->columns.empty()) {
        result = result.substr(0, result.length() - 2);  // Remove trailing ", "
      }
      return result;
    } else {
      return callFunc.name;
    }
  }

  std::unique_ptr<LogicalOperator> copy() override {
    if (bindData) {
      return std::make_unique<LogicalTableFunctionCall>(tableFunc,
                                                        bindData->copy());
    } else {
      return std::make_unique<LogicalTableFunctionCall>(callFunc, callParams,
                                                        callOutput);
    }
  }

 private:
  function::TableFunction tableFunc;
  std::unique_ptr<function::TableFuncBindData> bindData;

  function::NeugCallFunction callFunc;
  binder::expression_vector callParams;
  binder::expression_vector callOutput;

  std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots;
};

}  // namespace planner
}  // namespace gs
