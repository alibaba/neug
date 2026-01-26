#pragma once

#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/function/gds/rec_joins.h"
#include "neug/compiler/optimizer/expand_getv_fusion.h"
#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug {
namespace planner {

enum ResultOpt { END_V = 0, ALL_V = 1, ALL_V_E = 2 };

class LogicalRecursiveExtend final : public LogicalOperator {
  static constexpr LogicalOperatorType operatorType_ =
      LogicalOperatorType::RECURSIVE_EXTEND;

 public:
  LogicalRecursiveExtend(std::unique_ptr<function::RJAlgorithm> function,
                         const function::RJBindData& bindData,
                         binder::expression_vector resultColumns)
      : LogicalOperator{operatorType_},
        function{std::move(function)},
        bindData{bindData},
        resultColumns{std::move(resultColumns)},
        limitNum{common::INVALID_LIMIT} {}

  LogicalRecursiveExtend(std::unique_ptr<function::RJAlgorithm> function,
                         const function::RJBindData& bindData,
                         binder::expression_vector resultColumns,
                         std::shared_ptr<binder::RelExpression> relExpr)
      : LogicalOperator{operatorType_},
        function{std::move(function)},
        bindData{bindData},
        resultColumns{std::move(resultColumns)},
        limitNum{common::INVALID_LIMIT},
        relExpr{relExpr} {}

  void computeFlatSchema() override;
  void computeFactorizedSchema() override;

  common::ExtendDirection getDirection() const {
    return bindData.extendDirection;
  }

  void setFusionType(neug::optimizer::FusionType fusionType_) {
    fusionType = fusionType_;
  }

  neug::optimizer::FusionType getFusionType() const { return fusionType; }

  void setFunction(std::unique_ptr<function::RJAlgorithm> func) {
    function = std::move(func);
  }
  const function::RJAlgorithm& getFunction() const { return *function; }

  const function::RJBindData& getBindData() const { return bindData; }
  function::RJBindData& getBindDataUnsafe() { return bindData; }

  void setResultColumns(binder::expression_vector exprs) {
    resultColumns = std::move(exprs);
  }
  binder::expression_vector getResultColumns() const { return resultColumns; }

  void setLimitNum(common::offset_t num) { limitNum = num; }
  common::offset_t getLimitNum() const { return limitNum; }

  bool hasInputNodeMask() const { return hasInputNodeMask_; }
  void setInputNodeMask() { hasInputNodeMask_ = true; }

  bool hasOutputNodeMask() const { return hasOutputNodeMask_; }
  void setOutputNodeMask() { hasOutputNodeMask_ = true; }

  bool hasNodePredicate() const { return !children.empty(); }
  std::shared_ptr<LogicalOperator> getNodeMaskRoot() const {
    if (!children.empty()) {
      return children[0];
    }
    return nullptr;
  }

  std::string getExpressionsForPrinting() const override;

  std::unique_ptr<LogicalOperator> copy() override {
    auto result = std::make_unique<LogicalRecursiveExtend>(
        function->copy(), bindData, resultColumns, relExpr);
    result->limitNum = limitNum;
    result->hasInputNodeMask_ = hasInputNodeMask_;
    result->hasOutputNodeMask_ = hasOutputNodeMask_;
    return result;
  }

  std::shared_ptr<binder::NodeExpression> getBoundNode() const {
    return std::dynamic_pointer_cast<binder::NodeExpression>(
        bindData.nodeInput);
  }

  std::shared_ptr<binder::NodeExpression> getNbrNode() const {
    return std::dynamic_pointer_cast<binder::NodeExpression>(
        bindData.nodeOutput);
  }

  std::shared_ptr<binder::RelExpression> getRel() const { return relExpr; }

  void setResultOpt(ResultOpt opt) { resultOpt = opt; }

  ResultOpt getResultOpt() const {
    if (relExpr) {
      std::string relVarName = relExpr->getVariableName();
      if (relVarName.empty()) {
        return ResultOpt::END_V;  // optimize the ResultOpt to 'END_V' if no
                                  // query given alias
      }
    }
    return resultOpt;
  }

  std::string getExpandBaseName() const {
    auto recursiveInfo = relExpr->getRecursiveInfo();
    return recursiveInfo->rel->getUniqueName();
  }

  std::string getGetVBaseName() const {
    auto recursiveInfo = relExpr->getRecursiveInfo();
    return recursiveInfo->node->getUniqueName();
  }

  std::string getAliasName() const {
    if (!relExpr) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "LogicalRecursiveExtend does not have a relational expression.");
    }
    return relExpr->getUniqueName();
  }

  std::string getStartAliasName() const {
    return getBoundNode()->getUniqueName();
  }

  gopt::GAliasName getGAliasName() const {
    if (!relExpr) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "LogicalRecursiveExtend does not have a relational expression.");
    }
    auto queryName = relExpr->getVariableName().empty()
                         ? std::nullopt
                         : std::make_optional(relExpr->getVariableName());
    return gopt::GAliasName{relExpr->getUniqueName(), queryName};
  }

 private:
  std::unique_ptr<function::RJAlgorithm> function;
  function::RJBindData bindData;
  binder::expression_vector resultColumns;

  common::offset_t
      limitNum;  // TODO: remove this once recursive extend is pipelined.

  bool hasInputNodeMask_ = false;
  bool hasOutputNodeMask_ = false;
  std::shared_ptr<binder::RelExpression> relExpr;
  neug::optimizer::FusionType fusionType =
      neug::optimizer::FusionType::EXPANDE_GETV;
  ResultOpt resultOpt = ResultOpt::ALL_V;
};

}  // namespace planner
}  // namespace neug
