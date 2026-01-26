#pragma once

#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/transaction/transaction_action.h"

namespace neug {
namespace planner {

class LogicalTransaction : public LogicalOperator {
  static constexpr LogicalOperatorType type_ = LogicalOperatorType::TRANSACTION;

 public:
  explicit LogicalTransaction(transaction::TransactionAction transactionAction)
      : LogicalOperator{type_}, transactionAction{transactionAction} {}

  std::string getExpressionsForPrinting() const final { return std::string(); }

  void computeFlatSchema() final { createEmptySchema(); }
  void computeFactorizedSchema() final { createEmptySchema(); }

  transaction::TransactionAction getTransactionAction() const {
    return transactionAction;
  }

  std::unique_ptr<LogicalOperator> copy() final {
    return std::make_unique<LogicalTransaction>(transactionAction);
  }

 private:
  transaction::TransactionAction transactionAction;
};

}  // namespace planner
}  // namespace neug
