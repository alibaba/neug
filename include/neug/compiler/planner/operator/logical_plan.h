#pragma once

#include "logical_operator.h"

namespace neug {
namespace planner {

using cardinality_t = uint64_t;

class NEUG_API LogicalPlan {
  friend class CardinalityEstimator;
  friend class CostModel;

 public:
  LogicalPlan() : cost{0} {}

  void setLastOperator(std::shared_ptr<LogicalOperator> op) {
    lastOperator = std::move(op);
  }

  bool isEmpty() const { return lastOperator == nullptr; }

  bool emptyResult(std::shared_ptr<LogicalOperator> op) const {
    if (!op)
      return false;
    // check if any child returns empty result
    for (auto& child : op->getChildren()) {
      if (emptyResult(child)) {
        return true;
      }
    }
    return op->getOperatorType() == LogicalOperatorType::EMPTY_RESULT;
  }

  std::shared_ptr<LogicalOperator> getLastOperator() const {
    return lastOperator;
  }
  LogicalOperator& getLastOperatorRef() const {
    NEUG_ASSERT(lastOperator);
    return *lastOperator;
  }
  Schema* getSchema() const { return lastOperator->getSchema(); }

  cardinality_t getCardinality() const {
    NEUG_ASSERT(lastOperator);
    return lastOperator->getCardinality();
  }

  void setCost(uint64_t cost_) { cost = cost_; }
  uint64_t getCost() const { return cost; }

  std::string toString() const { return lastOperator->toString(); }

  bool isProfile() const;
  bool hasUpdate() const;

  std::unique_ptr<LogicalPlan> shallowCopy() const;

  std::unique_ptr<LogicalPlan> deepCopy() const;

 private:
  std::shared_ptr<LogicalOperator> lastOperator;
  uint64_t cost;
};

}  // namespace planner
}  // namespace neug
