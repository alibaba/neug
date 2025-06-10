// this class is used to analyze the physical mode of the given gs logical
// plan, it contains one field named mode to record the type of the plan. the
// mode can be QUERY or DDL; it behaves like a visitor to the logical plan, and
// it will visit each operator in the plan to analyze the physical mode. if it
// meet some operator that is DDL operations, like create, drop, alter, it is
// DDL mode, otherwise it is QUERY mode. help me to write the class
#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"

namespace gs {
namespace gopt {

enum class PhysicalMode { READ_ONLY, READ_WRITE, DDL };

class GPhysicalAnalyzer {
 public:
  GPhysicalAnalyzer() : mode(PhysicalMode::READ_ONLY) {}

  PhysicalMode analyze(const planner::LogicalPlan& plan) {
    mode = PhysicalMode::READ_ONLY;  // Default mode is READ_ONLY
    analyzeOperator(*plan.getLastOperator());
    return mode;
  }

 private:
  void analyzeOperator(const planner::LogicalOperator& op) {
    // Visit children first
    for (auto child : op.getChildren()) {
      analyzeOperator(*child);
    }

    // Check operator type
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::CREATE_TABLE:
    case planner::LogicalOperatorType::ALTER:
    case planner::LogicalOperatorType::DROP:
      if (mode == PhysicalMode::READ_WRITE) {
        throw common::Exception(
            "Cannot mix DDL and READ_WRITE operations in the same plan.");
      }
      mode = PhysicalMode::DDL;
      break;
    case planner::LogicalOperatorType::COPY_FROM:
      if (mode == PhysicalMode::DDL) {
        throw common::Exception(
            "Cannot mix READ_WRITE with DDL operations in the same plan.");
      }
      mode = PhysicalMode::READ_WRITE;
      break;
    default:
      // Keep existing mode (QUERY by default)
      break;
    }
  }

 private:
  PhysicalMode mode;
};

}  // namespace gopt
}  // namespace gs
