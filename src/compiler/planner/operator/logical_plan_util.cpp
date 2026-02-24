#include "neug/compiler/planner/operator/logical_plan_util.h"

#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

using namespace neug::binder;

namespace neug {
namespace planner {

std::string LogicalPlanUtil::encodeJoin(LogicalPlan& logicalPlan) {
  return encode(logicalPlan.getLastOperator().get());
}

std::string LogicalPlanUtil::encode(LogicalOperator* logicalOperator) {
  std::string result;
  encodeRecursive(logicalOperator, result);
  return result;
}

void LogicalPlanUtil::encodeRecursive(LogicalOperator* logicalOperator,
                                      std::string& encodeString) {
  switch (logicalOperator->getOperatorType()) {
  case LogicalOperatorType::CROSS_PRODUCT: {
    encodeCrossProduct(logicalOperator, encodeString);
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
      encodeString += "{";
      encodeRecursive(logicalOperator->getChild(i).get(), encodeString);
      encodeString += "}";
    }
  } break;
  case LogicalOperatorType::INTERSECT: {
    encodeIntersect(logicalOperator, encodeString);
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
      encodeString += "{";
      encodeRecursive(logicalOperator->getChild(i).get(), encodeString);
      encodeString += "}";
    }
  } break;
  case LogicalOperatorType::HASH_JOIN: {
    encodeHashJoin(logicalOperator, encodeString);
    encodeString += "{";
    encodeRecursive(logicalOperator->getChild(0).get(), encodeString);
    encodeString += "}{";
    encodeRecursive(logicalOperator->getChild(1).get(), encodeString);
    encodeString += "}";
  } break;
  case LogicalOperatorType::EXTEND: {
    encodeExtend(logicalOperator, encodeString);
    encodeRecursive(logicalOperator->getChild(0).get(), encodeString);
  } break;
  case LogicalOperatorType::SCAN_NODE_TABLE: {
    encodeScanNodeTable(logicalOperator, encodeString);
  } break;
  case LogicalOperatorType::FILTER: {
    encodeFilter(logicalOperator, encodeString);
    encodeRecursive(logicalOperator->getChild(0).get(), encodeString);
  } break;
  default:
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
      encodeRecursive(logicalOperator->getChild(i).get(), encodeString);
    }
  }
}

void LogicalPlanUtil::encodeCrossProduct(LogicalOperator* /*logicalOperator*/,
                                         std::string& encodeString) {
  encodeString += "CP()";
}

void LogicalPlanUtil::encodeIntersect(LogicalOperator* logicalOperator,
                                      std::string& encodeString) {
  auto& logicalIntersect = logicalOperator->constCast<LogicalIntersect>();
  encodeString +=
      "IIII(" + logicalIntersect.getIntersectNodeID()->toString() + ")IIII";
}

void LogicalPlanUtil::encodeHashJoin(LogicalOperator* logicalOperator,
                                     std::string& encodeString) {
  auto& logicalHashJoin = logicalOperator->constCast<LogicalHashJoin>();
  encodeString +=
      "HJJJ(" + logicalHashJoin.getExpressionsForPrinting() + ")HJJJ";
}

void LogicalPlanUtil::encodeExtend(LogicalOperator* logicalOperator,
                                   std::string& encodeString) {
  auto& logicalExtend = logicalOperator->constCast<LogicalExtend>();
  encodeString += "EEE(" + logicalExtend.getNbrNode()->toString() + ")EEE";
}

void LogicalPlanUtil::encodeScanNodeTable(LogicalOperator* logicalOperator,
                                          std::string& encodeString) {
  auto& scan = logicalOperator->constCast<LogicalScanNodeTable>();
  if (scan.getScanType() == LogicalScanNodeTableType::PRIMARY_KEY_SCAN) {
    encodeString += "IndexScan";
  } else {
    encodeString += "S";
  }
  encodeString +=
      "(" +
      scan.getNodeID()->constCast<PropertyExpression>().getRawVariableName() +
      ")";
}

void LogicalPlanUtil::encodeFilter(LogicalOperator*,
                                   std::string& encodedString) {
  encodedString += "Filter()";
}

}  // namespace planner
}  // namespace neug
