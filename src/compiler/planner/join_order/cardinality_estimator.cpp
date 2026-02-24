#include "neug/compiler/planner/join_order/cardinality_estimator.h"
#include <cmath>

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/planner/join_order/join_order_util.h"
#include "neug/compiler/planner/operator/logical_aggregate.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/storage/stats_manager.h"
#include "neug/compiler/storage/store/node_table.h"
#include "neug/compiler/storage/store/rel_table.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::transaction;

namespace neug {
namespace planner {

static cardinality_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

void CardinalityEstimator::initNodeIDDom(const Transaction* transaction,
                                         const QueryGraph& queryGraph) {
  perQueryGraphNodeIDName2dom.clear();
  for (uint64_t i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
    auto node = queryGraph.getQueryNode(i).get();
    addNodeIDDomAndStats(transaction, *node->getInternalID(),
                         node->getTableIDs());
  }
  for (uint64_t i = 0u; i < queryGraph.getNumQueryRels(); ++i) {
    auto rel = queryGraph.getQueryRel(i);
    if (QueryRelTypeUtils::isRecursive(rel->getRelType())) {
      auto node = rel->getRecursiveInfo()->node.get();
      addNodeIDDomAndStats(transaction, *node->getInternalID(),
                           node->getTableIDs());
    }
  }
}

void CardinalityEstimator::addNodeIDDomAndStats(
    const Transaction* transaction, const binder::Expression& nodeID,
    const std::vector<common::table_id_t>& tableIDs) {
  auto key = nodeID.getUniqueName();
  cardinality_t numNodes = 0u;
  for (auto tableID : tableIDs) {
    auto stats = context->getStatsManager()
                     ->getTable(tableID)
                     ->cast<storage::NodeTable>()
                     .getStats(transaction);
    numNodes += stats.getTableCard();
    if (!nodeTableStats.contains(tableID)) {
      nodeTableStats.insert({tableID, std::move(stats)});
    }
  }
  if (!nodeIDName2dom.contains(key)) {
    nodeIDName2dom.insert({key, numNodes});
  }
}

void CardinalityEstimator::addPerQueryGraphNodeIDDom(
    const binder::Expression& nodeID, cardinality_t numNodes) {
  const auto key = nodeID.getUniqueName();
  if (!perQueryGraphNodeIDName2dom.contains(key)) {
    perQueryGraphNodeIDName2dom.insert({key, numNodes});
  }
}

void CardinalityEstimator::clearPerQueryGraphStats() {
  perQueryGraphNodeIDName2dom.clear();
}

cardinality_t CardinalityEstimator::getNodeIDDom(
    const std::string& nodeIDName) const {
  NEUG_ASSERT(nodeIDName2dom.contains(nodeIDName));
  cardinality_t dom = nodeIDName2dom.at(nodeIDName);
  if (perQueryGraphNodeIDName2dom.contains(nodeIDName)) {
    dom = std::min(dom, perQueryGraphNodeIDName2dom.at(nodeIDName));
  }
  return dom;
}

uint64_t CardinalityEstimator::estimateScanNode(
    const LogicalOperator& op) const {
  const auto& scan = op.constCast<const LogicalScanNodeTable&>();
  switch (scan.getScanType()) {
  case LogicalScanNodeTableType::PRIMARY_KEY_SCAN:
    return 1;
  default:
    return atLeastOne(getNodeIDDom(scan.getNodeID()->getUniqueName()));
  }
}

uint64_t CardinalityEstimator::estimateAggregate(
    const LogicalAggregate& op) const {
  // TODO(Royi) we can use HLL to better estimate the number of distinct keys
  // here
  return op.getKeys().empty() ? 1 : op.getChild(0)->getCardinality();
}

cardinality_t CardinalityEstimator::multiply(double extensionRate,
                                             cardinality_t card) const {
  return atLeastOne(extensionRate * card);
}

uint64_t CardinalityEstimator::estimateHashJoin(
    const std::vector<binder::expression_pair>& joinConditions,
    const LogicalOperator& probeOp, const LogicalOperator& buildOp) const {
  if (LogicalHashJoin::isNodeIDOnlyJoin(joinConditions)) {
    cardinality_t denominator = 1u;
    auto joinKeys = LogicalHashJoin::getJoinNodeIDs(joinConditions);
    for (auto& joinKey : joinKeys) {
      if (nodeIDName2dom.contains(joinKey->getUniqueName())) {
        denominator *= getNodeIDDom(joinKey->getUniqueName());
      }
    }
    return atLeastOne(
        probeOp.getCardinality() *
        JoinOrderUtil::getJoinKeysFlatCardinality(joinKeys, buildOp) /
        atLeastOne(denominator));
  } else {
    // Naively estimate the cardinality if the join is non-ID based
    cardinality_t estCardinality =
        probeOp.getCardinality() * buildOp.getCardinality();
    for (size_t i = 0; i < joinConditions.size(); ++i) {
      estCardinality *= PlannerKnobs::EQUALITY_PREDICATE_SELECTIVITY;
    }
    return atLeastOne(estCardinality);
  }
}

uint64_t CardinalityEstimator::estimateCrossProduct(
    const LogicalOperator& probeOp, const LogicalOperator& buildOp) const {
  return atLeastOne(probeOp.getCardinality() * buildOp.getCardinality());
}

uint64_t CardinalityEstimator::estimateIntersect(
    const expression_vector& joinNodeIDs, const LogicalOperator& probeOp,
    const std::vector<LogicalOperator*>& buildOps) const {
  // Formula 1: treat intersect as a Filter on probe side.
  uint64_t estCardinality1 = probeOp.getCardinality() *
                             PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY;
  // Formula 2: assume independence on join conditions.
  cardinality_t denominator = 1u;
  for (auto& joinNodeID : joinNodeIDs) {
    denominator *= getNodeIDDom(joinNodeID->getUniqueName());
  }
  auto numerator = probeOp.getCardinality();
  for (auto& buildOp : buildOps) {
    numerator *= buildOp->getCardinality();
  }
  auto estCardinality2 = numerator / atLeastOne(denominator);
  // Pick minimum between the two formulas.
  return atLeastOne(std::min<uint64_t>(estCardinality1, estCardinality2));
}

uint64_t CardinalityEstimator::estimateFlatten(
    const LogicalOperator& childOp, f_group_pos groupPosToFlatten) const {
  auto group = childOp.getSchema()->getGroup(groupPosToFlatten);
  return atLeastOne(childOp.getCardinality() * group->cardinalityMultiplier);
}

static bool isPrimaryKey(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PROPERTY) {
    return false;
  }
  return ((PropertyExpression&) expression).isPrimaryKey();
}

static bool isSingleLabelledProperty(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PROPERTY) {
    return false;
  }
  return expression.constCast<PropertyExpression>().isSingleLabel();
}

uint64_t CardinalityEstimator::estimateFilter(
    const LogicalOperator& childPlan, const Expression& predicate) const {
  if (predicate.expressionType == ExpressionType::OR) {
    bool allEquals = true;
    for (auto& child : predicate.getChildren()) {
      if (child->expressionType != ExpressionType::EQUALS) {
        allEquals = false;
        break;
      }
    }
    if (allEquals && !predicate.getChildren().empty()) {
      return estimateFilter(childPlan, *predicate.getChildren()[0]);
    }
  }
  if (predicate.expressionType == ExpressionType::EQUALS) {
    if (isPrimaryKey(*predicate.getChild(0)) ||
        isPrimaryKey(*predicate.getChild(1))) {
      return 1;
    } else {
      return atLeastOne(childPlan.getCardinality() *
                        PlannerKnobs::EQUALITY_PREDICATE_SELECTIVITY);
    }
  } else {
    return atLeastOne(childPlan.getCardinality() *
                      PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY);
  }
}

// Here, we recalculate the cardinality based on the number of edges for the
// <src, dst, edge> triplet. In 'extend', the computed cardinality is the number
// of edges formed by src + edge, so we need to recalculate and calibrate the
// value here.
cardinality_t CardinalityEstimator::estimateGetV(
    const planner::LogicalExtend& extend) const {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  double extensionRate =
      getExtensionRate(*extend.getRel(), *extend.getBoundNode(), &transaction);
  CHECK(extend.getNumChildren() > 0)
      << "extend operator should have at least child";
  auto childOp = extend.getChild(0);
  auto childCard = childOp->getCardinality();
  return childCard * extensionRate;
}

cardinality_t CardinalityEstimator::estimateGetV(
    const planner::LogicalRecursiveExtend& extend) const {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  double extensionRate =
      getExtensionRate(*extend.getRel(), *extend.getBoundNode(), &transaction);
  CHECK(extend.getNumChildren() > 0)
      << "recursive extend operator should have at least child";
  auto childOp = extend.getChild(0);
  auto childCard = childOp->getCardinality();
  return childCard * extensionRate;
}

uint64_t CardinalityEstimator::getNumNodes(
    const Transaction*, const std::vector<table_id_t>& tableIDs) const {
  cardinality_t numNodes = 0u;
  for (auto& tableID : tableIDs) {
    NEUG_ASSERT(nodeTableStats.contains(tableID));
    numNodes += nodeTableStats.at(tableID).getTableCard();
  }
  return atLeastOne(numNodes);
}

uint64_t CardinalityEstimator::getNumRels(
    const Transaction* transaction,
    const std::vector<table_id_t>& tableIDs) const {
  cardinality_t numRels = 0u;
  for (auto tableID : tableIDs) {
    numRels += context->getStatsManager()->getTable(tableID)->getNumTotalRows(
        transaction);
  }
  return atLeastOne(numRels);
}

double CardinalityEstimator::getExtensionRate(
    const RelExpression& rel, const NodeExpression& boundNode,
    const Transaction* transaction) const {
  return getExtensionRate(rel, rel.getTableIDs(), boundNode, transaction);
}

double CardinalityEstimator::getExtensionRate(
    const RelExpression& rel, const table_id_vector_t& tableIDs,
    const NodeExpression& boundNode, const Transaction* transaction) const {
  auto numBoundNodes =
      static_cast<double>(getNumNodes(transaction, boundNode.getTableIDs()));
  auto numRels = static_cast<double>(getNumRels(transaction, tableIDs));
  NEUG_ASSERT(numBoundNodes > 0);
  auto oneHopExtensionRate = numRels / atLeastOne(numBoundNodes);
  switch (rel.getRelType()) {
  case QueryRelType::NON_RECURSIVE: {
    return oneHopExtensionRate;
  }
  case QueryRelType::VARIABLE_LENGTH_WALK:
  case QueryRelType::VARIABLE_LENGTH_TRAIL:
  case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
  case QueryRelType::SHORTEST:
  case QueryRelType::ALL_SHORTEST:
  case QueryRelType::WEIGHTED_SHORTEST:
  case QueryRelType::ALL_WEIGHTED_SHORTEST: {
    double rate = std::pow(
        std::max<double>(1, std::floor(oneHopExtensionRate)),
        std::max<uint16_t>(rel.getRecursiveInfo()->bindData->upperBound, 1));
    double bound =
        oneHopExtensionRate *
        std::max<uint16_t>(rel.getRecursiveInfo()->bindData->upperBound, 1) *
        context->getClientConfig()->recursivePatternCardinalityScaleFactor;
    return std::min<double>(rate, bound);
  }
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace planner
}  // namespace neug
