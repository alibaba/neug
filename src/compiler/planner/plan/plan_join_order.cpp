#include <glog/logging.h>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <utility>

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/enums/join_type.h"
#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/utils.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/optimizer/expand_getv_fusion.h"
#include "neug/compiler/optimizer/filter_push_down_optimizer.h"
#include "neug/compiler/optimizer/filter_push_down_pattern.h"
#include "neug/compiler/optimizer/flat_join_to_expand_optimizer.h"
#include "neug/compiler/planner/join_order/cardinality_estimator.h"
#include "neug/compiler/planner/join_order/cost_model.h"
#include "neug/compiler/planner/join_order/join_plan_solver.h"
#include "neug/compiler/planner/join_order/join_tree_constructor.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/planner/planner.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

static cardinality_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  return getBestPlan(enumerateQueryGraphCollection(queryGraphCollection, info));
}

bool containsEqualFiltering(std::shared_ptr<planner::LogicalOperator> bestOp) {
  if (bestOp->getOperatorType() == planner::LogicalOperatorType::FILTER) {
    auto filter = bestOp->ptrCast<planner::LogicalFilter>();
    auto visitor = binder::EqualFilteringVisitor();
    visitor.visit(filter->getPredicate());
    return visitor.containsEqualFiltering();
  }

  for (auto child : bestOp->getChildren()) {
    if (containsEqualFiltering(child)) {
      return true;
    }
  }
  return false;
}

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollectionInNewContext(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  auto prevContext = enterNewContext();
  auto plans = enumerateQueryGraphCollection(queryGraphCollection, info);
  exitContext(std::move(prevContext));
  auto bestPlan = getBestPlan(std::move(plans));
  optimizer::FlatJoinToExpandOptimizer joinToExpandOpt;
  auto scanParent = joinToExpandOpt.getScanParent(bestPlan->getLastOperator());
  if (!scanParent || scanParent->getNumChildren() == 0) {
    return bestPlan;
  }
  auto scan = scanParent->getChild(0);
  auto scanNode = scan->ptrCast<planner::LogicalScanNodeTable>();
  // the scan node starts from the correlated expressions
  if (!scanNode->isNodeIDScan()) {
    return bestPlan;
  }
  if (info.corrExprs.empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "correlated expressions should not be empty");
  }

  std::vector<planner::LogicalOperatorType> includeOps;

  if (info.corrExprs.size() > 2) {
    goto re_plan;
  }
  if (info.kind != MatchKind::OPTIONAL && info.kind != MatchKind::REGULAR) {
    goto re_plan;
  }
  if (containsEqualFiltering(bestPlan->getLastOperator())) {
    goto re_plan;
  }

  if (info.corrExprs.size() == 2) {
    includeOps.emplace_back(planner::LogicalOperatorType::SCAN_NODE_TABLE);
    includeOps.emplace_back(planner::LogicalOperatorType::EXTEND);
    includeOps.emplace_back(planner::LogicalOperatorType::GET_V);
  } else if (info.corrExprs.size() == 1) {
    if (info.kind == MatchKind::REGULAR) {
      includeOps.push_back(planner::LogicalOperatorType::SCAN_NODE_TABLE);
      includeOps.push_back(planner::LogicalOperatorType::EXTEND);
      includeOps.push_back(planner::LogicalOperatorType::GET_V);
      includeOps.push_back(planner::LogicalOperatorType::RECURSIVE_EXTEND);
      includeOps.push_back(planner::LogicalOperatorType::FILTER);
    } else {  // OPTIONAL
      includeOps.push_back(planner::LogicalOperatorType::SCAN_NODE_TABLE);
      includeOps.push_back(planner::LogicalOperatorType::EXTEND);
      includeOps.push_back(planner::LogicalOperatorType::GET_V);
      // check bestPlan after filtering push down optimization
      optimizer::FilterPushDownPattern pushDownOpt;
      pushDownOpt.rewrite(bestPlan.get());
    }
  }

  if (joinToExpandOpt.checkOperatorType(bestPlan->getLastOperator(),
                                        includeOps)) {
    return bestPlan;
  }
re_plan:
  // clear stats of correlated expressions and re-plan query
  QueryGraphPlanningInfo newInfo;
  newInfo.predicates = info.predicates;
  newInfo.subqueryType = info.subqueryType;
  newInfo.hint = info.hint;
  newInfo.kind = info.kind;
  cardinalityEstimator.clearPerQueryGraphStats();
  return planQueryGraphCollectionInNewContext(queryGraphCollection, newInfo);
}

static int32_t getConnectedQueryGraphIdx(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
    auto queryGraph = queryGraphCollection.getQueryGraph(i);
    for (auto& queryNode : queryGraph->getQueryNodes()) {
      if (info.containsCorrExpr(*queryNode->getInternalID())) {
        return i;
      }
    }
  }
  return -1;
}

std::vector<std::unique_ptr<LogicalPlan>>
Planner::enumerateQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  NEUG_ASSERT(queryGraphCollection.getNumQueryGraphs() > 0);
  auto& corrExprs = info.corrExprs;
  int32_t queryGraphIdxToPlanExpressionsScan = -1;
  if (info.subqueryType == SubqueryPlanningType::CORRELATED) {
    queryGraphIdxToPlanExpressionsScan =
        getConnectedQueryGraphIdx(queryGraphCollection, info);
  }
  std::unordered_set<uint32_t> evaluatedPredicatesIndices;
  std::vector<std::vector<std::unique_ptr<LogicalPlan>>> plansPerQueryGraph;
  for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
    auto queryGraph = queryGraphCollection.getQueryGraph(i);
    std::unordered_set<uint32_t> predicateToEvaluateIndices;
    for (auto j = 0u; j < info.predicates.size(); ++j) {
      if (info.predicates[j]->expressionType == ExpressionType::LITERAL) {
        continue;
      }
      if (evaluatedPredicatesIndices.contains(j)) {
        continue;
      }
      if (queryGraph->canProjectExpression(info.predicates[j])) {
        predicateToEvaluateIndices.insert(j);
      }
    }
    evaluatedPredicatesIndices.insert(predicateToEvaluateIndices.begin(),
                                      predicateToEvaluateIndices.end());
    expression_vector predicatesToEvaluate;
    for (auto idx : predicateToEvaluateIndices) {
      predicatesToEvaluate.push_back(info.predicates[idx]);
    }
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    auto newInfo = info;
    newInfo.predicates = predicatesToEvaluate;
    switch (info.subqueryType) {
    case SubqueryPlanningType::COMMON_PAT_REUSE:
    case SubqueryPlanningType::NONE:
    case SubqueryPlanningType::UNNEST_CORRELATED: {
      plans = enumerateQueryGraph(*queryGraph, newInfo);
    } break;
    case SubqueryPlanningType::CORRELATED: {
      if (i == (uint32_t) queryGraphIdxToPlanExpressionsScan) {
        plans = enumerateQueryGraph(*queryGraph, newInfo);
      } else {
        newInfo.subqueryType = SubqueryPlanningType::NONE;
        plans = enumerateQueryGraph(*queryGraph, newInfo);
      }
    } break;
    default:
      NEUG_UNREACHABLE;
    }
    plansPerQueryGraph.push_back(std::move(plans));
  }
  if (info.subqueryType == SubqueryPlanningType::CORRELATED &&
      queryGraphIdxToPlanExpressionsScan == -1) {
    // todo: support query like `MATCH (a)-[]->(b) WHERE a.ID = 0 OPTIONAL MATCH
    // (c:person) WHERE c.ID = b.ID + 3`, here the 2nd Match is not connected to
    // the 1st Match, but they have the correlated expression `c.ID = b.ID + 3`.
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Unconnected correlated query is not supported yet");
    // auto plan = std::make_unique<LogicalPlan>();
    // appendExpressionsScan(corrExprs, *plan);
    // appendDistinct(corrExprs, *plan);
    // std::vector<std::unique_ptr<LogicalPlan>> plans;
    // plans.push_back(std::move(plan));
    // plansPerQueryGraph.push_back(std::move(plans));
  }
  auto result = std::move(plansPerQueryGraph[0]);
  for (auto i = 1u; i < plansPerQueryGraph.size(); ++i) {
    result =
        planCrossProduct(std::move(result), std::move(plansPerQueryGraph[i]));
  }
  expression_vector remainingPredicates;
  for (auto i = 0u; i < info.predicates.size(); ++i) {
    if (!evaluatedPredicatesIndices.contains(i)) {
      remainingPredicates.push_back(info.predicates[i]);
    }
  }
  for (auto& plan : result) {
    for (auto& predicate : remainingPredicates) {
      appendFilter(predicate, *plan);
    }
  }
  return result;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::enumerateQueryGraph(
    const QueryGraph& queryGraph, const QueryGraphPlanningInfo& info) {
  context.init(&queryGraph, info.predicates);
  cardinalityEstimator.initNodeIDDom(clientContext->getTransaction(),
                                     queryGraph);
  if (info.hint != nullptr) {
    auto constructor = JoinTreeConstructor(queryGraph, propertyExprCollection,
                                           info.predicates, info);
    auto joinTree = constructor.construct(info.hint);
    auto plan = JoinPlanSolver(this).solve(joinTree);
    std::vector<std::unique_ptr<LogicalPlan>> result;
    result.push_back(plan.shallowCopy());
    cardinalityEstimator.clearPerQueryGraphStats();
    return result;
  }
  planBaseTableScans(info);
  context.currentLevel++;
  while (context.currentLevel < context.maxLevel) {
    planLevel(context.currentLevel++);
  }
  auto plans =
      std::move(context.getPlans(context.getFullyMatchedSubqueryGraph()));
  if (queryGraph.isEmpty()) {
    for (auto& plan : plans) {
      appendEmptyResult(*plan);
    }
  }
  cardinalityEstimator.clearPerQueryGraphStats();
  return plans;
}

void Planner::planLevel(uint32_t level) {
  NEUG_ASSERT(level > 1);
  planLevelExactly(level);
}

void Planner::planLevelExactly(uint32_t level) {
  auto maxLeftLevel = floor(level / 2.0);
  for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
    auto rightLevel = level - leftLevel;
    if (leftLevel > 1) {
      planWCOJoin(leftLevel, rightLevel);
    }
  }
  planLevelApproximately(level);
}

void Planner::planLevelApproximately(uint32_t level) {
  planInnerJoin(1, level - 1);
}

void Planner::planBaseTableScans(const QueryGraphPlanningInfo& info) {
  auto queryGraph = context.getQueryGraph();
  switch (info.subqueryType) {
  case SubqueryPlanningType::NONE: {
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes();
         ++nodePos) {
      planNodeScan(nodePos);
    }
  } break;
  case SubqueryPlanningType::UNNEST_CORRELATED: {
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes();
         ++nodePos) {
      auto queryNode = queryGraph->getQueryNode(nodePos);
      if (info.containsCorrExpr(*queryNode->getInternalID())) {
        // todo: if the subquery has filtering, i.e. where not
        // (a:person)-[:knows]->(b:person {id: 1}), the filtering `{id: 1}` is
        // omitted here, then the right branch of the anti join need to scan
        // from all persons without any filtering optimization
        planNodeIDScan(nodePos, info);
      } else {
        planNodeScan(nodePos);
      }
    }
  } break;
  case SubqueryPlanningType::COMMON_PAT_REUSE: {
    // append expression scan (scan from common expressions in pre query) and
    // scan node table (scan from the whole table) the optimizer will compare
    // the cardinality of the two scan(s) and choose the best automatically
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes();
         ++nodePos) {
      auto queryNode = queryGraph->getQueryNode(nodePos);
      if (info.containsCorrExpr(*queryNode->getInternalID())) {
        planCorrelatedExpressionsScan(info);
      } else {
        planNodeScan(nodePos);
      }
    }
  } break;
  case SubqueryPlanningType::CORRELATED: {
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes();
         ++nodePos) {
      // The query `MATCH (a:person)-[:knows]->(b:person) OPTIONAL
      // MATCH (b)-[:knows]->(c:person) WHERE b.age > 10` is a correlated query.
      // For now, we disable optimizations for correlated queries and replace
      // ExpressionScan with a global Scan. Support for correlated queries
      // will be added later in two ways:
      // 1. Flatten ExpressionScan into Extend operations by
      // `FlatJoinToExtendRule`.
      // 2. Keep the ExpressionScan structure and transform it into a fork-join
      //    implementation, which requires engine support.
      auto queryNode = queryGraph->getQueryNode(nodePos);
      // if (info.containsCorrExpr(*queryNode->getInternalID())) {
      //   continue;
      // }
      planNodeScan(nodePos);
    }
    // planCorrelatedExpressionsScan(info);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  for (auto relPos = 0u; relPos < queryGraph->getNumQueryRels(); ++relPos) {
    planRelScan(relPos);
  }
}

void Planner::planCorrelatedExpressionsScan(
    const QueryGraphPlanningInfo& info) {
  auto queryGraph = context.getQueryGraph();
  auto newSubgraph = context.getEmptySubqueryGraph();
  auto& corrExprs = info.corrExprs;
  for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
    auto queryNode = queryGraph->getQueryNode(nodePos);
    if (info.containsCorrExpr(*queryNode->getInternalID())) {
      newSubgraph.addQueryNode(nodePos);
    }
  }
  auto plan = std::make_unique<LogicalPlan>();
  appendExpressionsScan(corrExprs, *plan);
  plan->getLastOperator()->setCardinality(info.corrExprsCard);
  // auto predicates =
  //     getNewlyMatchedExprs(context.getEmptySubqueryGraph(), newSubgraph,
  //                          context.getWhereExpressions());
  // appendFilters(predicates, *plan);
  // appendDistinct(corrExprs, *plan);
  context.addPlan(newSubgraph, std::move(plan));
}

void Planner::planNodeScan(uint32_t nodePos) {
  auto node = context.queryGraph->getQueryNode(nodePos);
  auto newSubgraph = context.getEmptySubqueryGraph();
  newSubgraph.addQueryNode(nodePos);
  auto plan = std::make_unique<LogicalPlan>();
  auto properties = getProperties(*node);
  appendScanNodeTable(node->getInternalID(), node->getTableIDs(), properties,
                      *plan);
  auto predicates =
      getNewlyMatchedExprs(context.getEmptySubqueryGraph(), newSubgraph,
                           context.getWhereExpressions());
  appendFilters(predicates, *plan);
  context.addPlan(newSubgraph, std::move(plan));
}

void Planner::planNodeIDScan(uint32_t nodePos,
                             const QueryGraphPlanningInfo& info) {
  auto node = context.queryGraph->getQueryNode(nodePos);
  auto newSubgraph = context.getEmptySubqueryGraph();
  newSubgraph.addQueryNode(nodePos);
  auto plan = std::make_unique<LogicalPlan>();

  cardinalityEstimator.addPerQueryGraphNodeIDDom(*node->getInternalID(),
                                                 info.corrExprsCard);

  appendScanNodeTable(node->getInternalID(), node->getTableIDs(), {}, *plan,
                      true);
  context.addPlan(newSubgraph, std::move(plan));
}

static std::pair<std::shared_ptr<NodeExpression>,
                 std::shared_ptr<NodeExpression>>
getBoundAndNbrNodes(const RelExpression& rel, ExtendDirection direction) {
  NEUG_ASSERT(direction != ExtendDirection::BOTH);
  auto boundNode =
      direction == ExtendDirection::FWD ? rel.getSrcNode() : rel.getDstNode();
  auto dstNode =
      direction == ExtendDirection::FWD ? rel.getDstNode() : rel.getSrcNode();
  return make_pair(boundNode, dstNode);
}

static ExtendDirection getExtendDirection(
    const binder::RelExpression& relExpression,
    const binder::NodeExpression& boundNode) {
  if (relExpression.getDirectionType() == binder::RelDirectionType::BOTH) {
    NEUG_ASSERT(relExpression.getExtendDirections().size() ==
                common::NUM_REL_DIRECTIONS);
    return ExtendDirection::BOTH;
  }
  if (relExpression.getSrcNodeName() == boundNode.getUniqueName()) {
    return ExtendDirection::FWD;
  } else {
    return ExtendDirection::BWD;
  }
}

void Planner::planRelScan(uint32_t relPos) {
  const auto rel = context.queryGraph->getQueryRel(relPos);
  auto newSubgraph = context.getEmptySubqueryGraph();
  newSubgraph.addQueryRel(relPos);
  const auto predicates =
      getNewlyMatchedExprs(context.getEmptySubqueryGraph(), newSubgraph,
                           context.getWhereExpressions());
  for (const auto direction : rel->getExtendDirections()) {
    auto plan = std::make_unique<LogicalPlan>();
    auto [boundNode, nbrNode] = getBoundAndNbrNodes(*rel, direction);
    const auto extendDirection = getExtendDirection(*rel, *boundNode);
    appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(),
                        {}, *plan);
    appendExtend(boundNode, nbrNode, rel, extendDirection, getProperties(*rel),
                 *plan);
    appendFilters(predicates, *plan);
    context.addPlan(newSubgraph, std::move(plan));
  }
}

void Planner::appendExtend(std::shared_ptr<NodeExpression> boundNode,
                           std::shared_ptr<NodeExpression> nbrNode,
                           std::shared_ptr<RelExpression> rel,
                           ExtendDirection direction,
                           const binder::expression_vector& properties,
                           LogicalPlan& plan) {
  switch (rel->getRelType()) {
  case QueryRelType::NON_RECURSIVE: {
    auto extendFromSource = *boundNode == *rel->getSrcNode();
    appendNonRecursiveExtend(boundNode, nbrNode, rel, direction,
                             extendFromSource, properties, plan);
  } break;
  case QueryRelType::VARIABLE_LENGTH_WALK:
  case QueryRelType::VARIABLE_LENGTH_TRAIL:
  case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
  case QueryRelType::SHORTEST:
  case QueryRelType::ALL_SHORTEST:
  case QueryRelType::WEIGHTED_SHORTEST:
  case QueryRelType::ALL_WEIGHTED_SHORTEST: {
    appendRecursiveExtend(boundNode, nbrNode, rel, direction, plan);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

static std::unordered_map<uint32_t, std::vector<std::shared_ptr<RelExpression>>>
populateIntersectRelCandidates(const QueryGraph& queryGraph,
                               const SubqueryGraph& subgraph) {
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<RelExpression>>>
      intersectNodePosToRelsMap;
  for (auto relPos : subgraph.getRelNbrPositions()) {
    auto rel = queryGraph.getQueryRel(relPos);
    if (!queryGraph.containsQueryNode(rel->getSrcNodeName()) ||
        !queryGraph.containsQueryNode(rel->getDstNodeName())) {
      continue;
    }
    auto srcNodePos = queryGraph.getQueryNodeIdx(rel->getSrcNodeName());
    auto dstNodePos = queryGraph.getQueryNodeIdx(rel->getDstNodeName());
    auto isSrcConnected = subgraph.queryNodesSelector[srcNodePos];
    auto isDstConnected = subgraph.queryNodesSelector[dstNodePos];
    if (isSrcConnected && isDstConnected) {
      continue;
    }
    auto intersectNodePos = isSrcConnected ? dstNodePos : srcNodePos;
    if (!intersectNodePosToRelsMap.contains(intersectNodePos)) {
      intersectNodePosToRelsMap.insert(
          {intersectNodePos, std::vector<std::shared_ptr<RelExpression>>{}});
    }
    intersectNodePosToRelsMap.at(intersectNodePos).push_back(rel);
  }
  return intersectNodePosToRelsMap;
}

void Planner::planWCOJoin(uint32_t leftLevel, uint32_t rightLevel) {
  NEUG_ASSERT(leftLevel <= rightLevel);
  auto queryGraph = context.getQueryGraph();
  for (auto& rightSubgraph :
       context.subPlansTable->getSubqueryGraphs(rightLevel)) {
    auto candidates =
        populateIntersectRelCandidates(*queryGraph, rightSubgraph);
    for (auto& [intersectNodePos, rels] : candidates) {
      if (rels.size() == leftLevel) {
        auto intersectNode = queryGraph->getQueryNode(intersectNodePos);
        planWCOJoin(rightSubgraph, rels, intersectNode);
      }
    }
  }
}

static LogicalOperator* getSequentialScan(LogicalOperator* op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::FLATTEN:
  case LogicalOperatorType::FILTER:
  case LogicalOperatorType::EXTEND:
  case LogicalOperatorType::GET_V:
  case LogicalOperatorType::PROJECTION: {
    return getSequentialScan(op->getChild(0).get());
  }
  case LogicalOperatorType::SCAN_NODE_TABLE: {
    return op;
  }
  default:
    return nullptr;
  }
}

static bool isNodeSequentialOnPlan(const LogicalPlan& plan,
                                   const NodeExpression& node) {
  const auto seqScan = getSequentialScan(plan.getLastOperator().get());
  if (seqScan == nullptr) {
    return false;
  }
  const auto sequentialScan = neug_dynamic_cast<LogicalScanNodeTable*>(seqScan);
  return sequentialScan->getNodeID()->getUniqueName() ==
         node.getInternalID()->getUniqueName();
}

static bool isNodeSequentialOnPlan2(const LogicalPlan& plan,
                                    const NodeExpression& node) {
  const auto seqScan = getSequentialScan(plan.getLastOperator().get());
  const auto sequentialScan = neug_dynamic_cast<LogicalScanNodeTable*>(seqScan);
  return sequentialScan->getNodeID()->getUniqueName() ==
         node.getInternalID()->getUniqueName();
}

static std::unique_ptr<LogicalPlan> getWCOJBuildPlanForRel(
    std::vector<std::unique_ptr<LogicalPlan>>& candidatePlans,
    const NodeExpression& boundNode) {
  std::unique_ptr<LogicalPlan> result;
  for (auto& candidatePlan : candidatePlans) {
    if (isNodeSequentialOnPlan(*candidatePlan, boundNode)) {
      NEUG_ASSERT(result == nullptr);
      result = candidatePlan->shallowCopy();
    }
  }
  return result;
}

double Planner::computeRelCardRate(
    size_t relIdx, const std::vector<std::shared_ptr<RelExpression>>& rels,
    const std::shared_ptr<NodeExpression>& intersectNode) {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  auto& rel = rels[relIdx];
  auto boundNode = rel->getSrcNodeName() == intersectNode->getUniqueName()
                       ? rel->getDstNode()
                       : rel->getSrcNode();
  auto extensionRate =
      cardinalityEstimator.getExtensionRate(*rel, *boundNode, &transaction);
  auto intersectNodes = static_cast<double>(cardinalityEstimator.getNumNodes(
      &transaction, intersectNode->getTableIDs()));
  if (relIdx == 0) {
    return extensionRate;
  } else {
    return extensionRate / atLeastOne(intersectNodes);
  }
}

/**
 * Sort rels by the extension rate of the bound node.
 * The rel with the smallest extension rate is put at the front.
 */
std::vector<std::shared_ptr<RelExpression>> Planner::sortRels(
    const std::vector<std::shared_ptr<RelExpression>>& rels,
    const std::shared_ptr<NodeExpression>& intersectNode) {
  std::vector<std::shared_ptr<RelExpression>> sortedRels = rels;
  std::sort(
      sortedRels.begin(), sortedRels.end(),
      [&](const std::shared_ptr<RelExpression>& a,
          const std::shared_ptr<RelExpression>& b) {
        auto aBound = a->getSrcNodeName() == intersectNode->getUniqueName()
                          ? a->getDstNode()
                          : a->getSrcNode();
        auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
        double aRate =
            cardinalityEstimator.getExtensionRate(*a, *aBound, &transaction);
        auto bBound = b->getSrcNodeName() == intersectNode->getUniqueName()
                          ? b->getDstNode()
                          : b->getSrcNode();
        double bRate =
            cardinalityEstimator.getExtensionRate(*b, *bBound, &transaction);
        return aRate < bRate;
      });
  return std::move(sortedRels);
}

void Planner::planWCOJoin(
    const SubqueryGraph& subgraph,
    const std::vector<std::shared_ptr<RelExpression>>& rels,
    const std::shared_ptr<NodeExpression>& intersectNode) {
  auto newSubgraph = subgraph;
  std::vector<SubqueryGraph> prevSubgraphs;
  prevSubgraphs.push_back(subgraph);
  expression_vector boundNodeIDs;
  std::vector<std::unique_ptr<LogicalPlan>> relPlans;
  std::vector<std::shared_ptr<RelExpression>> sortedRels =
      sortRels(rels, intersectNode);
  for (auto& rel : sortedRels) {
    auto boundNode = rel->getSrcNodeName() == intersectNode->getUniqueName()
                         ? rel->getDstNode()
                         : rel->getSrcNode();
    const auto extendDirection = getExtendDirection(*rel, *boundNode);
    if (extendDirection != common::ExtendDirection::BOTH &&
        !common::containsValue(rel->getExtendDirections(), extendDirection)) {
      return;
    }
    boundNodeIDs.push_back(boundNode->getInternalID());
    auto relPos = context.getQueryGraph()->getQueryRelIdx(rel->getUniqueName());
    auto prevSubgraph = context.getEmptySubqueryGraph();
    prevSubgraph.addQueryRel(relPos);
    prevSubgraphs.push_back(prevSubgraph);
    newSubgraph.addQueryRel(relPos);
    auto relSubgraph = context.getEmptySubqueryGraph();
    relSubgraph.addQueryRel(relPos);
    NEUG_ASSERT(context.subPlansTable->containSubgraphPlans(relSubgraph));
    auto& relPlanCandidates =
        context.subPlansTable->getSubgraphPlans(relSubgraph);
    auto relPlan = getWCOJBuildPlanForRel(relPlanCandidates, *boundNode);

    if (relPlan == nullptr) {
      return;
    }
    relPlans.push_back(std::move(relPlan));
  }
  auto predicates = getNewlyMatchedExprs(prevSubgraphs, newSubgraph,
                                         context.getWhereExpressions());
  for (auto& leftPlan : context.getPlans(subgraph)) {
    if (leftPlan->getSchema()->isExpressionInScope(
            *intersectNode->getInternalID())) {
      continue;
    }
    auto leftPlanCard = leftPlan->getLastOperator()->getCardinality();
    auto leftPlanCopy = leftPlan->shallowCopy();
    std::vector<std::unique_ptr<LogicalPlan>> rightPlansCopy;
    rightPlansCopy.reserve(relPlans.size());
    for (auto& relPlan : relPlans) {
      rightPlansCopy.push_back(relPlan->shallowCopy());
    }
    common::cardinality_t prevCard = leftPlan->getCardinality();
    size_t relIdx = 0;
    std::vector<cardinality_t> buildCards;
    for (auto& relPlan : relPlans) {
      auto relCardRate =
          computeRelCardRate(relIdx++, sortedRels, intersectNode);
      auto relCard = prevCard * relCardRate;
      prevCard = relCard;
      buildCards.emplace_back(relCard);
    }
    appendIntersect(intersectNode->getInternalID(), boundNodeIDs, *leftPlanCopy,
                    rightPlansCopy, buildCards);
    for (auto& predicate : predicates) {
      appendFilter(predicate, *leftPlanCopy);
    }
    context.subPlansTable->addPlan(newSubgraph, std::move(leftPlanCopy));
  }
}

static bool needPruneImplicitJoins(const SubqueryGraph& leftSubgraph,
                                   const SubqueryGraph& rightSubgraph,
                                   uint32_t numJoinNodes) {
  auto leftNodePositions = leftSubgraph.getNodePositionsIgnoringNodeSelector();
  auto rightNodePositions =
      rightSubgraph.getNodePositionsIgnoringNodeSelector();
  auto intersectionSize = 0u;
  for (auto& pos : leftNodePositions) {
    if (rightNodePositions.contains(pos)) {
      intersectionSize++;
    }
  }
  return intersectionSize != numJoinNodes;
}

void Planner::planInnerJoin(uint32_t leftLevel, uint32_t rightLevel) {
  NEUG_ASSERT(leftLevel <= rightLevel);
  for (auto& rightSubgraph :
       context.subPlansTable->getSubqueryGraphs(rightLevel)) {
    for (auto& nbrSubgraph : rightSubgraph.getNbrSubgraphs(leftLevel)) {
      if (!context.containPlans(nbrSubgraph)) {
        continue;
      }
      auto joinNodePositions = rightSubgraph.getConnectedNodePos(nbrSubgraph);
      auto joinNodes = context.queryGraph->getQueryNodes(joinNodePositions);
      if (needPruneImplicitJoins(nbrSubgraph, rightSubgraph,
                                 joinNodes.size())) {
        continue;
      }
      if (tryPlanSelfLoopEdge(rightSubgraph, nbrSubgraph, joinNodes)) {
        continue;
      }
      if (joinNodes.size() > 1) {
        continue;
      }
      if (tryPlanINLJoin(rightSubgraph, nbrSubgraph, joinNodes)) {
        continue;
      }
      planGetV(rightSubgraph, nbrSubgraph, joinNodes);
    }
  }
}

bool Planner::tryPlanINLJoin(
    const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
  if (joinNodes.size() > 1) {
    return false;
  }
  if (!subgraph.isSingleRel() && !otherSubgraph.isSingleRel()) {
    return false;
  }
  if (subgraph.isSingleRel()) {
    return tryPlanINLJoin(otherSubgraph, subgraph, joinNodes);
  }
  auto relPos = UINT32_MAX;
  for (auto i = 0u; i < context.queryGraph->getNumQueryRels(); ++i) {
    if (otherSubgraph.queryRelsSelector[i]) {
      relPos = i;
    }
  }
  NEUG_ASSERT(relPos != UINT32_MAX);
  auto rel = context.queryGraph->getQueryRel(relPos);
  const auto& boundNode = joinNodes[0];
  auto nbrNode = boundNode->getUniqueName() == rel->getSrcNodeName()
                     ? rel->getDstNode()
                     : rel->getSrcNode();
  auto extendDirection = getExtendDirection(*rel, *boundNode);
  if (extendDirection != common::ExtendDirection::BOTH &&
      !common::containsValue(rel->getExtendDirections(), extendDirection)) {
    return false;
  }
  auto newSubgraph = subgraph;
  newSubgraph.addQueryRel(relPos);
  auto predicates = getNewlyMatchedExprs(subgraph, newSubgraph,
                                         context.getWhereExpressions());
  bool hasAppliedINLJoin = false;
  for (auto& prevPlan : context.getPlans(subgraph)) {
    auto plan = prevPlan->shallowCopy();
    appendExtend(boundNode, nbrNode, rel, extendDirection, getProperties(*rel),
                 *plan);
    appendFilters(predicates, *plan);
    context.addPlan(newSubgraph, std::move(plan));
    hasAppliedINLJoin = true;
  }
  return hasAppliedINLJoin;
}

bool Planner::tryPlanSelfLoopEdge(
    const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
  if (joinNodes.size() < 2) {
    return false;
  }

  if (subgraph.getNumQueryRels() != 1 || otherSubgraph.getNumQueryRels() != 1) {
    return false;
  }

  if (!subgraph.isSingleRel() && !otherSubgraph.isSingleRel()) {
    return false;
  }

  if (!otherSubgraph.isSingleRel()) {
    return tryPlanSelfLoopEdge(otherSubgraph, subgraph, joinNodes);
  }

  auto relPos = UINT32_MAX;
  for (auto i = 0u; i < context.queryGraph->getNumQueryRels(); ++i) {
    if (otherSubgraph.queryRelsSelector[i]) {
      relPos = i;
      break;
    }
  }
  NEUG_ASSERT(relPos != UINT32_MAX);

  const auto& queryGraph = context.queryGraph;
  auto rel = queryGraph->getQueryRel(relPos);
  auto srcNodePos = queryGraph->getQueryNodeIdx(rel->getSrcNodeName());
  auto dstNodePos = queryGraph->getQueryNodeIdx(rel->getDstNodeName());
  auto isSrcConnected = subgraph.queryNodesSelector[srcNodePos];
  auto isDstConnected = subgraph.queryNodesSelector[dstNodePos];
  if (!isSrcConnected || !isDstConnected) {
    return false;
  }
  // contains self-loop edge, i.e. Match (a)-[]->(b), (b)-[]->(a)
  // convert to inner join
  auto otherCopy = context.getEmptySubqueryGraph();
  otherCopy.addSubqueryGraph(otherSubgraph);
  otherCopy.addQueryNode(srcNodePos);
  otherCopy.addQueryNode(dstNodePos);
  planInnerHashJoin(otherCopy, subgraph, joinNodes, true);
  return true;
}

void Planner::planInnerHashJoin(
    const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes,
    bool flipPlan) {
  auto newSubgraph = subgraph;
  newSubgraph.addSubqueryGraph(otherSubgraph);
  auto maxCost = context.subPlansTable->getMaxCost(newSubgraph);
  expression_vector joinNodeIDs;
  for (auto& joinNode : joinNodes) {
    joinNodeIDs.push_back(joinNode->getInternalID());
  }
  auto predicates = getNewlyMatchedExprs(subgraph, otherSubgraph, newSubgraph,
                                         context.getWhereExpressions());
  for (auto& leftPlan : context.getPlans(subgraph)) {
    for (auto& rightPlan : context.getPlans(otherSubgraph)) {
      if (flipPlan && CostModel::computeHashJoinCost(joinNodeIDs, *rightPlan,
                                                     *leftPlan) < maxCost) {
        auto leftPlanBuildCopy = leftPlan->shallowCopy();
        auto rightPlanProbeCopy = rightPlan->shallowCopy();
        appendHashJoin(joinNodeIDs, JoinType::INNER, *rightPlanProbeCopy,
                       *leftPlanBuildCopy, *rightPlanProbeCopy);
        appendFilters(predicates, *rightPlanProbeCopy);
        context.addPlan(newSubgraph, std::move(rightPlanProbeCopy));
      }
    }
  }
}

planner::GetVOpt getGetVOpt(common::ExtendDirection direction) {
  switch (direction) {
  case common::ExtendDirection::FWD:
    return planner::GetVOpt::END;
  case common::ExtendDirection::BWD:
    return planner::GetVOpt::START;
  case common::ExtendDirection::BOTH:
    return planner::GetVOpt::OTHER;
  default:
    THROW_RUNTIME_ERROR("Unsupported extend direction for GetV: " +
                        static_cast<u_int8_t>(direction));
  }
}

planner::GetVOpt getGetVOpt(std::shared_ptr<LogicalOperator> op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::EXTEND: {
    auto extend = op->ptrCast<LogicalExtend>();
    if (extend->getExtendOpt() == VERTEX) {
      return GetVOpt::ITSELF;
    }
    return getGetVOpt(extend->getDirection());
  }
  case LogicalOperatorType::RECURSIVE_EXTEND: {
    auto extend2 = op->ptrCast<LogicalRecursiveExtend>();
    return getGetVOpt(extend2->getBindData().extendDirection);
  }
  default:
    NEUG_UNREACHABLE;
  }
}

std::shared_ptr<binder::RelExpression> getRel(
    std::shared_ptr<LogicalOperator> op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::EXTEND: {
    auto extend = op->ptrCast<LogicalExtend>();
    return extend->getRel();
  }
  case LogicalOperatorType::RECURSIVE_EXTEND: {
    auto extend2 = op->ptrCast<LogicalRecursiveExtend>();
    return extend2->getRel();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

std::shared_ptr<binder::NodeExpression> getNbrNode(
    std::shared_ptr<LogicalOperator> op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::EXTEND: {
    auto extend = op->ptrCast<LogicalExtend>();
    return extend->getNbrNode();
  }
  case LogicalOperatorType::RECURSIVE_EXTEND: {
    auto extend2 = op->ptrCast<LogicalRecursiveExtend>();
    return extend2->getNbrNode();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

std::unique_ptr<LogicalPlan> Planner::planGetV(
    std::unique_ptr<LogicalPlan> leftPlan,
    std::unique_ptr<LogicalPlan> rightPlan,
    const expression_vector& joinNodeIDs) {
  // extract extend operator from leftPlan, which top may be filtering
  // operator
  auto leftExtend = extractExtend(leftPlan->getLastOperator());
  // extract getV operator from rightPlan, which top may be filtering
  // operator
  auto rightGetV = std::dynamic_pointer_cast<LogicalScanNodeTable>(
      extractGetV(rightPlan->getLastOperator()));
  if (leftExtend && rightGetV) {
    auto nbrNode = getNbrNode(leftExtend);
    if (nbrNode->getInternalID()->toString() !=
        rightGetV->getNodeID()->toString()) {
      return nullptr;
    }
    // build join for cost, cardinality and schema
    auto leftPlanBuildCopy = leftPlan->shallowCopy();
    auto rightPlanProbeCopy = rightPlan->shallowCopy();
    appendHashJoin(joinNodeIDs, JoinType::INNER, *rightPlanProbeCopy,
                   *leftPlanBuildCopy, *rightPlanProbeCopy);

    cardinality_t joinCard;
    uint64_t joinCost;
    if (leftExtend->getOperatorType() == LogicalOperatorType::EXTEND) {
      joinCard = cardinalityEstimator.estimateGetV(
          *leftExtend->ptrCast<LogicalExtend>());
      joinCost =
          CostModel::computeGetVCost(*leftExtend->ptrCast<LogicalExtend>());
    } else if (leftExtend->getOperatorType() ==
               LogicalOperatorType::RECURSIVE_EXTEND) {
      joinCard = cardinalityEstimator.estimateGetV(
          *leftExtend->ptrCast<LogicalRecursiveExtend>());
      joinCost = CostModel::computeGetVCost(
          *leftExtend->ptrCast<LogicalRecursiveExtend>());
    } else {
      NEUG_UNREACHABLE;
    }

    auto joinSchema = rightPlanProbeCopy->getSchema();

    // start to build the getV operator
    auto getVPlan = std::make_unique<LogicalPlan>();
    auto getV = std::make_unique<planner::LogicalGetV>(
        rightGetV->getNodeID(), rightGetV->getTableIDs(),
        rightGetV->getProperties(), getGetVOpt(leftExtend), getRel(leftExtend),
        leftPlan->getLastOperator(), joinSchema->copy(), joinCard);
    getVPlan->setLastOperator(std::move(getV));
    // append filtering predicates corresponding to the getV
    auto getVFilter =
        std::dynamic_pointer_cast<LogicalFilter>(rightPlan->getLastOperator());
    while (getVFilter) {
      appendFilter(getVFilter->getPredicate(), *getVPlan);
      if (getVFilter->getNumChildren() == 0) {
        break;
      }
      getVFilter =
          std::dynamic_pointer_cast<LogicalFilter>(getVFilter->getChild(0));
    }
    getVPlan->setCost(leftPlan->getCost() + joinCost);
    return getVPlan;
  }
  // if the left plan is a intersect, getV will be pushed into the intersect
  auto leftIntersect = extractIntersect(leftPlan->getLastOperator());
  if (leftIntersect && rightGetV) {
    auto intersect = leftIntersect->ptrCast<planner::LogicalIntersect>();
    for (size_t idx = 1; idx < intersect->getNumChildren(); ++idx) {
      auto childLeft = intersect->getChild(idx);
      auto childLeftPlan = std::make_unique<LogicalPlan>();
      childLeftPlan->setLastOperator(childLeft);
      auto childRight = rightPlan->getLastOperator();
      auto childRightPlan = std::make_unique<LogicalPlan>();
      childRightPlan->setLastOperator(childRight);
      auto planChild = planGetV(std::move(childLeftPlan),
                                std::move(childRightPlan), joinNodeIDs);
      if (planChild && planChild->getLastOperator()) {
        intersect->setChild(idx, planChild->getLastOperator());
      }
    }
    return leftPlan;
  }
  return nullptr;
}

void Planner::planGetV(
    const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
  auto newSubgraph = subgraph;
  newSubgraph.addSubqueryGraph(otherSubgraph);
  auto maxCost = context.subPlansTable->getMaxCost(newSubgraph);
  expression_vector joinNodeIDs;
  for (auto& joinNode : joinNodes) {
    joinNodeIDs.push_back(joinNode->getInternalID());
  }
  auto predicates = getNewlyMatchedExprs(subgraph, otherSubgraph, newSubgraph,
                                         context.getWhereExpressions());
  for (auto& leftPlan : context.getPlans(subgraph)) {
    for (auto& rightPlan : context.getPlans(otherSubgraph)) {
      if (leftPlan->getCost() < maxCost) {
        auto getVPlan = planGetV(leftPlan->shallowCopy(),
                                 rightPlan->shallowCopy(), joinNodeIDs);
        if (getVPlan) {
          appendFilters(predicates, *getVPlan);
          context.addPlan(newSubgraph, std::move(getVPlan));
        }
      }
    }
  }
}

std::shared_ptr<planner::LogicalOperator> Planner::extractIntersect(
    std::shared_ptr<LogicalOperator> top) {
  if (top->getOperatorType() == LogicalOperatorType::INTERSECT) {
    return top;
  }
  if (top->getOperatorType() == LogicalOperatorType::FILTER) {
    return extractIntersect(top->getChild(0));
  }
  return nullptr;
}

std::shared_ptr<planner::LogicalOperator> Planner::extractExtend(
    std::shared_ptr<LogicalOperator> top) {
  if (top->getOperatorType() == LogicalOperatorType::EXTEND ||
      top->getOperatorType() == LogicalOperatorType::RECURSIVE_EXTEND) {
    return top;
  }
  if (top->getOperatorType() == LogicalOperatorType::FILTER ||
      top->getOperatorType() == LogicalOperatorType::NODE_LABEL_FILTER) {
    return extractExtend(top->getChild(0));
  }
  return nullptr;
}

std::shared_ptr<planner::LogicalOperator> Planner::extractGetV(
    std::shared_ptr<LogicalOperator> top) {
  if (top->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
    return top;
  }
  if (top->getOperatorType() == LogicalOperatorType::FILTER) {
    return extractGetV(top->getChild(0));
  }
  return nullptr;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::planCrossProduct(
    std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
    std::vector<std::unique_ptr<LogicalPlan>> rightPlans) {
  std::vector<std::unique_ptr<LogicalPlan>> result;
  for (auto& leftPlan : leftPlans) {
    for (auto& rightPlan : rightPlans) {
      auto leftPlanCopy = leftPlan->shallowCopy();
      auto rightPlanCopy = rightPlan->shallowCopy();
      appendCrossProduct(*leftPlanCopy, *rightPlanCopy, *leftPlanCopy);
      result.push_back(std::move(leftPlanCopy));
    }
  }
  return result;
}

static bool isExpressionNewlyMatched(
    const std::vector<SubqueryGraph>& prevs, const SubqueryGraph& newSubgraph,
    const std::shared_ptr<Expression>& expression) {
  auto collector = DependentVarNameCollector();
  collector.visit(expression);
  auto variables = collector.getVarNames();
  for (auto& prev : prevs) {
    if (prev.containAllVariables(variables)) {
      return false;
    }
  }
  return newSubgraph.containAllVariables(variables);
}

expression_vector Planner::getNewlyMatchedExprs(
    const std::vector<SubqueryGraph>& prevs, const SubqueryGraph& new_,
    const expression_vector& exprs) {
  expression_vector result;
  for (auto& expr : exprs) {
    if (isExpressionNewlyMatched(prevs, new_, expr)) {
      result.push_back(expr);
    }
  }
  return result;
}

expression_vector Planner::getNewlyMatchedExprs(
    const SubqueryGraph& prev, const SubqueryGraph& new_,
    const expression_vector& exprs) {
  return getNewlyMatchedExprs(std::vector<SubqueryGraph>{prev}, new_, exprs);
}

expression_vector Planner::getNewlyMatchedExprs(
    const SubqueryGraph& leftPrev, const SubqueryGraph& rightPrev,
    const SubqueryGraph& new_, const expression_vector& exprs) {
  return getNewlyMatchedExprs(std::vector<SubqueryGraph>{leftPrev, rightPrev},
                              new_, exprs);
}

}  // namespace planner
}  // namespace neug
