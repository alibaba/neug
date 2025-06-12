#include <cmath>
#include <iostream>
#include <memory>

#include "binder/expression_visitor.h"
#include "common/enums/join_type.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "main/client_context.h"
#include "planner/join_order/cost_model.h"
#include "planner/join_order/join_plan_solver.h"
#include "planner/join_order/join_tree_constructor.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_get_v.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace gs::binder;
using namespace gs::common;

namespace gs {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  return getBestPlan(enumerateQueryGraphCollection(queryGraphCollection, info));
}

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollectionInNewContext(
    const QueryGraphCollection& queryGraphCollection,
    const QueryGraphPlanningInfo& info) {
  auto prevContext = enterNewContext();
  auto plans = enumerateQueryGraphCollection(queryGraphCollection, info);
  exitContext(std::move(prevContext));
  return getBestPlan(std::move(plans));
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
  KU_ASSERT(queryGraphCollection.getNumQueryGraphs() > 0);
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
      KU_UNREACHABLE;
    }
    plansPerQueryGraph.push_back(std::move(plans));
  }
  if (info.subqueryType == SubqueryPlanningType::CORRELATED &&
      queryGraphIdxToPlanExpressionsScan == -1) {
    auto plan = std::make_unique<LogicalPlan>();
    appendExpressionsScan(corrExprs, *plan);
    appendDistinct(corrExprs, *plan);
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    plans.push_back(std::move(plan));
    plansPerQueryGraph.push_back(std::move(plans));
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
  KU_ASSERT(level > 1);
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
        planNodeIDScan(nodePos, info);
      } else {
        planNodeScan(nodePos);
      }
    }
  } break;
  case SubqueryPlanningType::CORRELATED: {
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes();
         ++nodePos) {
      auto queryNode = queryGraph->getQueryNode(nodePos);
      if (info.containsCorrExpr(*queryNode->getInternalID())) {
        continue;
      }
      planNodeScan(nodePos);
    }
    planCorrelatedExpressionsScan(info);
  } break;
  default:
    KU_UNREACHABLE;
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
  auto predicates =
      getNewlyMatchedExprs(context.getEmptySubqueryGraph(), newSubgraph,
                           context.getWhereExpressions());
  appendFilters(predicates, *plan);
  appendDistinct(corrExprs, *plan);
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

  appendScanNodeTable(node->getInternalID(), node->getTableIDs(), {}, *plan);
  context.addPlan(newSubgraph, std::move(plan));
}

static std::pair<std::shared_ptr<NodeExpression>,
                 std::shared_ptr<NodeExpression>>
getBoundAndNbrNodes(const RelExpression& rel, ExtendDirection direction) {
  KU_ASSERT(direction != ExtendDirection::BOTH);
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
    KU_ASSERT(relExpression.getExtendDirections().size() ==
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
    KU_UNREACHABLE;
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
  KU_ASSERT(leftLevel <= rightLevel);
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
  const auto sequentialScan = ku_dynamic_cast<LogicalScanNodeTable*>(seqScan);
  return sequentialScan->getNodeID()->getUniqueName() ==
         node.getInternalID()->getUniqueName();
}

static bool isNodeSequentialOnPlan2(const LogicalPlan& plan,
                                    const NodeExpression& node) {
  const auto seqScan = getSequentialScan(plan.getLastOperator().get());
  const auto sequentialScan = ku_dynamic_cast<LogicalScanNodeTable*>(seqScan);
  return sequentialScan->getNodeID()->getUniqueName() ==
         node.getInternalID()->getUniqueName();
}

static std::unique_ptr<LogicalPlan> getWCOJBuildPlanForRel(
    std::vector<std::unique_ptr<LogicalPlan>>& candidatePlans,
    const NodeExpression& boundNode) {
  std::unique_ptr<LogicalPlan> result;
  for (auto& candidatePlan : candidatePlans) {
    if (isNodeSequentialOnPlan(*candidatePlan, boundNode)) {
      KU_ASSERT(result == nullptr);
      result = candidatePlan->shallowCopy();
    }
  }
  return result;
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
  for (auto& rel : rels) {
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
    prevSubgraphs.push_back(subgraph);
    newSubgraph.addQueryRel(relPos);
    auto relSubgraph = context.getEmptySubqueryGraph();
    relSubgraph.addQueryRel(relPos);
    KU_ASSERT(context.subPlansTable->containSubgraphPlans(relSubgraph));
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
    auto leftPlanCopy = leftPlan->shallowCopy();
    std::vector<std::unique_ptr<LogicalPlan>> rightPlansCopy;
    rightPlansCopy.reserve(relPlans.size());
    for (auto& relPlan : relPlans) {
      rightPlansCopy.push_back(relPlan->shallowCopy());
    }
    appendIntersect(intersectNode->getInternalID(), boundNodeIDs, *leftPlanCopy,
                    rightPlansCopy);
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
  KU_ASSERT(leftLevel <= rightLevel);
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
      if (joinNodes.size() > 1) {
        continue;
      }
      if (tryPlanINLJoin(rightSubgraph, nbrSubgraph, joinNodes)) {
        continue;
      }
      // planInnerHashJoin(rightSubgraph, nbrSubgraph, joinNodes,
      //                   leftLevel != rightLevel);
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
  KU_ASSERT(relPos != UINT32_MAX);
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

planner::GetVOpt getGetVOpt(const planner::LogicalExtend& extend) {
  if (extend.getExtendOpt() == planner::ExtendOpt::VERTEX) {
    return planner::GetVOpt::ITSELF;
  }
  switch (extend.getDirection()) {
  case common::ExtendDirection::FWD:
    return planner::GetVOpt::END;
  case common::ExtendDirection::BWD:
    return planner::GetVOpt::START;
  case common::ExtendDirection::BOTH:
    return planner::GetVOpt::OTHER;
  default:
    throw std::runtime_error("Unsupported extend direction for GetV: " +
                             static_cast<u_int8_t>(extend.getDirection()));
  }
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
      if (CostModel::computeHashJoinCost(joinNodeIDs, *rightPlan, *leftPlan) <
          maxCost) {
        // extract extend operator from leftPlan, which top may be filtering
        // operator
        auto leftExtend = std::dynamic_pointer_cast<LogicalExtend>(
            extractExtend(leftPlan->getLastOperator()));
        // extract getV operator from rightPlan, which top may be filtering
        // operator
        auto rightGetV = std::dynamic_pointer_cast<LogicalScanNodeTable>(
            extractGetV(rightPlan->getLastOperator()));
        if (leftExtend && rightGetV) {
          // build join for cost, cardinality and schema
          auto leftPlanBuildCopy = leftPlan->shallowCopy();
          auto rightPlanProbeCopy = rightPlan->shallowCopy();
          appendHashJoin(joinNodeIDs, JoinType::INNER, *rightPlanProbeCopy,
                         *leftPlanBuildCopy, *rightPlanProbeCopy);
          auto joinCard =
              rightPlanProbeCopy->getLastOperator()->getCardinality();
          auto joinCost = rightPlanProbeCopy->getCost();
          auto joinSchema = rightPlanProbeCopy->getSchema();

          // start to build the getV operator
          auto getVPlan = std::make_unique<LogicalPlan>();
          auto getV = std::make_unique<planner::LogicalGetV>(
              rightGetV->getNodeID(), rightGetV->getTableIDs(),
              rightGetV->getProperties(), getGetVOpt(*leftExtend),
              leftExtend->getRel(), leftPlan->getLastOperator(),
              joinSchema->copy(), joinCard);
          getVPlan->setLastOperator(std::move(getV));
          // append filtering predicates corresponding to the getV
          auto getVFilter = std::dynamic_pointer_cast<LogicalFilter>(
              rightPlan->getLastOperator());
          while (getVFilter) {
            appendFilter(getVFilter->getPredicate(), *getVPlan);
            if (getVFilter->getNumChildren() == 0) {
              break;
            }
            getVFilter = std::dynamic_pointer_cast<LogicalFilter>(
                getVFilter->getChild(0));
          }
          getVPlan->setCost(joinCost);
          // appendFilters(predicates, *getVPlan);
          context.addPlan(newSubgraph, std::move(getVPlan));
          continue;
        }
      }
    }
  }
}

std::shared_ptr<planner::LogicalOperator> Planner::extractExtend(
    std::shared_ptr<LogicalOperator> top) {
  if (top->getOperatorType() == LogicalOperatorType::EXTEND) {
    return top;
  }
  if (top->getOperatorType() == LogicalOperatorType::FILTER) {
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
}  // namespace gs
