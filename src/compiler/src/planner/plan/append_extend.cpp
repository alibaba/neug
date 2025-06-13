#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/join_type.h"
#include "graph/graph_entry.h"
#include "main/client_context.h"
#include "planner/join_order/cost_model.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "planner/operator/logical_node_label_filter.h"
#include "planner/operator/logical_path_property_probe.h"
#include "planner/planner.h"

using namespace gs::common;
using namespace gs::binder;
using namespace gs::catalog;
using namespace gs::transaction;
using namespace gs::function;

namespace gs {
namespace planner {

static std::unordered_set<table_id_t> getBoundNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection) {
  std::unordered_set<table_id_t> result;
  for (auto entry : rel.getEntries()) {
    auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
    switch (extendDirection) {
    case ExtendDirection::FWD: {
      result.insert(relTableEntry.getBoundTableID(RelDataDirection::FWD));
    } break;
    case ExtendDirection::BWD: {
      result.insert(relTableEntry.getBoundTableID(RelDataDirection::BWD));
    } break;
    case ExtendDirection::BOTH: {
      result.insert(relTableEntry.getBoundTableID(RelDataDirection::FWD));
      result.insert(relTableEntry.getBoundTableID(RelDataDirection::BWD));
    } break;
    default:
      KU_UNREACHABLE;
    }
  }
  return result;
}

static std::unordered_set<table_id_t> getNbrNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection) {
  std::unordered_set<table_id_t> result;
  for (auto entry : rel.getEntries()) {
    auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
    switch (extendDirection) {
    case ExtendDirection::FWD: {
      result.insert(relTableEntry.getNbrTableID(RelDataDirection::FWD));
    } break;
    case ExtendDirection::BWD: {
      result.insert(relTableEntry.getNbrTableID(RelDataDirection::BWD));
    } break;
    case ExtendDirection::BOTH: {
      result.insert(relTableEntry.getNbrTableID(RelDataDirection::FWD));
      result.insert(relTableEntry.getNbrTableID(RelDataDirection::BWD));
    } break;
    default:
      KU_UNREACHABLE;
    }
  }
  return result;
}

void Planner::appendNonRecursiveExtend(
    const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode,
    const std::shared_ptr<RelExpression>& rel, ExtendDirection direction,
    bool extendFromSource, const expression_vector& properties,
    LogicalPlan& plan) {
  // Filter bound node label if we know some incoming nodes won't have any
  // outgoing rel. This cannot be done at binding time because the pruning is
  // affected by extend direction.
  auto boundNodeTableIDSet = getBoundNodeTableIDSet(*rel, direction);
  if (boundNode->getNumEntries() > boundNodeTableIDSet.size()) {
    appendNodeLabelFilter(boundNode->getInternalID(), boundNodeTableIDSet,
                          plan);
  }
  auto properties_ = properties;
  // Append extend
  auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel, direction,
                                           extendFromSource, properties_,
                                           plan.getLastOperator());
  extend->computeFactorizedSchema();
  // Update cost & cardinality. Note that extend does not change factorized
  // cardinality.
  const auto extensionRate = cardinalityEstimator.getExtensionRate(
      *rel, *boundNode, clientContext->getTransaction());
  extend->setCardinality(plan.getLastOperator()->getCardinality());
  plan.setCost(plan.getCost() + plan.getCardinality() * extensionRate);
  auto group = extend->getSchema()->getGroup(nbrNode->getInternalID());
  group->setMultiplier(extensionRate);
  plan.setLastOperator(std::move(extend));
  auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction);
  if (nbrNodeTableIDSet.size() > nbrNode->getNumEntries()) {
    appendNodeLabelFilter(nbrNode->getInternalID(), nbrNode->getTableIDsSet(),
                          plan);
  }
}

void Planner::appendRecursiveExtend(
    const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode,
    const std::shared_ptr<RelExpression>& rel, ExtendDirection direction,
    LogicalPlan& plan) {
  // GDS pipeline
  auto recursiveInfo = rel->getRecursiveInfo();
  // Fill bind data with direction information. This can only be decided at
  // planning time.
  auto bindData = recursiveInfo->bindData.get();
  bindData->nodeOutput = nbrNode;
  bindData->nodeInput = boundNode;
  bindData->extendDirection = direction;
  // If we extend from right to left, we need to print path in reverse
  // direction.
  bindData->flipPath = *boundNode == *rel->getRightNode();

  auto resultColumns = recursiveInfo->function->getResultColumns(*bindData);

  auto recursiveExtend = std::make_shared<LogicalRecursiveExtend>(
      recursiveInfo->function->copy(), *recursiveInfo->bindData, resultColumns,
      rel);

  if (plan.getLastOperator()) {
    recursiveExtend->addChild(plan.getLastOperator());
  }

  recursiveExtend->computeFactorizedSchema();

  auto extensionRate = cardinalityEstimator.getExtensionRate(
      *rel, *boundNode, clientContext->getTransaction());
  auto resultCard = cardinalityEstimator.multiply(
      extensionRate, plan.getLastOperator()->getCardinality());

  recursiveExtend->setCardinality(resultCard);
  plan.setCost(plan.getCost() + resultCard);
  plan.setLastOperator(std::move(recursiveExtend));
}

void Planner::createPathNodePropertyScanPlan(
    const std::shared_ptr<NodeExpression>& node,
    const expression_vector& properties, LogicalPlan& plan) {
  appendScanNodeTable(node->getInternalID(), node->getTableIDs(), properties,
                      plan);
}

void Planner::createPathRelPropertyScanPlan(
    const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode,
    const std::shared_ptr<RelExpression>& rel, ExtendDirection direction,
    bool extendFromSource, const expression_vector& properties,
    LogicalPlan& plan) {
  appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(), {},
                      plan);
  appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, extendFromSource,
                           properties, plan);
  appendProjection(properties, plan);
}

void Planner::appendNodeLabelFilter(std::shared_ptr<Expression> nodeID,
                                    std::unordered_set<table_id_t> tableIDSet,
                                    LogicalPlan& plan) {
  auto filter = std::make_shared<LogicalNodeLabelFilter>(
      std::move(nodeID), std::move(tableIDSet), plan.getLastOperator());
  filter->computeFactorizedSchema();
  plan.setLastOperator(std::move(filter));
}

}  // namespace planner
}  // namespace gs
