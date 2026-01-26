/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/optimizer/expand_getv_fusion.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"

namespace neug {
namespace optimizer {

void ExpandGetVFusion::rewrite(planner::LogicalPlan* plan) {
  auto root = plan->getLastOperator();
  auto rootOpt = visitOperator(root);
  plan->setLastOperator(rootOpt);
}

std::shared_ptr<planner::LogicalOperator> ExpandGetVFusion::visitOperator(
    const std::shared_ptr<planner::LogicalOperator>& op) {
  // bottom-up traversal
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    op->setChild(i, visitOperator(op->getChild(i)));
  }
  auto result = visitOperatorReplaceSwitch(op);
  // schema of each operator is unchanged
  // result->computeFlatSchema();
  return result;
}

std::shared_ptr<planner::LogicalOperator>
ExpandGetVFusion::visitRecursiveExtendReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  NEUG_ASSERT(op->getOperatorType() ==
              planner::LogicalOperatorType::RECURSIVE_EXTEND);
  auto extendOp = op->ptrCast<planner::LogicalRecursiveExtend>();
  if (extendOp->getResultOpt() == planner::ResultOpt::ALL_V_E) {
    // 'ALL_V_E' means edge must be kept independently, so we cannot fuse
    return op;
  }
  auto recursiveInfo = extendOp->getRel()->getRecursiveInfo();
  // check if the base node need to filter by predicates, if exist, we cannot
  // fuse.
  if (recursiveInfo && recursiveInfo->nodePredicate) {
    return op;
  }

  // check if the base node need to filter by label, if exist, we cannot fuse.
  if (hasLabelFiltering(gopt::GNodeType{*extendOp->getNbrNode()},
                        gopt::GRelType{*extendOp->getRel()},
                        gopt::GNodeType{*extendOp->getBoundNode()},
                        extendOp->getDirection(), catalog)) {
    return op;
  }
  // todo: handle the case of EXPANDV_GETV
  extendOp->setFusionType(neug::optimizer::FusionType::EXPANDV);
  return op;
}

std::shared_ptr<planner::LogicalOperator> ExpandGetVFusion::visitGetVReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  NEUG_ASSERT(op->getOperatorType() == planner::LogicalOperatorType::GET_V);
  auto child = op->getChild(0);
  if (child->getOperatorType() != planner::LogicalOperatorType::EXTEND) {
    return op;  // no extend operator, nothing to fuse
  }
  auto fusionType = analyze(op, child);
  return perform(op, child, fusionType);
}

FusionType ExpandGetVFusion::analyze(
    std::shared_ptr<planner::LogicalOperator> getV,
    std::shared_ptr<planner::LogicalOperator> expand) {
  auto getVOp = getV->constPtrCast<planner::LogicalGetV>();
  auto expandOp = expand->constPtrCast<planner::LogicalExtend>();
  auto alias = expandOp->getGAliasName();
  // expand has a query given alias, cannot be fused;
  if (alias.queryName.has_value()) {
    return FusionType::EXPANDE_GETV;
  }
  auto nbrExpr = expandOp->getNbrNode();
  auto getVAlias = getVOp->getGAliasName();
  // the start alias of getV is not aligned with the expand, cannot be fused;
  if (getVOp->getNodeID()->getUniqueName() !=
      nbrExpr->getInternalID()->getUniqueName()) {
    LOG(WARNING) << "Alias name of GetV is not consistent with Expand, skip "
                    "'ExpandGetVFusion' optimization";
    return FusionType::EXPANDE_GETV;
  }
  // todo: support EXTANDV_GETV
  return hasGetVFiltering(getV, expand) ? FusionType::EXPANDE_GETV
                                        : FusionType::EXPANDV;  // can be fused
}

bool ExpandGetVFusion::hasGetVFiltering(
    std::shared_ptr<planner::LogicalOperator> getV,
    std::shared_ptr<planner::LogicalOperator> expand) {
  auto getVOp = getV->constPtrCast<planner::LogicalGetV>();
  auto expandOp = expand->constPtrCast<planner::LogicalExtend>();
  if (getVOp->getPredicates() != nullptr)
    return true;
  auto getVType = getVOp->getNodeType(catalog);
  auto expandType = expandOp->getRelType();
  gopt::GNodeType sourceType(*expandOp->getBoundNode());
  return hasLabelFiltering(*getVType, *expandType, sourceType,
                           expandOp->getDirection(), catalog);
}

// the expand_edge in physical pb only support filtering edge labels by edge
// type itself, we need to lookup the catalog to match all triplets <src, edge,
// dst> by the expandType, thus can determine whether label filtering in getV is
// necessary or not.
gopt::GRelType ExpandGetVFusion::transformExpandType(
    const gopt::GRelType& expandType, catalog::Catalog* catalog) {
  auto& transaction = Constants::DEFAULT_TRANSACTION;
  std::vector<common::table_id_t> targetLabels =
      std::move(expandType.getLabelIds());
  std::vector<catalog::GRelTableCatalogEntry*> transformEntries;
  for (auto& entry : catalog->getTableEntries(&transaction)) {
    if (entry->getType() == catalog::CatalogEntryType::REL_TABLE_ENTRY) {
      auto edgeEntry = entry->ptrCast<catalog::GRelTableCatalogEntry>();
      if (std::find(targetLabels.begin(), targetLabels.end(),
                    edgeEntry->getLabelId()) != targetLabels.end()) {
        // if the edge label is in the expand type, we keep it
        transformEntries.emplace_back(edgeEntry);
      }
    }
  }
  return gopt::GRelType(std::move(transformEntries));
}

bool ExpandGetVFusion::hasLabelFiltering(const gopt::GNodeType& getVType,
                                         const gopt::GRelType& expandType,
                                         const gopt::GNodeType& sourceType,
                                         common::ExtendDirection direction,
                                         catalog::Catalog* catalog) {
  std::vector<common::table_id_t> targetLabels;
  auto transformType = transformExpandType(expandType, catalog);
  for (auto& edge : transformType.relTables) {
    for (auto& node : sourceType.nodeTables) {
      if (direction != common::ExtendDirection::BWD &&
          node->getTableID() == edge->getSrcTableID()) {
        targetLabels.push_back(edge->getDstTableID());
      } else if (direction != common::ExtendDirection::FWD &&
                 node->getTableID() == edge->getDstTableID()) {
        targetLabels.push_back(edge->getSrcTableID());
      }
    }
  }
  std::vector<common::table_id_t> getVLabels;
  for (auto& node : getVType.nodeTables) {
    getVLabels.push_back(node->getTableID());
  }
  for (auto& targetLabel : targetLabels) {
    if (std::find(getVLabels.begin(), getVLabels.end(), targetLabel) ==
        getVLabels.end()) {
      return true;  // at least one label in expand is not in getV
    }
  }
  return false;  // all labels in expand are in getV
}

std::shared_ptr<planner::LogicalOperator> ExpandGetVFusion::perform(
    std::shared_ptr<planner::LogicalOperator> getV,
    std::shared_ptr<planner::LogicalOperator> expand, FusionType fusionType) {
  auto expandOp = expand->ptrCast<planner::LogicalExtend>();
  auto getVOp = getV->ptrCast<planner::LogicalGetV>();
  switch (fusionType) {
  case EXPANDV_GETV:
    // for this condition, expandOp and getVOp will have the same aliasId,
    // this will lead execution issues, so we currently do not support this
    // case.
    // todo: set alias of expand as empty
    expandOp->setExtendOpt(planner::ExtendOpt::VERTEX);
    getVOp->setGetVOpt(planner::GetVOpt::ITSELF);
    return getV;
  case EXPANDV:
    expandOp->setExtendOpt(planner::ExtendOpt::VERTEX);
    // getV has been fused into expandV, let expand as the new operator
    return expand;
  default:
    return getV;  // EXPANDE_GETV, no fusion, return original getV
  }
}

}  // namespace optimizer
}  // namespace neug