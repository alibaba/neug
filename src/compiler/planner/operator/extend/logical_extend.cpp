#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/gopt/g_rel_table_entry.h"

namespace neug {
namespace planner {

void LogicalExtend::computeFactorizedSchema() {
  copyChildSchema(0);
  const auto boundGroupPos = schema->getGroupPos(*boundNode->getInternalID());
  if (!schema->getGroup(boundGroupPos)->isFlat()) {
    schema->flattenGroup(boundGroupPos);
  }
  uint32_t nbrGroupPos = 0u;
  nbrGroupPos = schema->createGroup();
  schema->insertToGroupAndScope(nbrNode->getInternalID(), nbrGroupPos);
  for (auto& property : properties) {
    schema->insertToGroupAndScope(property, nbrGroupPos);
  }
  if (rel->hasDirectionExpr()) {
    schema->insertToGroupAndScope(rel->getDirectionExpr(), nbrGroupPos);
  }
}

void LogicalExtend::computeFlatSchema() {
  copyChildSchema(0);
  schema->insertToGroupAndScope(nbrNode->getInternalID(), 0);
  for (auto& property : properties) {
    schema->insertToGroupAndScope(property, 0);
  }
  if (rel->hasDirectionExpr()) {
    schema->insertToGroupAndScope(rel->getDirectionExpr(), 0);
  }
}

std::unique_ptr<LogicalOperator> LogicalExtend::copy() {
  auto extend = std::make_unique<LogicalExtend>(
      boundNode, nbrNode, rel, direction, extendFromSource_, properties,
      children[0]->copy(), cardinality);
  extend->setPropertyPredicates(copyVector(propertyPredicates));
  extend->scanNbrID = scanNbrID;
  extend->setPredicates(getPredicates());
  extend->setExtendOpt(opt);
  extend->setOptional(optional);
  return extend;
}

std::string LogicalExtend::getAliasName() const {
  if (opt == planner::ExtendOpt::VERTEX) {
    return nbrNode->getUniqueName();
  }
  return rel->getUniqueName();
}

std::string LogicalExtend::getStartAliasName() const {
  return boundNode->getUniqueName();
}

gopt::GAliasName LogicalExtend::getExprGAliasName(
    const binder::NodeOrRelExpression& rel) const {
  auto queryName = rel.getVariableName().empty()
                       ? std::nullopt
                       : std::make_optional(rel.getVariableName());
  return gopt::GAliasName{rel.getUniqueName(), queryName};
}

gopt::GAliasName LogicalExtend::getGAliasName() const {
  if (opt == planner::ExtendOpt::VERTEX) {
    return getExprGAliasName(*nbrNode);
  }
  return getExprGAliasName(*rel);
}

std::vector<common::table_id_t> LogicalExtend::getLabelIds() const {
  gopt::GRelType relType(*rel);
  return relType.getLabelIds();
}

std::unique_ptr<gopt::GRelType> LogicalExtend::getRelType() const {
  return std::make_unique<gopt::GRelType>(*rel);
}

}  // namespace planner
}  // namespace neug
