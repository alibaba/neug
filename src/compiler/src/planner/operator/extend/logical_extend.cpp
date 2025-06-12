#include "planner/operator/extend/logical_extend.h"
#include "gopt/g_rel_table_entry.h"

namespace gs {
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

gopt::GAliasName getGAliasName0(const binder::NodeOrRelExpression& rel) {
  auto queryName = rel.getVariableName().empty()
                       ? std::nullopt
                       : std::make_optional(rel.getVariableName());
  return gopt::GAliasName{rel.getUniqueName(), queryName};
}

gopt::GAliasName LogicalExtend::getGAliasName() const {
  if (opt == planner::ExtendOpt::VERTEX) {
    return getGAliasName0(*nbrNode);
  }
  return getGAliasName0(*rel);
}

std::vector<common::table_id_t> LogicalExtend::getLabelIds() const {
  auto relExpr = getRel();
  auto& tableEntries = relExpr->getEntries();
  std::vector<common::table_id_t> labelIds;
  labelIds.reserve(tableEntries.size());
  for (auto& entry : tableEntries) {
    auto gRel = entry->constPtrCast<catalog::GRelTableCatalogEntry>();
    if (!gRel) {
      throw common::Exception("Invalid relation table entry in extend: " +
                              entry->getName());
    }
    labelIds.emplace_back(gRel->getLabelId());
  }
  return labelIds;
}

std::unique_ptr<gopt::GRelType> LogicalExtend::getRelType() const {
  std::vector<catalog::GRelTableCatalogEntry*> relTables;
  for (const auto& entry : rel->getEntries()) {
    auto gRel = entry->ptrCast<catalog::GRelTableCatalogEntry>();
    if (!gRel) {
      throw common::Exception("Invalid relation table entry in extend: " +
                              entry->getName());
    }
    relTables.emplace_back(gRel);
  }
  return std::make_unique<gopt::GRelType>(std::move(relTables));
}

}  // namespace planner
}  // namespace gs
