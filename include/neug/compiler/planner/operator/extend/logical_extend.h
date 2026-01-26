#pragma once

#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_rel_table_entry.h"
#include "neug/compiler/planner/operator/extend/base_logical_extend.h"
#include "neug/compiler/storage/predicate/column_predicate.h"

namespace neug {
namespace planner {

enum ExtendOpt { VERTEX = 0, EDGE = 1, DEGREE = 2 };

class LogicalExtend final : public BaseLogicalExtend {
  static constexpr LogicalOperatorType type_ = LogicalOperatorType::EXTEND;

 public:
  LogicalExtend(std::shared_ptr<binder::NodeExpression> boundNode,
                std::shared_ptr<binder::NodeExpression> nbrNode,
                std::shared_ptr<binder::RelExpression> rel,
                common::ExtendDirection direction, bool extendFromSource,
                binder::expression_vector properties,
                std::shared_ptr<LogicalOperator> child,
                common::cardinality_t cardinality = 0)
      : BaseLogicalExtend{type_,
                          std::move(boundNode),
                          std::move(nbrNode),
                          std::move(rel),
                          direction,
                          extendFromSource,
                          std::move(child)},
        scanNbrID{true},
        properties{std::move(properties)},
        opt{planner::ExtendOpt::EDGE},
        optional{false} {
    this->cardinality = cardinality;
  }

  f_group_pos_set getGroupsPosToFlatten() override { return f_group_pos_set{}; }
  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  binder::expression_vector getProperties() const { return properties; }
  void setPropertyPredicates(
      std::vector<storage::ColumnPredicateSet> predicates) {
    propertyPredicates = std::move(predicates);
  }
  const std::vector<storage::ColumnPredicateSet>& getPropertyPredicates()
      const {
    return propertyPredicates;
  }
  void setScanNbrID(bool scanNbrID_) { scanNbrID = scanNbrID_; }
  bool shouldScanNbrID() const { return scanNbrID; }

  std::unique_ptr<LogicalOperator> copy() override;

  std::vector<common::table_id_t> getLabelIds() const;

  std::string getAliasName() const;

  std::string getStartAliasName() const;

  gopt::GAliasName getGAliasName() const;

  gopt::GAliasName getExprGAliasName(
      const binder::NodeOrRelExpression& rel) const;

  std::unique_ptr<gopt::GRelType> getRelType() const;

  void setPredicates(std::shared_ptr<binder::Expression> predicates_) {
    predicates = std::move(predicates_);
  }

  std::shared_ptr<binder::Expression> getPredicates() const {
    return predicates;
  }

  void setExtendOpt(planner::ExtendOpt opt_) { opt = opt_; }

  planner::ExtendOpt getExtendOpt() const { return opt; }

  void setOptional(bool optional_) { optional = optional_; }
  bool isOptional() const { return optional; }

  std::string getExpressionsForPrinting() const override {
    auto base = BaseLogicalExtend::getExpressionsForPrinting();
    base += " Cardinality: " + std::to_string(cardinality);
    if (predicates) {
      base += " WHERE " + predicates->toString();
    }
    if (optional) {
      base += " OPTIONAL";
    }
    return base;
  }

 private:
  bool scanNbrID;
  binder::expression_vector properties;
  std::vector<storage::ColumnPredicateSet> propertyPredicates;
  std::shared_ptr<binder::Expression> predicates;
  planner::ExtendOpt opt;
  bool optional;
};

}  // namespace planner
}  // namespace neug
