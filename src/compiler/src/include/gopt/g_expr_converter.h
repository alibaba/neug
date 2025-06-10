#pragma once

#include <memory>
#include "binder/expression/expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/variable_expression.h"
#include "common/types/types.h"
#include "gopt/g_alias_manager.h"
#include "gopt/g_type_converter.h"
#include "src/proto_generated_gie/algebra.pb.h"
#include "src/proto_generated_gie/expr.pb.h"

namespace gs {
namespace gopt {

class GExprConverter {
 public:
  GExprConverter(const std::shared_ptr<gopt::GAliasManager> aliasManager)
      : aliasManager{std::move(aliasManager)} {}

  // Main conversion function
  std::unique_ptr<::common::Expression> convert(const binder::Expression& expr);
  std::unique_ptr<::algebra::IndexPredicate> convertPrimaryKey(
      const std::string& key, const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertVar(common::alias_id_t columnId);

 private:
  // Core expression type converters
  std::unique_ptr<::common::Expression> convertLiteral(
      const binder::LiteralExpression& expr);
  std::unique_ptr<::common::Expression> convertProperty(
      const binder::PropertyExpression& expr);
  std::unique_ptr<::common::Expression> convertVariable(
      const binder::VariableExpression& expr);
  std::unique_ptr<::common::Expression> convertComparison(
      const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertAnd(
      const binder::Expression& expr);

  // helper functions
  std::unique_ptr<::common::Value> convertValue(gs::common::Value value);
  std::unique_ptr<::common::NameOrId> convertAlias(common::alias_id_t aliasId);
  std::unique_ptr<::common::Variable> convertVarProperty(
      const std::string& aliasName, const std::string& propertyName,
      common::LogicalType& type);
  std::unique_ptr<::common::Variable> convertSingleVar(
      const std::string& aliasName, const common::LogicalType& type);
  ::common::Logical convertCompare(common::ExpressionType type);
  std::unique_ptr<::common::Expression> convertPattern(
      const binder::NodeOrRelExpression& expr);

 private:
  const std::shared_ptr<gopt::GAliasManager> aliasManager;
  gopt::GTypeConverter typeConverter;
};

}  // namespace gopt
}  // namespace gs