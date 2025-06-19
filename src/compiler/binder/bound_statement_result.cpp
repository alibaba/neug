#include "src/include/binder/bound_statement_result.h"

#include "src/include/binder/expression/literal_expression.h"

using namespace gs::common;

namespace gs {
namespace binder {

BoundStatementResult BoundStatementResult::createSingleStringColumnResult(
    const std::string& columnName) {
  auto result = BoundStatementResult();
  auto value = Value(LogicalType::STRING(), columnName);
  auto stringColumn =
      std::make_shared<LiteralExpression>(std::move(value), columnName);
  result.addColumn(columnName, stringColumn);
  return result;
}

}  // namespace binder
}  // namespace gs
