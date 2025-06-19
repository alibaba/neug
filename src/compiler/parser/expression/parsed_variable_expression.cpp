#include "src/include/parser/expression/parsed_variable_expression.h"

#include "src/include/common/serializer/deserializer.h"

using namespace gs::common;

namespace gs {
namespace parser {

std::unique_ptr<ParsedVariableExpression> ParsedVariableExpression::deserialize(
    Deserializer& deserializer) {
  std::string variableName;
  deserializer.deserializeValue(variableName);
  return std::make_unique<ParsedVariableExpression>(std::move(variableName));
}

}  // namespace parser
}  // namespace gs
