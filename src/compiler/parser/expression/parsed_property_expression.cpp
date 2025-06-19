#include "src/include/parser/expression/parsed_property_expression.h"

#include "src/include/common/serializer/deserializer.h"

using namespace gs::common;

namespace gs {
namespace parser {

std::unique_ptr<ParsedPropertyExpression> ParsedPropertyExpression::deserialize(
    Deserializer& deserializer) {
  std::string propertyName;
  deserializer.deserializeValue(propertyName);
  return std::make_unique<ParsedPropertyExpression>(std::move(propertyName));
}

}  // namespace parser
}  // namespace gs
