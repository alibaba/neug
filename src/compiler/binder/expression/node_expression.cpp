#include "src/include/binder/expression/node_expression.h"

#include "src/include/binder/expression/property_expression.h"

namespace gs {
namespace binder {

NodeExpression::~NodeExpression() = default;

std::shared_ptr<Expression> NodeExpression::getPrimaryKey(
    common::table_id_t tableID) const {
  for (auto& e : propertyExprs) {
    if (e->constCast<PropertyExpression>().isPrimaryKey(tableID)) {
      return e->copy();
    }
  }
  KU_UNREACHABLE;
}

}  // namespace binder
}  // namespace gs
