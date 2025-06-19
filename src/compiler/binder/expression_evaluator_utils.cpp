#include "src/include/binder/expression_evaluator_utils.h"

#include "src/include/binder/expression_mapper.h"
#include "src/include/common/types/value/value.h"

using namespace gs::common;
using namespace gs::processor;

namespace gs {
namespace evaluator {

Value ExpressionEvaluatorUtils::evaluateConstantExpression(
    std::shared_ptr<binder::Expression> expression,
    main::ClientContext* clientContext) {
  auto exprMapper = ExpressionMapper();
  auto evaluator = exprMapper.getConstantEvaluator(expression);
  auto emptyResultSet = std::make_unique<ResultSet>(0);
  evaluator->init(*emptyResultSet, clientContext);
  evaluator->evaluate();
  auto& selVector = evaluator->resultVector->state->getSelVector();
  KU_ASSERT(selVector.getSelSize() == 1);
  return *evaluator->resultVector->getAsValue(selVector[0]);
}

}  // namespace evaluator
}  // namespace gs