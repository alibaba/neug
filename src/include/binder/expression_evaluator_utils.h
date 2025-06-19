#pragma once

#include "src/include/binder/expression/expression.h"
#include "src/include/common/types/value/value.h"
#include "src/include/main/client_context.h"

namespace gs {
namespace evaluator {

struct ExpressionEvaluatorUtils {
  static KUZU_API common::Value evaluateConstantExpression(
      std::shared_ptr<binder::Expression> expression,
      main::ClientContext* clientContext);
};

}  // namespace evaluator
}  // namespace gs