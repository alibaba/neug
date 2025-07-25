#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/main/client_context.h"

namespace gs {
namespace evaluator {

struct ExpressionEvaluatorUtils {
  static KUZU_API common::Value evaluateConstantExpression(
      std::shared_ptr<binder::Expression> expression,
      main::ClientContext* clientContext);
};

}  // namespace evaluator
}  // namespace gs