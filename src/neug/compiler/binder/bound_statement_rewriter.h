#pragma once

#include "bound_statement.h"
#include "neug/compiler/main/client_context.h"

namespace gs {
namespace binder {

// Perform semantic rewrite over bound statement.
class BoundStatementRewriter {
 public:
  static void rewrite(BoundStatement& boundStatement,
                      main::ClientContext& clientContext);
};

}  // namespace binder
}  // namespace gs
