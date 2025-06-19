#include "src/include/binder/binder.h"
#include "src/include/binder/bound_explain.h"
#include "src/include/parser/explain_statement.h"

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExplain(
    const parser::Statement& statement) {
  auto& explain = statement.constCast<parser::ExplainStatement>();
  auto boundStatementToExplain = bind(*explain.getStatementToExplain());
  return std::make_unique<BoundExplain>(std::move(boundStatementToExplain),
                                        explain.getExplainType());
}

}  // namespace binder
}  // namespace gs
