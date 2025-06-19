#include "src/include/binder/visitor/confidential_statement_analyzer.h"

#include "src/include/binder/bound_standalone_call.h"

using namespace gs::common;

namespace gs {
namespace binder {

void ConfidentialStatementAnalyzer::visitStandaloneCall(
    const BoundStatement& boundStatement) {
  auto& standaloneCall = boundStatement.constCast<BoundStandaloneCall>();
  confidentialStatement = standaloneCall.getOption()->isConfidential;
}

}  // namespace binder
}  // namespace gs
