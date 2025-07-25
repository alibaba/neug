#include "neug/compiler/main/prepared_statement.h"

#include "neug/compiler/binder/bound_statement_result.h"  // IWYU pragma: keep (used to avoid error in destructor)
#include "neug/compiler/common/enums/statement_type.h"
#include "neug/compiler/planner/operator/logical_plan.h"

using namespace gs::common;

namespace gs {
namespace main {

bool PreparedStatement::isTransactionStatement() const {
  return preparedSummary.statementType == StatementType::TRANSACTION;
}

bool PreparedStatement::isSuccess() const { return success; }

std::string PreparedStatement::getErrorMessage() const { return errMsg; }

bool PreparedStatement::isReadOnly() const { return readOnly; }

bool PreparedStatement::isProfile() const { return logicalPlan->isProfile(); }

StatementType PreparedStatement::getStatementType() const {
  return parsedStatement->getStatementType();
}

PreparedStatement::~PreparedStatement() = default;

}  // namespace main
}  // namespace gs
