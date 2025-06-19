#include "src/include/main/query_summary.h"

#include "src/include/common/enums/statement_type.h"

using namespace gs::common;

namespace gs {
namespace main {

double QuerySummary::getCompilingTime() const {
  return preparedSummary.compilingTime;
}

double QuerySummary::getExecutionTime() const { return executionTime; }

void QuerySummary::incrementCompilingTime(double increment) {
  preparedSummary.compilingTime += increment;
}

void QuerySummary::incrementExecutionTime(double increment) {
  executionTime += increment;
}

void QuerySummary::setPreparedSummary(PreparedSummary preparedSummary_) {
  preparedSummary = preparedSummary_;
}

bool QuerySummary::isExplain() const {
  return preparedSummary.statementType == StatementType::EXPLAIN;
}

common::StatementType QuerySummary::getStatementType() const {
  return preparedSummary.statementType;
}

}  // namespace main
}  // namespace gs
