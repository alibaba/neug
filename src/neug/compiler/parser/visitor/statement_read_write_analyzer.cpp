#include "neug/compiler/parser/visitor/statement_read_write_analyzer.h"

#include "neug/compiler/parser/expression/parsed_expression_visitor.h"
#include "neug/compiler/parser/query/reading_clause/reading_clause.h"
#include "neug/compiler/parser/query/return_with_clause/with_clause.h"

namespace gs {
namespace parser {

void StatementReadWriteAnalyzer::visitReadingClause(
    const ReadingClause* readingClause) {
  if (readingClause->hasWherePredicate()) {
    if (!isExprReadOnly(readingClause->getWherePredicate())) {
      readOnly = false;
    }
  }
}

void StatementReadWriteAnalyzer::visitWithClause(const WithClause* withClause) {
  for (auto& expr :
       withClause->getProjectionBody()->getProjectionExpressions()) {
    if (!isExprReadOnly(expr.get())) {
      readOnly = false;
      return;
    }
  }
}

void StatementReadWriteAnalyzer::visitReturnClause(
    const ReturnClause* returnClause) {
  for (auto& expr :
       returnClause->getProjectionBody()->getProjectionExpressions()) {
    if (!isExprReadOnly(expr.get())) {
      readOnly = false;
      return;
    }
  }
}

bool StatementReadWriteAnalyzer::isExprReadOnly(const ParsedExpression* expr) {
  auto analyzer = ReadWriteExprAnalyzer(context);
  analyzer.visit(expr);
  return analyzer.isReadOnly();
}

}  // namespace parser
}  // namespace gs
