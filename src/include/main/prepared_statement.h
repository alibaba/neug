#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "kuzu_fwd.h"
#include "query_summary.h"
#include "src/include/common/api.h"
#include "src/include/parser/statement.h"

namespace gs {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning
 * the same query for repeated execution.
 */
class PreparedStatement {
  friend class Connection;
  friend class ClientContext;
  friend class testing::TestHelper;
  friend class testing::TestRunner;

 public:
  bool isTransactionStatement() const;
  /**
   * @return the query is prepared successfully or not.
   */
  KUZU_API bool isSuccess() const;
  /**
   * @return the error message if the query is not prepared successfully.
   */
  KUZU_API std::string getErrorMessage() const;
  /**
   * @return the prepared statement is read-only or not.
   */
  KUZU_API bool isReadOnly() const;

  std::unordered_map<std::string, std::shared_ptr<common::Value>>
  getParameterMap() {
    return parameterMap;
  }

  common::StatementType getStatementType() const;

  KUZU_API ~PreparedStatement();

  std::unique_ptr<planner::LogicalPlan> logicalPlan;
  bool success = true;
  std::string errMsg;

 private:
  bool isProfile() const;

 private:
  bool readOnly = false;
  bool useInternalCatalogEntry = false;
  PreparedSummary preparedSummary;
  std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
  std::unique_ptr<binder::BoundStatementResult> statementResult;
  std::shared_ptr<parser::Statement> parsedStatement;
};

}  // namespace main
}  // namespace gs
