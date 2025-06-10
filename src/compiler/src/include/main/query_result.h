#pragma once

#include <string>

#include "common/api.h"
#include "common/arrow/arrow.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "kuzu_fwd.h"
#include "query_summary.h"

namespace gs {
namespace main {

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
  friend class Connection;
  friend class ClientContext;
  class QueryResultIterator {
   private:
    QueryResult* currentResult;

   public:
    QueryResultIterator() = default;

    explicit QueryResultIterator(QueryResult* startResult)
        : currentResult(startResult) {}

    void operator++() {
      if (currentResult) {
        currentResult = currentResult->nextQueryResult.get();
      }
    }

    bool isEnd() const { return currentResult == nullptr; }

    bool hasNextQueryResult() const {
      return currentResult->nextQueryResult != nullptr;
    }

    QueryResult* getCurrentResult() const { return currentResult; }
  };

 public:
  /**
   * @brief Used to create a QueryResult object for the failing query.
   */
  KUZU_API QueryResult();

  explicit QueryResult(const PreparedSummary& preparedSummary);
  /**
   * @brief Deconstructs the QueryResult object.
   */
  KUZU_API ~QueryResult();
  /**
   * @return query is executed successfully or not.
   */
  KUZU_API bool isSuccess() const;
  /**
   * @return error message of the query execution if the query fails.
   */
  KUZU_API std::string getErrorMessage() const;
  /**
   * @return number of columns in query result.
   */
  KUZU_API size_t getNumColumns() const;
  /**
   * @return name of each column in query result.
   */
  KUZU_API std::vector<std::string> getColumnNames() const;
  /**
   * @return dataType of each column in query result.
   */
  KUZU_API std::vector<common::LogicalType> getColumnDataTypes() const;
  /**
   * @return num of tuples in query result.
   */
  KUZU_API uint64_t getNumTuples() const;
  /**
   * @return query summary which stores the execution time, compiling time, plan
   * and query options.
   */
  KUZU_API QuerySummary* getQuerySummary() const;
  /**
   * @return whether there are more tuples to read.
   */
  KUZU_API bool hasNext() const;
  /**
   * @return whether there are more query results to read.
   */
  KUZU_API bool hasNextQueryResult() const;
  /**
   * @return get next query result to read (for multiple query statements).
   */
  KUZU_API QueryResult* getNextQueryResult();

  std::unique_ptr<QueryResult> nextQueryResult;

  /**
   * @return string of first query result.
   */
  KUZU_API std::string toString() const;

  /**
   * @brief Returns the arrow schema of the query result.
   * @return datatypes of the columns as an arrow schema
   *
   * It is the caller's responsibility to call the release function to release
   * the underlying data If converting to another arrow type, this this is
   * usually handled automatically.
   */
  KUZU_API std::unique_ptr<ArrowSchema> getArrowSchema() const;

  /**
   * @brief Returns the next chunk of the query result as an arrow array.
   * @param chunkSize number of tuples to return in the chunk.
   * @return An arrow array representation of the next chunkSize tuples of the
   * query result.
   *
   * The ArrowArray internally stores an arrow struct with fields for each of
   * the columns. This can be converted to a RecordBatch with arrow's
   * ImportRecordBatch function
   *
   * It is the caller's responsibility to call the release function to release
   * the underlying data If converting to another arrow type, this this is
   * usually handled automatically.
   */
  KUZU_API std::unique_ptr<ArrowArray> getNextArrowChunk(int64_t chunkSize);

 private:
  void setColumnHeader(std::vector<std::string> columnNames,
                       std::vector<common::LogicalType> columnTypes);
  void validateQuerySucceed() const;

 private:
  bool success = true;
  std::string errMsg;

  std::vector<std::string> columnNames;
  std::vector<common::LogicalType> columnDataTypes;

  std::unique_ptr<QuerySummary> querySummary;

  QueryResultIterator queryResultIterator;
};

}  // namespace main
}  // namespace gs
