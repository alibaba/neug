#include "main/query_result.h"

#include <memory>

#include "common/arrow/arrow_converter.h"
#include "common/exception/runtime.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

QueryResult::QueryResult()
    : nextQueryResult{nullptr}, queryResultIterator{this} {}

QueryResult::QueryResult(const PreparedSummary& preparedSummary)
    : nextQueryResult{nullptr}, queryResultIterator{this} {
  querySummary = std::make_unique<QuerySummary>();
  querySummary->setPreparedSummary(preparedSummary);
}

QueryResult::~QueryResult() = default;

bool QueryResult::isSuccess() const { return success; }

std::string QueryResult::getErrorMessage() const { return errMsg; }

size_t QueryResult::getNumColumns() const { return columnDataTypes.size(); }

std::vector<std::string> QueryResult::getColumnNames() const {
  return columnNames;
}

std::vector<LogicalType> QueryResult::getColumnDataTypes() const {
  return LogicalType::copy(columnDataTypes);
}

uint64_t QueryResult::getNumTuples() const { return 0; }

QuerySummary* QueryResult::getQuerySummary() const {
  return querySummary.get();
}

void QueryResult::setColumnHeader(std::vector<std::string> columnNames_,
                                  std::vector<LogicalType> columnTypes_) {
  columnNames = std::move(columnNames_);
  columnDataTypes = std::move(columnTypes_);
}

bool QueryResult::hasNext() const {
  validateQuerySucceed();
  return false;
}

bool QueryResult::hasNextQueryResult() const {
  return queryResultIterator.hasNextQueryResult();
}

QueryResult* QueryResult::getNextQueryResult() {
  if (hasNextQueryResult()) {
    ++queryResultIterator;
    return queryResultIterator.getCurrentResult();
  }
  return nullptr;
}

std::string QueryResult::toString() const {
  std::string result;
  if (isSuccess()) {
    for (auto i = 0u; i < columnNames.size(); ++i) {
      if (i != 0) {
        result += "|";
      }
      result += columnNames[i];
    }
    result += "\n";
  } else {
    result = errMsg;
  }
  return result;
}

void QueryResult::validateQuerySucceed() const {
  if (!success) {
    throw Exception(errMsg);
  }
}

std::unique_ptr<ArrowSchema> QueryResult::getArrowSchema() const {
  return ArrowConverter::toArrowSchema(getColumnDataTypes(), getColumnNames());
}

std::unique_ptr<ArrowArray> QueryResult::getNextArrowChunk(int64_t chunkSize) {
  auto data = std::make_unique<ArrowArray>();
  ArrowConverter::toArrowArray(*this, data.get(), chunkSize);
  return data;
}

}  // namespace main
}  // namespace kuzu
