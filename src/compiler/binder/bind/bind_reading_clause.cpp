#include "src/include/binder/binder.h"
#include "src/include/parser/query/reading_clause/reading_clause.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindReadingClause(
    const ReadingClause& readingClause) {
  switch (readingClause.getClauseType()) {
  case ClauseType::MATCH: {
    return bindMatchClause(readingClause);
  }
  case ClauseType::UNWIND: {
    return bindUnwindClause(readingClause);
  }
  case ClauseType::IN_QUERY_CALL: {
    return bindInQueryCall(readingClause);
  }
  case ClauseType::LOAD_FROM: {
    return bindLoadFrom(readingClause);
  }
  default:
    KU_UNREACHABLE;
  }
}

}  // namespace binder
}  // namespace gs
