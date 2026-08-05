#pragma once

#include <string>

#include "neug/compiler/binder/bound_statement.h"

namespace neug::binder {

class BoundDropIndex final : public BoundStatement {
 public:
  BoundDropIndex(std::string indexName, bool ifExists)
      : BoundStatement{common::StatementType::DROP_INDEX,
                       BoundStatementResult::createSingleStringColumnResult()},
        indexName{std::move(indexName)},
        ifExists{ifExists} {}

  const std::string& getIndexName() const { return indexName; }
  bool getIfExists() const { return ifExists; }

 private:
  std::string indexName;
  bool ifExists;
};

}  // namespace neug::binder
