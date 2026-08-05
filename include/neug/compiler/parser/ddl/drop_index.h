#pragma once

#include <string>

#include "neug/compiler/parser/statement.h"

namespace neug::parser {

class DropIndex final : public Statement {
 public:
  DropIndex(std::string indexName, bool ifExists)
      : Statement{common::StatementType::DROP_INDEX},
        indexName{std::move(indexName)},
        ifExists{ifExists} {}

  const std::string& getIndexName() const { return indexName; }
  bool getIfExists() const { return ifExists; }

 private:
  std::string indexName;
  bool ifExists;
};

}  // namespace neug::parser
