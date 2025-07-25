#pragma once

#include <string>

#include "neug/compiler/parser/statement.h"

namespace gs {
namespace parser {

class DatabaseStatement : public Statement {
 public:
  explicit DatabaseStatement(common::StatementType type, std::string dbName)
      : Statement{type}, dbName{std::move(dbName)} {}

  std::string getDBName() const { return dbName; }

 private:
  std::string dbName;
};

}  // namespace parser
}  // namespace gs
