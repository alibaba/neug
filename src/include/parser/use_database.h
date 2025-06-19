#pragma once

#include "src/include/parser/database_statement.h"

namespace gs {
namespace parser {

class UseDatabase final : public DatabaseStatement {
 public:
  explicit UseDatabase(std::string dbName)
      : DatabaseStatement{common::StatementType::USE_DATABASE,
                          std::move(dbName)} {}
};

}  // namespace parser
}  // namespace gs
