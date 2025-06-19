#pragma once

#include "bound_create_table_info.h"
#include "src/include/binder/bound_statement.h"

namespace gs {
namespace binder {

class BoundCreateTable final : public BoundStatement {
 public:
  explicit BoundCreateTable(BoundCreateTableInfo info)
      : BoundStatement{common::StatementType::CREATE_TABLE,
                       BoundStatementResult::createSingleStringColumnResult()},
        info{std::move(info)} {}

  inline const BoundCreateTableInfo* getInfo() const { return &info; }

 private:
  BoundCreateTableInfo info;
};

}  // namespace binder
}  // namespace gs
