#pragma once

#include "bound_alter_info.h"
#include "src/include/binder/bound_statement.h"

namespace gs {
namespace binder {

class BoundAlter final : public BoundStatement {
 public:
  explicit BoundAlter(BoundAlterInfo info)
      : BoundStatement{common::StatementType::ALTER,
                       BoundStatementResult::createSingleStringColumnResult()},
        info{std::move(info)} {}

  inline const BoundAlterInfo* getInfo() const { return &info; }

 private:
  BoundAlterInfo info;
};

}  // namespace binder
}  // namespace gs
