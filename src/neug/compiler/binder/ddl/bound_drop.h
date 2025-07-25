#pragma once

#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/parser/ddl/drop_info.h"

namespace gs {
namespace binder {

class BoundDrop final : public BoundStatement {
  static constexpr common::StatementType type_ = common::StatementType::DROP;

 public:
  explicit BoundDrop(parser::DropInfo dropInfo)
      : BoundStatement{type_,
                       BoundStatementResult::createSingleStringColumnResult()},
        dropInfo{std::move(dropInfo)} {}

  const parser::DropInfo& getDropInfo() const { return dropInfo; };

 private:
  parser::DropInfo dropInfo;
};

}  // namespace binder
}  // namespace gs
