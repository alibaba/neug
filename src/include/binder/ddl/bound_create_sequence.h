#pragma once

#include "bound_create_sequence_info.h"
#include "src/include/binder/bound_statement.h"
namespace gs {
namespace binder {

class BoundCreateSequence final : public BoundStatement {
 public:
  explicit BoundCreateSequence(BoundCreateSequenceInfo info)
      : BoundStatement{common::StatementType::CREATE_SEQUENCE,
                       BoundStatementResult::createSingleStringColumnResult()},
        info{std::move(info)} {}

  const BoundCreateSequenceInfo* getInfo() const { return &info; }

 private:
  BoundCreateSequenceInfo info;
};

}  // namespace binder
}  // namespace gs
