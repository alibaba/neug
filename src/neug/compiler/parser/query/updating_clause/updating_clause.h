#pragma once

#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/clause_type.h"

namespace gs {
namespace parser {

class UpdatingClause {
 public:
  explicit UpdatingClause(common::ClauseType clauseType)
      : clauseType{clauseType} {};
  virtual ~UpdatingClause() = default;

  common::ClauseType getClauseType() const { return clauseType; }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::ku_dynamic_cast<const TARGET&>(*this);
  }

 private:
  common::ClauseType clauseType;
};

}  // namespace parser
}  // namespace gs
