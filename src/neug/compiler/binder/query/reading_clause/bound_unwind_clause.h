#pragma once

#include "bound_reading_clause.h"
#include "neug/compiler/binder/expression/expression.h"

namespace gs {
namespace binder {

class BoundUnwindClause final : public BoundReadingClause {
 public:
  BoundUnwindClause(std::shared_ptr<Expression> inExpr,
                    std::shared_ptr<Expression> outExpr,
                    std::shared_ptr<Expression> idExpr)
      : BoundReadingClause{common::ClauseType::UNWIND},
        inExpr{std::move(inExpr)},
        outExpr{std::move(outExpr)},
        idExpr{std::move(idExpr)} {}

  std::shared_ptr<Expression> getInExpr() const { return inExpr; }
  std::shared_ptr<Expression> getOutExpr() const { return outExpr; }
  std::shared_ptr<Expression> getIDExpr() const { return idExpr; }

 private:
  std::shared_ptr<Expression> inExpr;
  std::shared_ptr<Expression> outExpr;
  std::shared_ptr<Expression> idExpr;
};

}  // namespace binder
}  // namespace gs
