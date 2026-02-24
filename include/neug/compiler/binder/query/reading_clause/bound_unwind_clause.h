/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "bound_reading_clause.h"
#include "neug/compiler/binder/expression/expression.h"

namespace neug {
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
}  // namespace neug
