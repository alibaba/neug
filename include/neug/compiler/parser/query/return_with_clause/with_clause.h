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

#include "return_clause.h"

namespace neug {
namespace parser {

class WithClause : public ReturnClause {
 public:
  explicit WithClause(ProjectionBody projectionBody)
      : ReturnClause{std::move(projectionBody)} {}
  DELETE_COPY_DEFAULT_MOVE(WithClause);

  inline void setWhereExpression(std::unique_ptr<ParsedExpression> expression) {
    whereExpression = std::move(expression);
  }

  inline bool hasWhereExpression() const { return whereExpression != nullptr; }

  inline ParsedExpression* getWhereExpression() const {
    return whereExpression.get();
  }

 private:
  std::unique_ptr<ParsedExpression> whereExpression;
};

}  // namespace parser
}  // namespace neug
