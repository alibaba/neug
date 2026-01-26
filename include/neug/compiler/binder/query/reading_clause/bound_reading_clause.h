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

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/enums/clause_type.h"

namespace neug {
namespace binder {

class NEUG_API BoundReadingClause {
 public:
  explicit BoundReadingClause(common::ClauseType clauseType)
      : clauseType{clauseType} {}
  DELETE_COPY_DEFAULT_MOVE(BoundReadingClause);
  virtual ~BoundReadingClause() = default;

  common::ClauseType getClauseType() const { return clauseType; }

  void setPredicate(std::shared_ptr<Expression> predicate_) {
    predicate = std::move(predicate_);
  }
  bool hasPredicate() const { return predicate != nullptr; }
  std::shared_ptr<Expression> getPredicate() const { return predicate; }
  expression_vector getConjunctivePredicates() const {
    return hasPredicate() ? predicate->splitOnAND() : expression_vector{};
  }

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  TARGET* ptrCast() const {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

 private:
  common::ClauseType clauseType;
  // Predicate in WHERE clause
  std::shared_ptr<Expression> predicate;
};

}  // namespace binder
}  // namespace neug
