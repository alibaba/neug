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

#include "bound_join_hint.h"
#include "bound_reading_clause.h"
#include "neug/compiler/binder/query/query_graph.h"

namespace neug {
namespace binder {

class BoundMatchClause final : public BoundReadingClause {
  static constexpr common::ClauseType clauseType_ = common::ClauseType::MATCH;

 public:
  BoundMatchClause(QueryGraphCollection collection,
                   common::MatchClauseType matchClauseType)
      : BoundReadingClause{clauseType_},
        collection{std::move(collection)},
        matchClauseType{matchClauseType} {}

  QueryGraphCollection* getQueryGraphCollectionUnsafe() { return &collection; }
  const QueryGraphCollection* getQueryGraphCollection() const {
    return &collection;
  }

  common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

  void setHint(std::shared_ptr<BoundJoinHintNode> root) {
    hintRoot = std::move(root);
  }
  bool hasHint() const { return hintRoot != nullptr; }
  std::shared_ptr<BoundJoinHintNode> getHint() const { return hintRoot; }

 private:
  QueryGraphCollection collection;
  common::MatchClauseType matchClauseType;
  std::shared_ptr<BoundJoinHintNode> hintRoot;
};

}  // namespace binder
}  // namespace neug
