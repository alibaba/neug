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

#include <optional>
#include <utility>
#include <vector>
#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/query/normalized_query_part.h"
#include "normalized_single_query.h"

namespace neug {
namespace binder {

class BoundRegularQuery final : public BoundStatement {
 public:
  explicit BoundRegularQuery(std::vector<bool> isUnionAll,
                             BoundStatementResult statementResult)
      : BoundStatement{common::StatementType::QUERY,
                       std::move(statementResult)},
        isUnionAll{std::move(isUnionAll)} {}

  void addSingleQuery(NormalizedSingleQuery singleQuery) {
    singleQueries.push_back(std::move(singleQuery));
  }
  uint64_t getNumSingleQueries() const { return singleQueries.size(); }
  NormalizedSingleQuery* getSingleQueryUnsafe(common::idx_t idx) {
    return &singleQueries[idx];
  }
  const NormalizedSingleQuery* getSingleQuery(common::idx_t idx) const {
    return &singleQueries[idx];
  }

  bool getIsUnionAll(common::idx_t idx) const { return isUnionAll[idx]; }

  void setPreQueryPart(std::vector<NormalizedQueryPart> preQueryParts) {
    this->preQueryPart = std::move(preQueryParts);
  }
  void setPostSingleQuery(NormalizedSingleQuery postSingleQuery) {
    this->postSingleQuery = std::move(postSingleQuery);
  }

  void setPreQueryExprs(binder::expression_vector preQueryExprs) {
    this->preQueryExprs = std::move(preQueryExprs);
  }

  const binder::expression_vector& getPreQueryExprs() const {
    return this->preQueryExprs;
  }

  const std::vector<NormalizedQueryPart>& getPreQueryPart() const {
    return this->preQueryPart;
  }

  std::vector<NormalizedQueryPart*> getPreQueryPartUnsafe() {
    std::vector<NormalizedQueryPart*> queries;
    for (auto& part : this->preQueryPart) {
      queries.push_back(&part);
    }
    return queries;
  }

  const NormalizedSingleQuery* getPostSingleQuery() const {
    return postSingleQuery ? &*postSingleQuery : nullptr;
  }

  NormalizedSingleQuery* getPostSingleQueryUnsafe() {
    return postSingleQuery ? &*postSingleQuery : nullptr;
  }

  void setCommonPatReuse(bool commonPatResuse) {}

 private:
  std::vector<NormalizedSingleQuery> singleQueries;
  std::vector<bool> isUnionAll;

  binder::expression_vector preQueryExprs;
  std::vector<NormalizedQueryPart> preQueryPart;
  std::optional<NormalizedSingleQuery> postSingleQuery;
};

}  // namespace binder
}  // namespace neug
