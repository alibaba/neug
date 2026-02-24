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

#include "neug/compiler/binder/bound_statement_result.h"
#include "normalized_query_part.h"

namespace neug {
namespace binder {

class NormalizedSingleQuery {
 public:
  NormalizedSingleQuery() = default;
  DELETE_COPY_DEFAULT_MOVE(NormalizedSingleQuery);

  void appendQueryPart(NormalizedQueryPart queryPart) {
    queryParts.push_back(std::move(queryPart));
  }

  void insertQueryPart(common::idx_t idx, NormalizedQueryPart queryPart) {
    queryParts.insert(queryParts.begin() + idx, std::move(queryPart));
  }

  common::idx_t getNumQueryParts() const { return queryParts.size(); }
  NormalizedQueryPart* getQueryPartUnsafe(common::idx_t idx) {
    return &queryParts[idx];
  }
  const NormalizedQueryPart* getQueryPart(common::idx_t idx) const {
    return &queryParts[idx];
  }

  void setStatementResult(BoundStatementResult result) {
    statementResult = std::move(result);
  }
  const BoundStatementResult* getStatementResult() const {
    return &statementResult;
  }

 private:
  std::vector<NormalizedQueryPart> queryParts;
  BoundStatementResult statementResult;
};

}  // namespace binder
}  // namespace neug
