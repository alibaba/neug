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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"
#include "query_part.h"

namespace neug {
namespace parser {

class SingleQuery {
 public:
  SingleQuery() = default;
  DELETE_COPY_DEFAULT_MOVE(SingleQuery);

  inline void addQueryPart(QueryPart queryPart) {
    queryParts.push_back(std::move(queryPart));
  }
  inline uint32_t getNumQueryParts() const { return queryParts.size(); }
  inline const QueryPart* getQueryPart(uint32_t idx) const {
    return &queryParts[idx];
  }

  inline uint32_t getNumUpdatingClauses() const {
    return updatingClauses.size();
  }
  inline UpdatingClause* getUpdatingClause(uint32_t idx) const {
    return updatingClauses[idx].get();
  }
  inline void addUpdatingClause(
      std::unique_ptr<UpdatingClause> updatingClause) {
    updatingClauses.push_back(std::move(updatingClause));
  }

  inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
  inline ReadingClause* getReadingClause(uint32_t idx) const {
    return readingClauses[idx].get();
  }
  inline void addReadingClause(std::unique_ptr<ReadingClause> readingClause) {
    readingClauses.push_back(std::move(readingClause));
  }

  inline void setReturnClause(ReturnClause clause) {
    returnClause = std::move(clause);
  }
  inline bool hasReturnClause() const { return returnClause.has_value(); }
  inline const ReturnClause* getReturnClause() const {
    NEUG_ASSERT(returnClause.has_value());
    return &returnClause.value();
  }

 private:
  std::vector<QueryPart> queryParts;
  std::vector<std::unique_ptr<ReadingClause>> readingClauses;
  std::vector<std::unique_ptr<UpdatingClause>> updatingClauses;
  std::optional<ReturnClause> returnClause;
};

}  // namespace parser
}  // namespace neug
