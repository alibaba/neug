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

#include "neug/compiler/parser/query/reading_clause/reading_clause.h"
#include "neug/compiler/parser/query/return_with_clause/with_clause.h"
#include "neug/compiler/parser/query/updating_clause/updating_clause.h"

namespace neug {
namespace parser {

class QueryPart {
 public:
  explicit QueryPart(WithClause withClause)
      : withClause{std::move(withClause)} {}

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

  inline const WithClause* getWithClause() const { return &withClause; }

 private:
  std::vector<std::unique_ptr<ReadingClause>> readingClauses;
  std::vector<std::unique_ptr<UpdatingClause>> updatingClauses;
  WithClause withClause;
};

}  // namespace parser
}  // namespace neug
