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

#include <memory>
#include <optional>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/query/query_part.h"
#include "neug/compiler/parser/statement.h"
#include "single_query.h"

namespace neug {
namespace parser {

class RegularQuery : public Statement {
  static constexpr common::StatementType type_ = common::StatementType::QUERY;

 public:
  explicit RegularQuery(SingleQuery singleQuery) : Statement{type_} {
    singleQueries.push_back(std::move(singleQuery));
  }

  void addSingleQuery(SingleQuery singleQuery, bool isUnionAllQuery) {
    singleQueries.push_back(std::move(singleQuery));
    isUnionAll.push_back(isUnionAllQuery);
  }

  common::idx_t getNumSingleQueries() const { return singleQueries.size(); }

  const SingleQuery* getSingleQuery(common::idx_t singleQueryIdx) const {
    return &singleQueries[singleQueryIdx];
  }

  std::vector<bool> getIsUnionAll() const { return isUnionAll; }

  void setPreQueryPart(std::vector<QueryPart> preQueryPart) {
    this->preQueryPart = std::move(preQueryPart);
  }
  void setPostSingleQuery(SingleQuery postSingleQuery) {
    this->postSingleQuery = std::move(postSingleQuery);
  }

  void setPreQueryExpressions(
      std::vector<std::unique_ptr<ParsedExpression>> preQueryExprs) {
    this->preQueryExprs = std::move(preQueryExprs);
  }

  const std::vector<QueryPart>& getPreQueryPart() const {
    return this->preQueryPart;
  }

  const SingleQuery* getPostSingleQuery() const {
    return postSingleQuery ? &*postSingleQuery : nullptr;
  }

  const std::vector<std::unique_ptr<ParsedExpression>>& getPreQueryExprs()
      const {
    return preQueryExprs;
  }

 private:
  std::vector<SingleQuery> singleQueries;
  std::vector<bool> isUnionAll;

  std::vector<std::unique_ptr<ParsedExpression>> preQueryExprs;
  std::vector<QueryPart> preQueryPart;
  std::optional<SingleQuery> postSingleQuery;
};

}  // namespace parser
}  // namespace neug
