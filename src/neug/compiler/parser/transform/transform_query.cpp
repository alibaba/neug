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

#include "neug/compiler/parser/query/regular_query.h"
#include "neug/compiler/parser/transformer.h"

namespace gs {
namespace parser {

std::unique_ptr<Statement> Transformer::transformQuery(
    CypherParser::OC_QueryContext& ctx) {
  return transformRegularQuery(*ctx.oC_RegularQuery());
}

std::unique_ptr<Statement> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
  auto regularQuery = std::make_unique<RegularQuery>(
      transformSingleQuery(*ctx.oC_SingleQuery()));
  for (auto unionClause : ctx.oC_Union()) {
    regularQuery->addSingleQuery(
        transformSingleQuery(*unionClause->oC_SingleQuery()),
        unionClause->ALL());
  }
  return regularQuery;
}

SingleQuery Transformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext& ctx) {
  auto singleQuery = ctx.oC_MultiPartQuery()
                         ? transformSinglePartQuery(
                               *ctx.oC_MultiPartQuery()->oC_SinglePartQuery())
                         : transformSinglePartQuery(*ctx.oC_SinglePartQuery());
  if (ctx.oC_MultiPartQuery()) {
    for (auto queryPart : ctx.oC_MultiPartQuery()->kU_QueryPart()) {
      singleQuery.addQueryPart(transformQueryPart(*queryPart));
    }
  }
  return singleQuery;
}

SingleQuery Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext& ctx) {
  auto singleQuery = SingleQuery();
  for (auto& readingClause : ctx.oC_ReadingClause()) {
    singleQuery.addReadingClause(transformReadingClause(*readingClause));
  }
  for (auto& updatingClause : ctx.oC_UpdatingClause()) {
    singleQuery.addUpdatingClause(transformUpdatingClause(*updatingClause));
  }
  if (ctx.oC_Return()) {
    singleQuery.setReturnClause(transformReturn(*ctx.oC_Return()));
  }
  return singleQuery;
}

QueryPart Transformer::transformQueryPart(
    CypherParser::KU_QueryPartContext& ctx) {
  auto queryPart = QueryPart(transformWith(*ctx.oC_With()));
  for (auto& readingClause : ctx.oC_ReadingClause()) {
    queryPart.addReadingClause(transformReadingClause(*readingClause));
  }
  for (auto& updatingClause : ctx.oC_UpdatingClause()) {
    queryPart.addUpdatingClause(transformUpdatingClause(*updatingClause));
  }
  return queryPart;
}

}  // namespace parser
}  // namespace gs
