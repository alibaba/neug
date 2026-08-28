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

#include <memory>
#include <vector>
// Windows headers define DELETE and OPTIONAL as macros, which conflict with
// ANTLR-generated token names and method names in cypher_parser.h.
#ifdef DELETE
#undef DELETE
#endif
#ifdef OPTIONAL
#undef OPTIONAL
#endif
#include "antlr4_cypher/include/cypher_parser.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/query/query_part.h"
#include "neug/compiler/parser/query/regular_query.h"
#include "neug/compiler/parser/transformer.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace parser {

static void validateUnionAll(CypherParser::OC_UnionContext& unionClause) {
  if (!unionClause.ALL()) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "UNION without ALL is not supported. Use UNION ALL instead.");
  }
}

std::unique_ptr<Statement> Transformer::transformQuery(
    CypherParser::OC_QueryContext& ctx) {
  auto query = transformRegularQuery(*ctx.oC_RegularQuery());
  if (ctx.NAMESPACE()) {
    query->cast<RegularQuery>().setNamespaceName(
        transformSchemaName(*ctx.oC_SchemaName()));
  }
  return query;
}

std::vector<std::unique_ptr<ParsedExpression>> Transformer::transformCallScope(
    CypherParser::OC_CallUnionScopeContext& scope) {
  std::vector<std::unique_ptr<ParsedExpression>> preQueryExprs;
  for (auto expr : scope.oC_Expression()) {
    preQueryExprs.emplace_back(std::move(transformExpression(*expr)));
  }
  return std::move(preQueryExprs);
}

std::unique_ptr<Statement> Transformer::transformCallUnionQuery(
    CypherParser::OC_CallUnionQueryContext& ctx) {
  NEUG_ASSERT(ctx.oC_CallUnion());
  auto oC_CallUnion = ctx.oC_CallUnion();
  auto regularQuery = std::make_unique<RegularQuery>(
      transformSingleQuery(*oC_CallUnion->oC_SingleQuery()));
  for (auto unionClause : oC_CallUnion->oC_Union()) {
    validateUnionAll(*unionClause);
    regularQuery->addSingleQuery(
        transformSingleQuery(*unionClause->oC_SingleQuery()),
        unionClause->ALL());
  }
  std::vector<QueryPart> preQueryParts;
  for (auto part : ctx.nEUG_QueryPart()) {
    preQueryParts.emplace_back(std::move(transformQueryPart(*part)));
  }
  if (!preQueryParts.empty()) {
    regularQuery->setPreQueryPart(std::move(preQueryParts));
  }
  if (oC_CallUnion->oC_CallUnionScope()) {
    std::vector<std::unique_ptr<ParsedExpression>> preQueryExprs =
        std::move(transformCallScope(*oC_CallUnion->oC_CallUnionScope()));
    if (!preQueryExprs.empty()) {
      regularQuery->setPreQueryExpressions(std::move(preQueryExprs));
    }
  }
  if (ctx.oC_SingleQuery()) {
    regularQuery->setPostSingleQuery(
        transformSingleQuery(*ctx.oC_SingleQuery()));
  }
  return regularQuery;
}

std::unique_ptr<Statement> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
  if (ctx.oC_CallUnionQuery()) {
    return transformCallUnionQuery(*ctx.oC_CallUnionQuery());
  }
  auto regularQuery = std::make_unique<RegularQuery>(
      transformSingleQuery(*ctx.oC_SingleQuery()));
  for (auto unionClause : ctx.oC_Union()) {
    validateUnionAll(*unionClause);
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
    for (auto queryPart : ctx.oC_MultiPartQuery()->nEUG_QueryPart()) {
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
    CypherParser::NEUG_QueryPartContext& ctx) {
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
}  // namespace neug
