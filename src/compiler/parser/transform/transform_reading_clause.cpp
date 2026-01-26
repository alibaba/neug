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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/parser/query/reading_clause/in_query_call_clause.h"
#include "neug/compiler/parser/query/reading_clause/load_from.h"
#include "neug/compiler/parser/query/reading_clause/match_clause.h"
#include "neug/compiler/parser/query/reading_clause/unwind_clause.h"
#include "neug/compiler/parser/transformer.h"

using namespace neug::common;

namespace neug {
namespace parser {

std::unique_ptr<ReadingClause> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
  if (ctx.oC_Match()) {
    return transformMatch(*ctx.oC_Match());
  } else if (ctx.oC_Unwind()) {
    return transformUnwind(*ctx.oC_Unwind());
  } else if (ctx.nEUG_InQueryCall()) {
    return transformInQueryCall(*ctx.nEUG_InQueryCall());
  } else if (ctx.nEUG_LoadFrom()) {
    return transformLoadFrom(*ctx.nEUG_LoadFrom());
  }
  NEUG_UNREACHABLE;
}

std::unique_ptr<ReadingClause> Transformer::transformMatch(
    CypherParser::OC_MatchContext& ctx) {
  auto matchClauseType =
      ctx.OPTIONAL() ? MatchClauseType::OPTIONAL_MATCH : MatchClauseType::MATCH;
  auto matchClause = std::make_unique<MatchClause>(
      transformPattern(*ctx.oC_Pattern()), matchClauseType);
  if (ctx.oC_Where()) {
    matchClause->setWherePredicate(transformWhere(*ctx.oC_Where()));
  }
  if (ctx.nEUG_Hint()) {
    matchClause->setHint(transformJoinHint(*ctx.nEUG_Hint()->nEUG_JoinNode()));
  }
  return matchClause;
}

std::shared_ptr<JoinHintNode> Transformer::transformJoinHint(
    CypherParser::NEUG_JoinNodeContext& ctx) {
  if (!ctx.MULTI_JOIN().empty()) {
    auto joinNode = std::make_shared<JoinHintNode>();
    joinNode->addChild(transformJoinHint(*ctx.nEUG_JoinNode(0)));
    for (auto& schemaNameCtx : ctx.oC_SchemaName()) {
      joinNode->addChild(
          std::make_shared<JoinHintNode>(transformSchemaName(*schemaNameCtx)));
    }
    return joinNode;
  }
  if (!ctx.oC_SchemaName().empty()) {
    return std::make_shared<JoinHintNode>(
        transformSchemaName(*ctx.oC_SchemaName(0)));
  }
  if (ctx.nEUG_JoinNode().size() == 1) {
    return transformJoinHint(*ctx.nEUG_JoinNode(0));
  }
  NEUG_ASSERT(ctx.nEUG_JoinNode().size() == 2);
  auto joinNode = std::make_shared<JoinHintNode>();
  joinNode->addChild(transformJoinHint(*ctx.nEUG_JoinNode(0)));
  joinNode->addChild(transformJoinHint(*ctx.nEUG_JoinNode(1)));
  return joinNode;
}

std::unique_ptr<ReadingClause> Transformer::transformUnwind(
    CypherParser::OC_UnwindContext& ctx) {
  auto expression = transformExpression(*ctx.oC_Expression());
  auto transformedVariable = transformVariable(*ctx.oC_Variable());
  return std::make_unique<UnwindClause>(std::move(expression),
                                        std::move(transformedVariable));
}

std::vector<YieldVariable> Transformer::transformYieldVariables(
    CypherParser::OC_YieldItemsContext& ctx) {
  std::vector<YieldVariable> yieldVariables;
  std::string name;
  for (auto& yieldItem : ctx.oC_YieldItem()) {
    std::string alias;
    if (yieldItem->AS()) {
      alias = transformVariable(*yieldItem->oC_Variable(1));
    }
    name = transformVariable(*yieldItem->oC_Variable(0));
    yieldVariables.emplace_back(name, alias);
  }
  return yieldVariables;
}

std::unique_ptr<ReadingClause> Transformer::transformInQueryCall(
    CypherParser::NEUG_InQueryCallContext& ctx) {
  auto functionExpression =
      Transformer::transformFunctionInvocation(*ctx.oC_FunctionInvocation());
  std::vector<YieldVariable> yieldVariables;
  if (ctx.oC_YieldItems()) {
    yieldVariables = transformYieldVariables(*ctx.oC_YieldItems());
  }
  auto inQueryCall = std::make_unique<InQueryCallClause>(
      std::move(functionExpression), std::move(yieldVariables));
  if (ctx.oC_Where()) {
    inQueryCall->setWherePredicate(transformWhere(*ctx.oC_Where()));
  }
  return inQueryCall;
}

std::unique_ptr<ReadingClause> Transformer::transformLoadFrom(
    CypherParser::NEUG_LoadFromContext& ctx) {
  auto source = transformScanSource(*ctx.nEUG_ScanSource());
  auto loadFrom = std::make_unique<LoadFrom>(std::move(source));
  if (ctx.nEUG_ColumnDefinitions()) {
    loadFrom->setPropertyDefinitions(
        transformColumnDefinitions(*ctx.nEUG_ColumnDefinitions()));
  }
  if (ctx.nEUG_Options()) {
    loadFrom->setParingOptions(transformOptions(*ctx.nEUG_Options()));
  }
  if (ctx.oC_Where()) {
    loadFrom->setWherePredicate(transformWhere(*ctx.oC_Where()));
  }
  return loadFrom;
}

}  // namespace parser
}  // namespace neug
