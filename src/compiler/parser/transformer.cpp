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

#include "neug/compiler/parser/transformer.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/parser/explain_statement.h"
#include "neug/compiler/parser/query/regular_query.h"  // IWYU pragma: keep (fixes a forward declaration error)

using namespace neug::common;

namespace neug {
namespace parser {

std::vector<std::shared_ptr<Statement>> Transformer::transform() {
  std::vector<std::shared_ptr<Statement>> statements;
  for (auto& oc_Statement : root.oC_Cypher()) {
    auto statement = transformStatement(*oc_Statement->oC_Statement());
    if (oc_Statement->oC_AnyCypherOption()) {
      auto cypherOption = oc_Statement->oC_AnyCypherOption();
      auto explainType = ExplainType::PROFILE;
      if (cypherOption->oC_Explain()) {
        explainType = cypherOption->oC_Explain()->LOGICAL()
                          ? ExplainType::LOGICAL_PLAN
                          : ExplainType::PHYSICAL_PLAN;
      }
      statements.push_back(std::make_unique<ExplainStatement>(
          std::move(statement), explainType));
      continue;
    }
    statements.push_back(std::move(statement));
  }
  return statements;
}

std::unique_ptr<Statement> Transformer::transformStatement(
    CypherParser::OC_StatementContext& ctx) {
  if (ctx.oC_Query()) {
    return transformQuery(*ctx.oC_Query());
  } else if (ctx.nEUG_CreateNodeTable()) {
    return transformCreateNodeTable(*ctx.nEUG_CreateNodeTable());
  } else if (ctx.nEUG_CreateRelTable()) {
    return transformCreateRelTable(*ctx.nEUG_CreateRelTable());
  } else if (ctx.nEUG_CreateSequence()) {
    return transformCreateSequence(*ctx.nEUG_CreateSequence());
  } else if (ctx.nEUG_CreateType()) {
    return transformCreateType(*ctx.nEUG_CreateType());
  } else if (ctx.nEUG_Drop()) {
    return transformDrop(*ctx.nEUG_Drop());
  } else if (ctx.nEUG_AlterTable()) {
    return transformAlterTable(*ctx.nEUG_AlterTable());
  } else if (ctx.nEUG_CopyFromByColumn()) {
    return transformCopyFromByColumn(*ctx.nEUG_CopyFromByColumn());
  } else if (ctx.nEUG_CopyFrom()) {
    return transformCopyFrom(*ctx.nEUG_CopyFrom());
  } else if (ctx.nEUG_CopyTO()) {
    return transformCopyTo(*ctx.nEUG_CopyTO());
  } else if (ctx.nEUG_StandaloneCall()) {
    return transformStandaloneCall(*ctx.nEUG_StandaloneCall());
  } else if (ctx.nEUG_CreateMacro()) {
    return transformCreateMacro(*ctx.nEUG_CreateMacro());
  } else if (ctx.nEUG_CommentOn()) {
    return transformCommentOn(*ctx.nEUG_CommentOn());
  } else if (ctx.nEUG_Transaction()) {
    return transformTransaction(*ctx.nEUG_Transaction());
  } else if (ctx.nEUG_Extension()) {
    return transformExtension(*ctx.nEUG_Extension());
  } else if (ctx.nEUG_ExportDatabase()) {
    return transformExportDatabase(*ctx.nEUG_ExportDatabase());
  } else if (ctx.nEUG_ImportDatabase()) {
    return transformImportDatabase(*ctx.nEUG_ImportDatabase());
  } else if (ctx.nEUG_AttachDatabase()) {
    return transformAttachDatabase(*ctx.nEUG_AttachDatabase());
  } else if (ctx.nEUG_DetachDatabase()) {
    return transformDetachDatabase(*ctx.nEUG_DetachDatabase());
  } else if (ctx.nEUG_UseDatabase()) {
    return transformUseDatabase(*ctx.nEUG_UseDatabase());
  } else {
    NEUG_UNREACHABLE;
  }
}

std::unique_ptr<ParsedExpression> Transformer::transformWhere(
    CypherParser::OC_WhereContext& ctx) {
  return transformExpression(*ctx.oC_Expression());
}

std::string Transformer::transformVariable(
    CypherParser::OC_VariableContext& ctx) {
  return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSchemaName(
    CypherParser::OC_SchemaNameContext& ctx) {
  return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSymbolicName(
    CypherParser::OC_SymbolicNameContext& ctx) {
  if (ctx.EscapedSymbolicName()) {
    std::string escapedSymbolName = ctx.EscapedSymbolicName()->getText();
    // escapedSymbolName symbol will be of form "`Some.Value`". Therefore, we
    // need to sanitize it such that we don't store the symbol with escape
    // character.
    return escapedSymbolName.substr(1, escapedSymbolName.size() - 2);
  } else {
    NEUG_ASSERT(ctx.HexLetter() || ctx.UnescapedSymbolicName() ||
                ctx.nEUG_NonReservedKeywords());
    return ctx.getText();
  }
}

std::string Transformer::transformStringLiteral(
    antlr4::tree::TerminalNode& stringLiteral) {
  auto str = stringLiteral.getText();
  return StringUtils::removeEscapedCharacters(str);
}

}  // namespace parser
}  // namespace neug
