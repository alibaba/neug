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

#include "neug/compiler/parser/parser.h"

#include <fstream>
#include <iostream>

// ANTLR4 generates code with unused parameters.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_lexer.h"
#pragma GCC diagnostic pop

#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/parser/antlr_parser/kuzu_cypher_parser.h"
#include "neug/compiler/parser/antlr_parser/parser_error_listener.h"
#include "neug/compiler/parser/antlr_parser/parser_error_strategy.h"
#include "neug/compiler/parser/transformer.h"
#include "neug/utils/exception/exception.h"

using namespace antlr4;

namespace neug {
namespace parser {

std::vector<std::shared_ptr<Statement>> Parser::parseQuery(
    std::string_view query) {
  auto queryStr = std::string(query);
  queryStr = common::StringUtils::ltrim(queryStr);
  queryStr = common::StringUtils::ltrimNewlines(queryStr);
  // LCOV_EXCL_START
  // We should have enforced this in connection, but I also realize empty query
  // will cause antlr to hang. So enforce a duplicate check here.
  if (queryStr.empty()) {
    THROW_PARSER_EXCEPTION(
        "Cannot parse empty query. This should be handled in connection.");
  }
  // LCOV_EXCL_STOP

  auto inputStream = ANTLRInputStream(queryStr);
  auto parserErrorListener = ParserErrorListener();

  auto cypherLexer = CypherLexer(&inputStream);
  cypherLexer.removeErrorListeners();
  cypherLexer.addErrorListener(&parserErrorListener);
  auto tokens = CommonTokenStream(&cypherLexer);
  tokens.fill();

  auto kuzuCypherParser = KuzuCypherParser(&tokens);
  kuzuCypherParser.removeErrorListeners();
  kuzuCypherParser.addErrorListener(&parserErrorListener);
  kuzuCypherParser.setErrorHandler(std::make_shared<ParserErrorStrategy>());

  Transformer transformer(*kuzuCypherParser.neug_Statements());
  return transformer.transform();
}

}  // namespace parser
}  // namespace neug
