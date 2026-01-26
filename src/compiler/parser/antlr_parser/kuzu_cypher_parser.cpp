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

#include "neug/compiler/parser/antlr_parser/kuzu_cypher_parser.h"

#include <string>

namespace neug {
namespace parser {

void KuzuCypherParser::notifyQueryNotConcludeWithReturn(
    antlr4::Token* startToken) {
  auto errorMsg = "Query must conclude with RETURN clause";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

void KuzuCypherParser::notifyNodePatternWithoutParentheses(
    std::string nodeName, antlr4::Token* startToken) {
  auto errorMsg =
      "Parentheses are required to identify nodes in patterns, i.e. (" +
      nodeName + ")";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

void KuzuCypherParser::notifyInvalidNotEqualOperator(
    antlr4::Token* startToken) {
  auto errorMsg =
      "Unknown operation '!=' (you probably meant to use '<>', which is the "
      "operator "
      "for inequality testing.)";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

void KuzuCypherParser::notifyEmptyToken(antlr4::Token* startToken) {
  auto errorMsg =
      "'' is not a valid token name. Token names cannot be empty or contain "
      "any null-bytes";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

void KuzuCypherParser::notifyReturnNotAtEnd(antlr4::Token* startToken) {
  auto errorMsg = "RETURN can only be used at the end of the query";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

void KuzuCypherParser::notifyNonBinaryComparison(antlr4::Token* startToken) {
  auto errorMsg = "Non-binary comparison (e.g. a=b=c) is not supported";
  notifyErrorListeners(startToken, errorMsg, nullptr);
}

}  // namespace parser
}  // namespace neug
