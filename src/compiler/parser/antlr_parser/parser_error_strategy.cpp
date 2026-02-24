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

#include "neug/compiler/parser/antlr_parser/parser_error_strategy.h"

namespace neug {
namespace parser {

void ParserErrorStrategy::reportNoViableAlternative(
    antlr4::Parser* recognizer, const antlr4::NoViableAltException& e) {
  auto tokens = recognizer->getTokenStream();
  auto errorMsg = tokens ? antlr4::Token::EOF == e.getStartToken()->getType()
                               ? "Unexpected end of input"
                               : "Invalid input <" +
                                     tokens->getText(e.getStartToken(),
                                                     e.getOffendingToken()) +
                                     ">"
                         : "Unknown input";
  auto expectedRuleName =
      recognizer->getRuleNames()[recognizer->getContext()->getRuleIndex()];
  errorMsg += ": expected rule " + expectedRuleName;
  recognizer->notifyErrorListeners(e.getOffendingToken(), errorMsg,
                                   make_exception_ptr(e));
}

}  // namespace parser
}  // namespace neug
