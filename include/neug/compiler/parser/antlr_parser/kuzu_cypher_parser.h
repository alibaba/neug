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

// ANTLR4 generates code with unused parameters.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_parser.h"
#pragma GCC diagnostic pop

namespace neug {
namespace parser {

class KuzuCypherParser : public CypherParser {
 public:
  explicit KuzuCypherParser(antlr4::TokenStream* input) : CypherParser(input) {}

  void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) override;

  void notifyNodePatternWithoutParentheses(std::string nodeName,
                                           antlr4::Token* startToken) override;

  void notifyInvalidNotEqualOperator(antlr4::Token* startToken) override;

  void notifyEmptyToken(antlr4::Token* startToken) override;

  void notifyReturnNotAtEnd(antlr4::Token* startToken) override;

  void notifyNonBinaryComparison(antlr4::Token* startToken) override;
};

}  // namespace parser
}  // namespace neug
