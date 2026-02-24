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

#include "neug/compiler/parser/antlr_parser/parser_error_listener.h"

#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/exception.h"

using namespace antlr4;
using namespace neug::common;

namespace neug {
namespace parser {

void ParserErrorListener::syntaxError(Recognizer* recognizer,
                                      Token* offendingSymbol, size_t line,
                                      size_t charPositionInLine,
                                      const std::string& msg,
                                      std::exception_ptr /*e*/) {
  auto finalError = msg + " (line: " + std::to_string(line) +
                    ", offset: " + std::to_string(charPositionInLine) + ")\n" +
                    formatUnderLineError(*recognizer, *offendingSymbol, line,
                                         charPositionInLine);
  THROW_PARSER_EXCEPTION(finalError);
}

std::string ParserErrorListener::formatUnderLineError(
    Recognizer& recognizer, const Token& offendingToken, size_t line,
    size_t charPositionInLine) {
  auto tokens = (CommonTokenStream*) recognizer.getInputStream();
  auto input = tokens->getTokenSource()->getInputStream()->toString();
  auto errorLine = StringUtils::split(input, "\n", false)[line - 1];
  auto underLine = std::string(" ");
  for (auto i = 0u; i < charPositionInLine; ++i) {
    underLine += " ";
  }
  for (auto i = offendingToken.getStartIndex();
       i <= offendingToken.getStopIndex(); ++i) {
    underLine += "^";
  }
  return "\"" + errorLine + "\"\n" + underLine;
}

}  // namespace parser
}  // namespace neug
