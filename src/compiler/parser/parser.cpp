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

#include <cctype>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

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
namespace {

bool isIdentifierChar(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

bool startsWithDefaultKeyword(const std::string& query, size_t pos) {
  static constexpr std::string_view keyword = "DEFAULT";
  if (pos + keyword.size() > query.size()) {
    return false;
  }
  if (pos > 0 && isIdentifierChar(query[pos - 1])) {
    return false;
  }
  for (auto i = 0u; i < keyword.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(query[pos + i])) !=
        keyword[i]) {
      return false;
    }
  }
  return pos + keyword.size() == query.size() ||
         !isIdentifierChar(query[pos + keyword.size()]);
}

std::string trim(std::string_view input) {
  size_t begin = 0;
  while (begin < input.size() &&
         std::isspace(static_cast<unsigned char>(input[begin]))) {
    ++begin;
  }
  auto end = input.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return std::string(input.substr(begin, end - begin));
}

size_t findMatchingBracket(const std::string& query, size_t openPos) {
  size_t squareDepth = 1;
  size_t parenDepth = 0;
  size_t braceDepth = 0;
  char quote = '\0';
  for (auto i = openPos + 1; i < query.size(); ++i) {
    const auto ch = query[i];
    if (quote != '\0') {
      if (ch == '\\') {
        ++i;
      } else if (ch == quote) {
        quote = '\0';
      }
      continue;
    }
    if (ch == '\'' || ch == '"' || ch == '`') {
      quote = ch;
      continue;
    }
    switch (ch) {
    case '[':
      ++squareDepth;
      break;
    case ']':
      if (--squareDepth == 0) {
        return i;
      }
      break;
    case '(':
      ++parenDepth;
      break;
    case ')':
      if (parenDepth > 0) {
        --parenDepth;
      }
      break;
    case '{':
      ++braceDepth;
      break;
    case '}':
      if (braceDepth > 0) {
        --braceDepth;
      }
      break;
    default:
      break;
    }
  }
  return std::string::npos;
}

std::vector<std::pair<std::string_view, char>> splitTopLevelListItems(
    std::string_view content) {
  std::vector<std::pair<std::string_view, char>> items;
  size_t start = 0;
  size_t squareDepth = 0;
  size_t parenDepth = 0;
  size_t braceDepth = 0;
  char quote = '\0';
  for (auto i = 0u; i < content.size(); ++i) {
    const auto ch = content[i];
    if (quote != '\0') {
      if (ch == '\\') {
        ++i;
      } else if (ch == quote) {
        quote = '\0';
      }
      continue;
    }
    if (ch == '\'' || ch == '"' || ch == '`') {
      quote = ch;
      continue;
    }
    switch (ch) {
    case '[':
      ++squareDepth;
      break;
    case ']':
      if (squareDepth > 0) {
        --squareDepth;
      }
      break;
    case '(':
      ++parenDepth;
      break;
    case ')':
      if (parenDepth > 0) {
        --parenDepth;
      }
      break;
    case '{':
      ++braceDepth;
      break;
    case '}':
      if (braceDepth > 0) {
        --braceDepth;
      }
      break;
    case ',':
    case ';':
      if (squareDepth == 0 && parenDepth == 0 && braceDepth == 0) {
        items.emplace_back(content.substr(start, i - start), ch);
        start = i + 1;
      }
      break;
    default:
      break;
    }
  }
  items.emplace_back(content.substr(start), '\0');
  return items;
}

std::optional<size_t> findTopLevelColon(std::string_view item) {
  size_t squareDepth = 0;
  size_t parenDepth = 0;
  size_t braceDepth = 0;
  char quote = '\0';
  for (auto i = 0u; i < item.size(); ++i) {
    const auto ch = item[i];
    if (quote != '\0') {
      if (ch == '\\') {
        ++i;
      } else if (ch == quote) {
        quote = '\0';
      }
      continue;
    }
    if (ch == '\'' || ch == '"' || ch == '`') {
      quote = ch;
      continue;
    }
    switch (ch) {
    case '[':
      ++squareDepth;
      break;
    case ']':
      if (squareDepth > 0) {
        --squareDepth;
      }
      break;
    case '(':
      ++parenDepth;
      break;
    case ')':
      if (parenDepth > 0) {
        --parenDepth;
      }
      break;
    case '{':
      ++braceDepth;
      break;
    case '}':
      if (braceDepth > 0) {
        --braceDepth;
      }
      break;
    case ':':
      if (squareDepth == 0 && parenDepth == 0 && braceDepth == 0) {
        return i;
      }
      break;
    default:
      break;
    }
  }
  return std::nullopt;
}

uint64_t parseFillCount(const std::string& text) {
  if (text.empty()) {
    THROW_PARSER_EXCEPTION("ARRAY fill count cannot be empty.");
  }
  for (const auto ch : text) {
    if (!std::isdigit(static_cast<unsigned char>(ch))) {
      THROW_PARSER_EXCEPTION("Invalid ARRAY fill count: " + text + ".");
    }
  }
  try {
    return std::stoull(text);
  } catch (const std::exception&) {
    THROW_PARSER_EXCEPTION("Invalid ARRAY fill count: " + text + ".");
  }
}

std::optional<std::string> rewriteCompactDefaultList(std::string_view content) {
  const auto items = splitTopLevelListItems(content);
  bool hasCompactSyntax = false;
  std::vector<std::pair<std::string, std::string>> compactFields;
  for (const auto& [rawItem, separator] : items) {
    auto item = trim(rawItem);
    auto colon = findTopLevelColon(item);
    if (!colon.has_value()) {
      if (separator == ';') {
        THROW_PARSER_EXCEPTION(
            "ARRAY fill segments separated by ';' must use value:count.");
      }
      compactFields.emplace_back(std::move(item), "1");
      continue;
    }
    hasCompactSyntax = true;
    auto value = trim(std::string_view(item).substr(0, *colon));
    auto countText = trim(std::string_view(item).substr(*colon + 1));
    if (value.empty()) {
      THROW_PARSER_EXCEPTION("ARRAY fill value cannot be empty.");
    }
    parseFillCount(countText);
    compactFields.emplace_back(std::move(value), std::move(countText));
  }
  if (!hasCompactSyntax) {
    return std::nullopt;
  }
  std::string result = "NEUG_COMPACT_DEFAULT(";
  for (auto i = 0u; i < compactFields.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += compactFields[i].first;
    result += ", ";
    result += compactFields[i].second;
  }
  result += ")";
  return result;
}

std::string rewriteCompactDefaultArrayLiterals(const std::string& query) {
  std::string result;
  result.reserve(query.size());
  char quote = '\0';
  for (auto i = 0u; i < query.size();) {
    const auto ch = query[i];
    if (quote != '\0') {
      result += ch;
      if (ch == '\\' && i + 1 < query.size()) {
        result += query[++i];
      } else if (ch == quote) {
        quote = '\0';
      }
      ++i;
      continue;
    }
    if (ch == '\'' || ch == '"' || ch == '`') {
      quote = ch;
      result += ch;
      ++i;
      continue;
    }
    if (ch == '/' && i + 1 < query.size() && query[i + 1] == '/') {
      result += query[i++];
      result += query[i++];
      while (i < query.size() && query[i] != '\n') {
        result += query[i++];
      }
      continue;
    }
    if (ch == '/' && i + 1 < query.size() && query[i + 1] == '*') {
      result += query[i++];
      result += query[i++];
      while (i < query.size()) {
        result += query[i];
        if (query[i] == '*' && i + 1 < query.size() && query[i + 1] == '/') {
          result += query[++i];
          ++i;
          break;
        }
        ++i;
      }
      continue;
    }
    if (!startsWithDefaultKeyword(query, i)) {
      result += ch;
      ++i;
      continue;
    }

    static constexpr std::string_view keyword = "DEFAULT";
    result.append(query, i, keyword.size());
    i += keyword.size();
    while (i < query.size() &&
           std::isspace(static_cast<unsigned char>(query[i]))) {
      result += query[i++];
    }
    if (i >= query.size() || query[i] != '[') {
      continue;
    }
    const auto closePos = findMatchingBracket(query, i);
    if (closePos == std::string::npos) {
      result += query.substr(i);
      break;
    }
    const auto content =
        std::string_view(query).substr(i + 1, closePos - i - 1);
    auto rewritten = rewriteCompactDefaultList(content);
    if (rewritten.has_value()) {
      result += *rewritten;
    } else {
      result += query.substr(i, closePos - i + 1);
    }
    i = closePos + 1;
  }
  return result;
}

}  // namespace

std::vector<std::shared_ptr<Statement>> Parser::parseQuery(
    std::string_view query) {
  auto queryStr = std::string(query);
  queryStr = common::StringUtils::ltrim(queryStr);
  queryStr = common::StringUtils::ltrimNewlines(queryStr);
  queryStr = rewriteCompactDefaultArrayLiterals(queryStr);
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
