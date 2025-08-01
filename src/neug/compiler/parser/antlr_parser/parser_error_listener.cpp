#include "neug/compiler/parser/antlr_parser/parser_error_listener.h"

#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/parser.h"

using namespace antlr4;
using namespace gs::common;

namespace gs {
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
  throw ParserException(finalError);
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
}  // namespace gs
