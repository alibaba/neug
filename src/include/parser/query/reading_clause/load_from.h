#pragma once

#include "reading_clause.h"
#include "src/include/parser/ddl/parsed_property_definition.h"
#include "src/include/parser/expression/parsed_expression.h"
#include "src/include/parser/scan_source.h"

namespace gs {
namespace parser {

class LoadFrom : public ReadingClause {
  static constexpr common::ClauseType clauseType_ =
      common::ClauseType::LOAD_FROM;

 public:
  explicit LoadFrom(std::unique_ptr<BaseScanSource> source)
      : ReadingClause{clauseType_}, source{std::move(source)} {}

  BaseScanSource* getSource() const { return source.get(); }

  void setParingOptions(options_t options) {
    parsingOptions = std::move(options);
  }
  const options_t& getParsingOptions() const { return parsingOptions; }

  void setPropertyDefinitions(std::vector<ParsedColumnDefinition> definitions) {
    columnDefinitions = std::move(definitions);
  }
  const std::vector<ParsedColumnDefinition>& getColumnDefinitions() const {
    return columnDefinitions;
  }

 private:
  std::unique_ptr<BaseScanSource> source;
  std::vector<ParsedColumnDefinition> columnDefinitions;
  options_t parsingOptions;
};

}  // namespace parser
}  // namespace gs
