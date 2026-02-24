/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/utils/reader/options.h"
#include "neug/compiler/common/assert.h"
#include "neug/utils/reader/expression_converter.h"
#include "neug/utils/reader/schema.h"

#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_csv.h>
#include <arrow/dataset/scanner.h>
#include <arrow/dataset/type_fwd.h>
#include <arrow/type.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <vector>

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/type_converter.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::Schema> createSchema(const EntrySchema& entrySchema) {
  // Build dataset schema from entry schema (column names and types)
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(entrySchema.columnNames.size());
  for (size_t i = 0; i < entrySchema.columnNames.size(); ++i) {
    const std::string& columnName = entrySchema.columnNames[i];
    if (!entrySchema.columnTypes[i]) {
      LOG(ERROR) << "Column type is null for column: " << columnName;
      THROW_RUNTIME_ERROR("Column type is null for column: " + columnName);
    }
    const ::common::DataType& columnType = *entrySchema.columnTypes[i];

    // Convert Protobuf DataType to Arrow DataType
    ArrowTypeConverter arrowConverter;
    auto arrowType = arrowConverter.convert(columnType);
    if (!arrowType) {
      LOG(ERROR) << "Failed to convert column type for column: " << columnName;
      THROW_RUNTIME_ERROR("Failed to convert column type for column: " +
                          columnName);
    }

    fields.push_back(arrow::field(columnName, arrowType, false));
  }

  return std::make_shared<arrow::Schema>(fields);
}

bool ArrowOptionsBuilder::skipColumns(ArrowOptions& options) {
  if (state->skipColumns.empty()) {
    return true;
  }

  if (!options.scanOptions) {
    THROW_INVALID_ARGUMENT_EXCEPTION("ScanOptions is null in ArrowOptions");
  }

  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  const EntrySchema& entrySchema = *state->schema.entry;

  // Build projection columns excluding skipped ones
  std::vector<std::string> project_columns;
  const auto& allColumnNames = entrySchema.columnNames;
  project_columns.reserve(allColumnNames.size());
  for (const auto& name : allColumnNames) {
    if (std::find(state->skipColumns.begin(), state->skipColumns.end(), name) ==
        state->skipColumns.end()) {
      project_columns.push_back(name);
    }
  }

  if (project_columns.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Empty project columns after column pruning");
  }

  auto dataset_schema = createSchema(entrySchema);
  auto project_desc = arrow::dataset::ProjectionDescr::FromNames(
      project_columns, *dataset_schema);
  if (!project_desc.ok()) {
    LOG(ERROR) << "Failed to build projection: "
               << project_desc.status().message();
    return false;
  }

  options.scanOptions->projection = project_desc.ValueOrDie().expression;
  options.scanOptions->projected_schema = project_desc.ValueOrDie().schema;
  return true;
}

bool ArrowOptionsBuilder::skipRows(ArrowOptions& options) {
  if (!state->skipRows) {
    return true;
  }

  if (!options.scanOptions) {
    LOG(ERROR) << "ScanOptions is null in ArrowOptions";
    return false;
  }

  ArrowExpressionConverter converter;
  options.scanOptions->filter = converter.convert(*state->skipRows);
  return true;
}

ArrowOptions ArrowCsvOptionsBuilder::build() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  auto scanOptions = std::make_shared<arrow::dataset::ScanOptions>();

  // Build format-specific fragment scan options
  auto fragment_scan_options = buildFragmentOptions();
  scanOptions->fragment_scan_options = fragment_scan_options;

  // Build file format using scan options
  auto fileFormat = buildFileFormat(*scanOptions);

  // Create ArrowOptions with both scanOptions and fileFormat
  ArrowOptions arrowOptions;
  arrowOptions.scanOptions = scanOptions;
  arrowOptions.fileFormat = fileFormat;
  return arrowOptions;
}

std::shared_ptr<arrow::dataset::FileFormat>
ArrowCsvOptionsBuilder::buildFileFormat(
    const arrow::dataset::ScanOptions& options) const {
  auto fileFormat = std::make_shared<arrow::dataset::CsvFileFormat>();
  auto fragmentOpts = options.fragment_scan_options;
  if (!fragmentOpts) {
    LOG(WARNING)
        << "fragment_scan_options is null in ScanOptions, parse_options "
           "will use default configuration";
    return fileFormat;
  }

  auto csvFragmentOpts =
      std::dynamic_pointer_cast<arrow::dataset::CsvFragmentScanOptions>(
          fragmentOpts);
  if (!csvFragmentOpts) {
    LOG(WARNING) << "fragment_scan_options is not CsvFragmentScanOptions, "
                    "parse_options will use default configuration";
    return fileFormat;
  }

  fileFormat->parse_options = csvFragmentOpts->parse_options;
  fileFormat->default_fragment_scan_options = options.fragment_scan_options;
  return fileFormat;
}

std::shared_ptr<arrow::dataset::FragmentScanOptions>
ArrowCsvOptionsBuilder::buildFragmentOptions() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  const FileSchema& fileSchema = state->schema.file;
  auto fragment_scan_options =
      std::make_shared<arrow::dataset::CsvFragmentScanOptions>();

  // set csv parse options
  arrow::csv::ParseOptions parse_options = arrow::csv::ParseOptions::Defaults();
  auto& options = fileSchema.options;
  CSVParseOptions parseOpts;
  parse_options.delimiter = parseOpts.delimiter.get(options);
  parse_options.quoting = parseOpts.quoting.get(options);
  parse_options.quote_char = parseOpts.quote_char.get(options);
  parse_options.escaping = parseOpts.escaping.get(options);
  parse_options.escape_char = parseOpts.escape_char.get(options);
  fragment_scan_options->parse_options = parse_options;

  // set read options
  const EntrySchema& entrySchema = *state->schema.entry;
  arrow::csv::ReadOptions read_options = arrow::csv::ReadOptions::Defaults();
  ReadOptions readOpts;
  read_options.use_threads = readOpts.use_threads.get(options);
  read_options.block_size = readOpts.batch_size.get(options);
  read_options.autogenerate_column_names =
      readOpts.autogenerate_column_names.get(options);
  read_options.skip_rows = readOpts.skip_rows.get(options);
  if (!entrySchema.columnNames.empty()) {
    read_options.column_names = entrySchema.columnNames;
  }
  fragment_scan_options->read_options = read_options;

  // set convert options
  arrow::csv::ConvertOptions convert_options =
      arrow::csv::ConvertOptions::Defaults();
  // set custom timestamp parsers
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  // set bool parsers
  convert_options.true_values.emplace_back("True");
  convert_options.true_values.emplace_back("true");
  convert_options.true_values.emplace_back("TRUE");
  convert_options.false_values.emplace_back("False");
  convert_options.false_values.emplace_back("false");
  convert_options.false_values.emplace_back("FALSE");
  if (!entrySchema.columnNames.empty() && !entrySchema.columnTypes.empty()) {
    auto& columnNames = entrySchema.columnNames;
    auto& columnTypes = entrySchema.columnTypes;
    NEUG_ASSERT(columnNames.size() == columnTypes.size());
    for (size_t i = 0; i < columnNames.size(); ++i) {
      const std::string& columnName = columnNames[i];
      const ::common::DataType& columnType = *columnTypes[i];
      // Convert Protobuf DataType to Arrow DataType
      ArrowTypeConverter arrowConverter;
      auto arrowType = arrowConverter.convert(columnType);
      if (!arrowType) {
        THROW_CONVERSION_EXCEPTION(
            "Failed to convert column type for column: " + columnName);
      }
      if (convert_options.column_types.find(columnName) !=
          convert_options.column_types.end()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Duplicate column name found: " +
                                         columnName);
      }
      convert_options.column_types[columnName] = arrowType;
    }
  }
  fragment_scan_options->convert_options = convert_options;
  return fragment_scan_options;
}

}  // namespace reader
}  // namespace neug
