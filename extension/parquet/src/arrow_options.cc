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

#include "parquet/arrow_options.h"

#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/reader.h"
#include "parquet/arrow_type_converter.h"
#include "parquet/expression_converter.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::Schema> createSchema(const EntrySchema& entrySchema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(entrySchema.columnNames.size());
  for (size_t i = 0; i < entrySchema.columnNames.size(); ++i) {
    const std::string& columnName = entrySchema.columnNames[i];
    if (!entrySchema.columnTypes[i]) {
      THROW_RUNTIME_ERROR("Column type is null for column: " + columnName);
    }
    ArrowTypeConverter arrowConverter;
    auto arrowType = arrowConverter.convert(*entrySchema.columnTypes[i]);
    if (!arrowType) {
      THROW_RUNTIME_ERROR("Failed to convert column type for column: " +
                          columnName);
    }
    fields.push_back(arrow::field(columnName, arrowType, false));
  }
  return std::make_shared<arrow::Schema>(fields);
}

bool ArrowOptionsBuilder::projectColumns(ArrowOptions& options) {
  if (state->projectColumns.empty()) {
    return true;
  }
  if (!options.scanOptions) {
    THROW_INVALID_ARGUMENT_EXCEPTION("ScanOptions is null in ArrowOptions");
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  const EntrySchema& entrySchema = *state->schema.entry;
  const auto& columns = state->projectColumns;
  const auto& allColumnNames = entrySchema.columnNames;
  for (const auto& column : columns) {
    if (std::find(allColumnNames.begin(), allColumnNames.end(), column) ==
        allColumnNames.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Column not found in entry schema: " +
                                       column);
    }
  }

  auto dataset_schema = createSchema(entrySchema);
  auto project_desc =
      arrow::dataset::ProjectionDescr::FromNames(columns, *dataset_schema);
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

}  // namespace reader
}  // namespace neug
