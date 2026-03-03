/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "json_options.h"
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_json.h>
#include <arrow/json/options.h>
#include <arrow/json/reader.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <memory>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace reader {

ArrowOptions ArrowJsonOptionsBuilder::build() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  auto scanOptions = std::make_shared<arrow::dataset::ScanOptions>();

  // Build format-specific fragment scan options
  auto fragment_scan_options = buildFragmentOptions();
  scanOptions->fragment_scan_options = fragment_scan_options;
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  scanOptions->dataset_schema = createSchema(*state->schema.entry);

  // Build file format using scan options
  auto fileFormat = buildFileFormat(*scanOptions);

  // Create ArrowOptions with both scanOptions and fileFormat
  ArrowOptions arrowOptions;
  arrowOptions.scanOptions = scanOptions;
  arrowOptions.fileFormat = fileFormat;
  return arrowOptions;
}

std::shared_ptr<arrow::dataset::FragmentScanOptions>
ArrowJsonOptionsBuilder::buildFragmentOptions() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  auto fragment_scan_options =
      std::make_shared<arrow::dataset::JsonFragmentScanOptions>();

  const FileSchema& fileSchema = state->schema.file;
  auto& options = fileSchema.options;
  JsonParseOptions jsonOpts;
  // Build json parse options
  arrow::json::ParseOptions parse_options =
      arrow::json::ParseOptions::Defaults();
  parse_options.newlines_in_values = jsonOpts.newlines_in_values.get(options);
  fragment_scan_options->parse_options = parse_options;

  // Build json read options
  arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();
  ReadOptions readOpts;
  read_options.use_threads = readOpts.use_threads.get(options);
  read_options.block_size = readOpts.batch_size.get(options);
  fragment_scan_options->read_options = read_options;

  return fragment_scan_options;
}

std::shared_ptr<arrow::dataset::FileFormat>
ArrowJsonOptionsBuilder::buildFileFormat(
    const arrow::dataset::ScanOptions& options) const {
  auto fileFormat = std::make_shared<arrow::dataset::JsonFileFormat>();
  auto fragmentOpts = options.fragment_scan_options;
  if (!fragmentOpts) {
    return fileFormat;
  }

  auto jsonFragmentOpts =
      std::dynamic_pointer_cast<arrow::dataset::JsonFragmentScanOptions>(
          fragmentOpts);
  if (!jsonFragmentOpts) {
    return fileFormat;
  }

  fileFormat->default_fragment_scan_options = options.fragment_scan_options;
  return fileFormat;
}

}  // namespace reader
}  // namespace neug
