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

#pragma once

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_json.h>
#include <arrow/json/options.h>
#include <memory>
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"

namespace neug {
namespace reader {

struct JsonParseOptions {
  Option<bool> newline_delimited =
      Option<bool>::BoolOption("newline_delimited", false);
  Option<bool> newlines_in_values =
      Option<bool>::BoolOption("newlines_in_values", false);
};

/**
 * @brief JSON-specific implementation of Arrow scan options builder
 *
 * This class extends ArrowOptionsBuilder to provide JSON-specific
 * functionality:
 * - buildFragmentOptions(): Builds JsonFragmentScanOptions with parse_options
 *   (newlines_in_values, etc.) from JsonParseOptions in the file schema
 * - buildFileFormat(): Builds JsonFileFormat
 */
class ArrowJsonOptionsBuilder : public ArrowOptionsBuilder {
 public:
  /**
   * @brief Constructs an ArrowJsonOptionsBuilder with the given shared state
   * @param state The shared read state containing JSON schema and configuration
   */
  explicit ArrowJsonOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : ArrowOptionsBuilder(state){};

  virtual ArrowOptions build() const override;

 protected:
  /**
   * @brief Builds JSON-specific fragment scan options
   *
   * Creates JsonFragmentScanOptions with:
   * - parse_options: configured from JsonParseOptions (newlines_in_values,
   * etc.) or defaults
   *
   * @return JsonFragmentScanOptions instance
   */
  std::shared_ptr<arrow::dataset::FragmentScanOptions> buildFragmentOptions()
      const;

  /**
   * @brief Builds JsonFileFormat from scan options
   *
   * Extracts parse_options from the JsonFragmentScanOptions in the scanOptions
   * and configures the JsonFileFormat.
   *
   * @param options The scan options containing fragment_scan_options
   * @return JsonFileFormat instance configured with parse_options
   */
  std::shared_ptr<arrow::dataset::FileFormat> buildFileFormat(
      const arrow::dataset::ScanOptions& options) const;
};

}  // namespace reader
}  // namespace neug
