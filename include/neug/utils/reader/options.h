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

#pragma once

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/scanner.h>
#include <cstdint>
#include <memory>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace reader {

struct ReadSharedState;
struct EntrySchema;

using options_t = common::case_insensitive_map_t<std::string>;

template <typename T>
class Option {
 public:
  using ParseFunc = std::function<T(const std::string&)>;

  Option(std::string key, std::string default_val, ParseFunc parse_func)
      : key_(std::move(key)),
        default_val_(std::move(default_val)),
        parse_func_(std::move(parse_func)) {}

  T get(const options_t& options) const {
    auto it = options.find(key_);
    std::string value = (it != options.end()) ? it->second : default_val_;
    return parse_func_(value);
  }

  const std::string& getKey() const { return key_; }

  static Option<int32_t> Int32Option(const std::string& key,
                                     int32_t default_val) {
    return Option<int>(
        key, std::to_string(default_val), [](const std::string& s) -> int32_t {
          try {
            return static_cast<int32_t>(std::stol(s));
          } catch (const std::exception& e) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse int: " + s);
          }
        });
  }

  static Option<int64_t> Int64Option(const std::string& key,
                                     int64_t default_val) {
    return Option<int64_t>(
        key, std::to_string(default_val), [](const std::string& s) -> int64_t {
          try {
            return std::stoll(s);
          } catch (const std::exception& e) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse int64_t: " + s);
          }
        });
  }

  static Option<std::string> StringOption(const std::string& key,
                                          const std::string& default_val) {
    return Option<std::string>(
        key, default_val,
        [](const std::string& s) -> std::string { return s; });
  }

  static Option<char> CharOption(const std::string& key, char default_val) {
    return Option<char>(key, std::string(1, default_val),
                        [](const std::string& s) -> char { return s[0]; });
  }

  static Option<bool> BoolOption(const std::string& key, bool default_val) {
    return Option<bool>(
        key, default_val ? "true" : "false", [](const std::string& s) -> bool {
          std::string lower = s;
          std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
          if (lower == "true" || lower == "1" || lower == "yes" ||
              lower == "on") {
            return true;
          } else if (lower == "false" || lower == "0" || lower == "no" ||
                     lower == "off") {
            return false;
          } else {
            THROW_INVALID_ARGUMENT_EXCEPTION("Invalid boolean value: " + s);
          }
        });
  }

 private:
  std::string key_;
  std::string default_val_;
  ParseFunc parse_func_;
};

struct CSVParseOptions {
  Option<char> delimiter = Option<char>::CharOption("delim", '|');
  Option<bool> quoting = Option<bool>::BoolOption("quoting", true);
  Option<char> quote_char = Option<char>::CharOption("quote", '"');
  Option<bool> escaping = Option<bool>::BoolOption("escaping", true);
  Option<char> escape_char = Option<char>::CharOption("escape", '\\');
  Option<bool> has_header = Option<bool>::BoolOption("header", true);
};

struct ReadOptions {
  Option<bool> use_threads = Option<bool>::BoolOption("parallel", true);
  Option<bool> batch_read = Option<bool>::BoolOption("batch_read", true);
  Option<int64_t> batch_size =
      Option<int64_t>::Int64Option("batch_size", 1 << 20);  // default 1MB
  Option<bool> autogenerate_column_names =
      Option<bool>::BoolOption("autogenerate_column_names", false);
  Option<int32_t> skip_rows = Option<int32_t>::Int32Option("skip_rows", 0);
};

/**
 * @brief Template base class for building format-specific scan options
 *
 * This template class provides a generic interface for building scan options
 * for different data formats. It handles column projection (skipColumns) and
 * row filtering (skipRows) operations. Derived classes implement
 * format-specific option building logic.
 *
 * @tparam T The type of options structure returned by build() and modified by
 *           skipColumns() and skipRows()
 */
template <class T>
class OptionsBuilder {
 public:
  /**
   * @brief Constructs an OptionsBuilder with the given shared state
   * @param state The shared read state containing schema and configuration
   */
  explicit OptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : state(state) {}
  virtual ~OptionsBuilder() = default;

  /**
   * @brief Builds the options structure from the shared state
   * @return The built options structure
   */
  virtual T build() const = 0;

  /**
   * @brief Applies column projection to exclude skipped columns
   * @param options The options structure to modify
   * @return true if column projection was successfully applied, false otherwise
   */
  virtual bool skipColumns(T& options) { return false; }

  /**
   * @brief Applies row filtering based on filter expressions
   * @param options The options structure to modify
   * @return true if row filtering was successfully applied, false otherwise
   */
  virtual bool skipRows(T& options) { return false; }

 protected:
  std::shared_ptr<ReadSharedState> state;
};

std::shared_ptr<arrow::Schema> createSchema(const EntrySchema& entrySchema);

/**
 * @brief Structure containing Arrow dataset scan options and file format
 *
 * This structure encapsulates both the scan options (schema, projection,
 * filtering, etc.) and the file format (CSV, Parquet, etc.) needed to read
 * Arrow datasets. The scanOptions and fileFormat are built together during
 * the build() process and used together when creating scanners.
 */
struct ArrowOptions {
  /// Arrow dataset scan options (schema, projection, filtering, batch size,
  /// etc.)
  std::shared_ptr<arrow::dataset::ScanOptions> scanOptions;
  /// File format implementation (CsvFileFormat, ParquetFileFormat, etc.)
  std::shared_ptr<arrow::dataset::FileFormat> fileFormat;
};

/**
 * @brief Base class for building Arrow dataset scan options
 *
 * This class builds ArrowOptions from ReadSharedState, which includes both
 * arrow::dataset::ScanOptions and arrow::dataset::FileFormat. The build()
 * method performs the following operations:
 * - Dataset schema: converts common::DataType (protobuf) to Arrow DataType
 * - Fragment scan options: builds format-specific fragment options via
 *   buildFragmentOptions()
 * - Scan options: configures dataset schema, fragment options, threading,
 *   and batch size
 * - File format: builds format-specific FileFormat via buildFileFormat()
 *
 * The skipColumns() method implements column pruning by setting the projection
 * expression in scanOptions, excluding columns listed in state.skipColumns.
 *
 * The skipRows() method implements filter pushdown by converting
 * common::Expression to arrow::compute::Expression and setting it as the
 * filter in scanOptions.
 *
 * Derived classes must implement:
 * - buildFragmentOptions(): to provide format-specific fragment scan options
 *   (e.g., CsvFragmentScanOptions with parse_options and convert_options)
 * - buildFileFormat(): to provide format-specific FileFormat instances
 *   (e.g., CsvFileFormat, ParquetFileFormat)
 */
class ArrowOptionsBuilder : public OptionsBuilder<ArrowOptions> {
 public:
  /**
   * @brief Constructs an ArrowOptionsBuilder with the given shared state
   * @param state The shared read state containing schema and configuration
   */
  explicit ArrowOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : OptionsBuilder<ArrowOptions>(state){};

  virtual ~ArrowOptionsBuilder() override = default;

  /**
   * @brief Builds ArrowOptions from the shared state
   *
   * Creates both scanOptions and fileFormat, converting the entry schema
   * to Arrow dataset schema and building format-specific options.
   *
   * @return ArrowOptions containing scanOptions and fileFormat
   */
  virtual ArrowOptions build() const override = 0;

  /**
   * @brief Applies column projection to exclude skipped columns
   *
   * Modifies the projection expression in options.scanOptions to exclude
   * columns listed in state.skipColumns, implementing column pruning.
   *
   * @param options The ArrowOptions to modify
   * @return true if column projection was successfully applied, false otherwise
   */
  virtual bool skipColumns(ArrowOptions& options) override;

  /**
   * @brief Applies row filtering based on filter expressions
   *
   * Converts state.skipRows (common::Expression) to Arrow filter expression
   * and sets it in options.scanOptions, implementing filter pushdown.
   *
   * @param options The ArrowOptions to modify
   * @return true if row filtering was successfully applied, false otherwise
   */
  virtual bool skipRows(ArrowOptions& options) override;
};

/**
 * @brief CSV-specific implementation of Arrow scan options builder
 *
 * This class extends ArrowOptionsBuilder to provide CSV-specific functionality:
 * - buildFragmentOptions(): Builds CsvFragmentScanOptions with parse_options
 *   (delimiter, quoting, escaping, etc.) from CSVFormatOptions in the file
 *   schema, and convert_options (column type mappings) from the entry schema
 * - buildFileFormat(): Builds CsvFileFormat and extracts parse_options from
 *   the fragment_scan_options in scanOptions
 *
 * The parse_options are configured from CSVFormatOptions if available in
 * the file schema, otherwise defaults are used. The convert_options are
 * populated from the entry schema column types to enable explicit type
 * conversion (e.g., forcing int64 instead of int32).
 */
class ArrowCsvOptionsBuilder : public ArrowOptionsBuilder {
 public:
  /**
   * @brief Constructs an ArrowCsvScanOptionsBuilder with the given shared state
   * @param state The shared read state containing CSV schema and configuration
   */
  explicit ArrowCsvOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : ArrowOptionsBuilder(state){};

  virtual ArrowOptions build() const override;

 protected:
  /**
   * @brief Builds CSV-specific fragment scan options
   *
   * Creates CsvFragmentScanOptions with:
   * - parse_options: configured from CSVFormatOptions (delimiter, quoting,
   *   escaping, etc.) or defaults
   * - convert_options: column type mappings from entry schema to enable
   *   explicit type conversion
   *
   * @return CsvFragmentScanOptions instance
   */
  std::shared_ptr<arrow::dataset::FragmentScanOptions> buildFragmentOptions()
      const;

  /**
   * @brief Builds CsvFileFormat from scan options
   *
   * Extracts parse_options from the CsvFragmentScanOptions in the scanOptions
   * and configures the CsvFileFormat. If fragment_scan_options is null or not
   * CsvFragmentScanOptions, a warning is logged and default configuration is
   * used.
   *
   * @param options The scan options containing fragment_scan_options
   * @return CsvFileFormat instance configured with parse_options
   */
  std::shared_ptr<arrow::dataset::FileFormat> buildFileFormat(
      const arrow::dataset::ScanOptions& options) const;
};

}  // namespace reader
}  // namespace neug
