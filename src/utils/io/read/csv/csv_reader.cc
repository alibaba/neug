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

#include "neug/utils/io/reader.h"

#include "neug/common/types/container_types.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/common/type_converter.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {
namespace {

CsvReadConfig read_config_for_supplier(const CsvReadConfig& config) {
  CsvReadConfig read_config = config;
  read_config.include_columns = config.column_names;
  return read_config;
}

}  // namespace

CsvReader::CsvReader(std::shared_ptr<ReadSharedState> sharedState,
                     std::unique_ptr<CsvOptionsBuilder> optionsBuilder)
    : sharedState_(std::move(sharedState)),
      optionsBuilder_(std::move(optionsBuilder)) {}

CsvReader::~CsvReader() = default;

void CsvReader::read(std::shared_ptr<ReadLocalState> /*localState*/,
                     execution::Context& ctx) {
  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
  }

  auto config = optionsBuilder_->build();
  if (!optionsBuilder_->projectColumns(config)) {
    LOG(WARNING) << "Failed to set column projection, using all columns";
  }

  const auto& fileSchema = sharedState_->schema.file;
  ReadOptions readOpts;
  const bool use_batch_read = readOpts.batch_read.get(fileSchema.options);

  auto read_config = read_config_for_supplier(config);
  if (sharedState_->skipRows) {
    // Need all columns to evaluate row-filter expression;
    // full_read will project afterwards.
    read_config.include_columns = config.column_names;
  } else if (!sharedState_->projectColumns.empty()) {
    if (use_batch_read) {
      // batch_read streams chunks directly to the consumer without
      // post-projection, so push column projection down to the supplier.
      read_config.include_columns = config.include_columns;
    } else {
      // full_read handles projection via project_chunk().
      read_config.include_columns = config.column_names;
    }
  }

  const auto& paths = fileSchema.paths;
  if (paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  suppliers.reserve(paths.size());
  for (const auto& path : paths) {
    suppliers.push_back(std::make_shared<CSVChunkSupplier>(
        path, read_config,
        io::bindInputStream(sharedState_->stream_opener, path)));
  }

  if (use_batch_read && !sharedState_->skipRows) {
    batch_read(suppliers, ctx);
  } else {
    full_read(suppliers, ctx, config);
  }
}

void CsvReader::full_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output, const CsvReadConfig& output_config) {
  auto merged = read_all_chunks(suppliers);

  if (merged.col_num() == 0) {
    // No data rows at all (e.g. a header-only file). Emit an empty result;
    // there is nothing to validate, filter or project.
    output.clear();
    return;
  }

  int expected_cols = sharedState_->columnNum();
  if (expected_cols > 0 &&
      static_cast<int>(merged.col_num()) != expected_cols &&
      sharedState_->projectColumns.empty()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and CSV data, schema: " +
        std::to_string(expected_cols) +
        ", data: " + std::to_string(merged.col_num()));
  }

  auto filtered =
      filter_chunk(merged, sharedState_->skipRows, output_config.column_names,
                   sharedState_->parameters);
  auto projected = project_chunk(filtered, output_config.column_names,
                                 sharedState_->projectColumns.empty()
                                     ? output_config.include_columns
                                     : sharedState_->projectColumns);

  output.clear();
  output.append_chunk(std::move(projected));
}

void CsvReader::batch_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output) {
  output.clear();
  for (const auto& supplier : suppliers) {
    while (auto chunk = supplier->GetNextChunk()) {
      output.append_chunk(std::move(*chunk));
    }
  }
}

result<std::shared_ptr<EntrySchema>> CsvReader::inferSchema() {
  if (!sharedState_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "SharedState is null");
  }
  if (!optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Options builder is null");
  }

  auto config = optionsBuilder_->build();
  const auto& paths = sharedState_->schema.file.paths;
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  ReadOptions readOpts;
  const bool autogenerate =
      readOpts.autogenerate_column_names.get(sharedState_->schema.file.options);

  const io::InputStreamFactory stream_factory =
      io::bindInputStream(sharedState_->stream_opener, paths[0]);

  if (config.column_names.empty() && !autogenerate) {
    config.column_names = read_header(paths[0], config, stream_factory);
  } else if (config.column_names.empty() && autogenerate) {
    std::string line;
    try {
      if (stream_factory) {
        line = io::readFirstLine(stream_factory);
      } else {
        std::ifstream input(paths[0]);
        if (!input || !std::getline(input, line)) {
          RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                              "Failed to read first row for schema inference");
        }
      }
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to open CSV file for schema inference: " +
                              std::string(e.what()));
    }
    size_t column_count = 1;
    for (char ch : line) {
      if (ch == config.delimiter) {
        ++column_count;
      }
    }
    config.column_names.reserve(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      config.column_names.push_back("f" + std::to_string(i));
    }
  }

  if (!autogenerate) {
    std::unordered_set<std::string> seen;
    for (const auto& name : config.column_names) {
      if (!seen.insert(name).second) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                            "Duplicate column name found: " + name);
      }
    }
  }

  CsvReadConfig sniff_config = config;
  sniff_config.include_columns = config.column_names;
  for (const auto& name : config.column_names) {
    sniff_config.column_types[name] = DataType(DataTypeId::kVarchar);
  }

  auto supplier = std::make_shared<CSVChunkSupplier>(paths[0], sniff_config,
                                                     stream_factory);
  auto sample_chunk = supplier->GetNextChunk();
  if (!sample_chunk) {
    // No data rows — default all columns to VARCHAR.
    auto entrySchema = std::make_shared<TableEntrySchema>();
    entrySchema->columnNames = config.column_names;
    entrySchema->columnTypes.reserve(config.column_names.size());
    NeuGTypeConverter converter;
    for (size_t i = 0; i < config.column_names.size(); ++i) {
      entrySchema->columnTypes.push_back(
          converter.inferCommonType(DataType(DataTypeId::kVarchar)));
    }
    return entrySchema;
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  entrySchema->columnNames = config.column_names;
  entrySchema->columnTypes.reserve(config.column_names.size());

  // Helper lambdas for type detection.
  auto is_bool_token = [](const std::string& s) -> bool {
    if (s.size() < 4 || s.size() > 5)
      return false;
    std::string lower;
    lower.reserve(s.size());
    for (char c : s)
      lower.push_back(static_cast<char>(std::tolower(c)));
    return lower == "true" || lower == "false";
  };
  auto is_date_token = [](const std::string& s) -> bool {
    // YYYY-MM-DD (exactly 10 chars)
    if (s.size() != 10)
      return false;
    if (s[4] != '-' || s[7] != '-')
      return false;
    for (int i : {0, 1, 2, 3, 5, 6, 8, 9}) {
      if (!std::isdigit(static_cast<unsigned char>(s[i])))
        return false;
    }
    return true;
  };
  auto is_datetime_token = [](const std::string& s) -> bool {
    // YYYY-MM-DD HH:MM:SS (19 chars) or with fractional seconds
    if (s.size() < 19)
      return false;
    if (s[4] != '-' || s[7] != '-')
      return false;
    if (s[10] != ' ' && s[10] != 'T')
      return false;
    if (s[13] != ':' || s[16] != ':')
      return false;
    for (int i : {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18}) {
      if (!std::isdigit(static_cast<unsigned char>(s[i])))
        return false;
    }
    return true;
  };

  NeuGTypeConverter converter;
  for (size_t col = 0; col < config.column_names.size(); ++col) {
    bool all_int = true;
    bool all_double = true;
    bool all_bool = true;
    bool all_date = true;
    bool all_datetime = true;
    bool all_date_or_datetime = true;
    bool any_datetime = false;

    bool has_value = false;
    for (size_t row = 0; row < sample_chunk->row_num(); ++row) {
      auto value = sample_chunk->get(static_cast<int>(col))->get_elem(row);
      if (value.IsNull()) {
        continue;
      }
      has_value = true;
      const std::string& token = value.GetValue<std::string>();
      if (token.empty()) {
        all_int = false;
        all_double = false;
        all_bool = false;
        all_date = false;
        all_datetime = false;
        all_date_or_datetime = false;
        continue;
      }
      // Integer check (must fit in int64_t)
      char* end = nullptr;
      errno = 0;
      std::strtoll(token.c_str(), &end, 10);
      if (end != token.c_str() + token.size() || errno == ERANGE) {
        all_int = false;
      }
      // Double check
      end = nullptr;
      std::strtod(token.c_str(), &end);
      if (end != token.c_str() + token.size()) {
        all_double = false;
      }
      // Bool check
      if (!is_bool_token(token))
        all_bool = false;
      // Temporal checks
      bool is_dt = is_datetime_token(token);
      bool is_d = is_date_token(token);
      if (!is_dt)
        all_datetime = false;
      if (!is_d)
        all_date = false;
      if (!is_dt && !is_d)
        all_date_or_datetime = false;
      if (is_dt)
        any_datetime = true;
    }
    DataType inferred_type(DataTypeId::kVarchar);
    if (has_value && all_int) {
      inferred_type = DataType(DataTypeId::kInt64);
    } else if (has_value && all_double) {
      inferred_type = DataType(DataTypeId::kDouble);
    } else if (has_value && all_bool) {
      inferred_type = DataType(DataTypeId::kBoolean);
    } else if (has_value && all_datetime) {
      inferred_type = DataType(DataTypeId::kTimestampMs);
    } else if (has_value && all_date_or_datetime && any_datetime) {
      // Mixed date/datetime values → infer as timestamp
      inferred_type = DataType(DataTypeId::kTimestampMs);
    } else if (has_value && all_date) {
      inferred_type = DataType(DataTypeId::kDate);
    }
    entrySchema->columnTypes.push_back(
        converter.inferCommonType(inferred_type));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace neug
