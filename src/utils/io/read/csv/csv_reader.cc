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
CsvReader::CsvReader(std::shared_ptr<ReadSharedState> sharedState,
                     std::unique_ptr<CsvOptionsBuilder> optionsBuilder)
    : sharedState_(std::move(sharedState)),
      optionsBuilder_(std::move(optionsBuilder)) {}

CsvReader::~CsvReader() = default;

std::shared_ptr<IDataChunkSupplier> CsvReader::getDataChunkSupplier() {
  if (!sharedState_ || !optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("CSV reader is not initialized");
  }
  auto config = optionsBuilder_->build();
  optionsBuilder_->projectColumns(config);
  if (sharedState_->schema.file.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  // Each execution owns this supplier. Files are initialized one at a time,
  // only when requested, and filtering/projection happen before yielding.
  class CsvStreamSupplier final : public IDataChunkSupplier {
   public:
    CsvStreamSupplier(std::shared_ptr<ReadSharedState> state,
                      CsvReadConfig config)
        : state_(std::move(state)), config_(std::move(config)) {}

    int64_t RowNum() const override { return -1; }

    std::shared_ptr<DataChunk> GetNextChunk() override {
      while (true) {
        if (!current_) {
          if (file_ == state_->schema.file.paths.size()) {
            return nullptr;
          }
          const auto& path = state_->schema.file.paths[file_++];
          auto read_config = config_;
          read_config.include_columns =
              state_->skipRows ? config_.column_names : config_.include_columns;
          current_ = std::make_shared<CSVChunkSupplier>(
              path, read_config,
              io::bindInputStream(state_->stream_opener, path));
        }
        auto chunk = current_->GetNextChunk();
        if (!chunk) {
          current_.reset();
          continue;
        }
        if (!state_->skipRows) {
          return chunk;
        }
        auto filtered = filter_chunk(*chunk, state_->skipRows,
                                     config_.column_names, state_->parameters);
        auto projected = project_chunk(filtered, config_.column_names,
                                       config_.include_columns);
        return std::make_shared<DataChunk>(std::move(projected));
      }
    }

   private:
    std::shared_ptr<ReadSharedState> state_;
    CsvReadConfig config_;
    size_t file_ = 0;
    std::shared_ptr<IDataChunkSupplier> current_;
  };
  return std::make_shared<CsvStreamSupplier>(sharedState_, std::move(config));
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
