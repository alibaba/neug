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

#include "neug/utils/reader/reader.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/row_expression_filter.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/type_converter.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace neug {
namespace reader {
namespace {

constexpr size_t kDefaultJsonChunkRows = 4096;
constexpr size_t kMaxJsonChunkRows = 65536;

size_t resolve_chunk_size(const JsonReadConfig& config) {
  if (config.chunk_size <= 0) {
    return kDefaultJsonChunkRows;
  }
  return static_cast<size_t>(
      std::clamp<int64_t>(config.chunk_size, 1, kMaxJsonChunkRows));
}

std::string read_file_to_string(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    THROW_IO_EXCEPTION("Failed to open file: " + path);
  }
  std::ostringstream ss;
  ss << input.rdbuf();
  return ss.str();
}

rapidjson::Document parse_json_array_file(const std::string& path) {
  const auto content = read_file_to_string(path);
  rapidjson::Document document;
  document.Parse(content.c_str(), content.size());
  if (document.HasParseError()) {
    THROW_IO_EXCEPTION("JSON parse error in file " + path + " at offset " +
                       std::to_string(document.GetErrorOffset()) + ": " +
                       rapidjson::GetParseError_En(document.GetParseError()));
  }
  if (!document.IsArray() || document.Empty()) {
    THROW_IO_EXCEPTION("Expected non-empty JSON array in file: " + path);
  }
  for (const auto& obj : document.GetArray()) {
    if (!obj.IsObject()) {
      THROW_IO_EXCEPTION("Expected JSON object in array in file: " + path);
    }
  }
  return document;
}

std::vector<std::string> convert_json_array_to_lines(const std::string& path) {
  auto document = parse_json_array_file(path);
  std::vector<std::string> lines;
  lines.reserve(document.Size());
  for (const auto& obj : document.GetArray()) {
    lines.push_back(rapidjson_stringify(obj));
  }
  return lines;
}

execution::Value parse_json_value(const rapidjson::Value& value,
                                  const DataType& data_type) {
  if (value.IsNull()) {
    return execution::Value(data_type);
  }
  switch (data_type.id()) {
  case DataTypeId::kBoolean:
    if (value.IsBool()) {
      return execution::Value::BOOLEAN(value.GetBool());
    }
    if (value.IsString()) {
      return execution::Value::BOOLEAN(
          execution::ValueConverter<bool>::typed_from_string(
              value.GetString()));
    }
    return execution::Value::BOOLEAN(value.GetBool());
  case DataTypeId::kInt32:
    if (value.IsInt()) {
      return execution::Value::INT32(value.GetInt());
    }
    return execution::Value::INT32(static_cast<int32_t>(value.GetInt64()));
  case DataTypeId::kUInt32:
    return execution::Value::UINT32(value.GetUint());
  case DataTypeId::kInt64:
    return execution::Value::INT64(value.GetInt64());
  case DataTypeId::kUInt64:
    return execution::Value::UINT64(value.GetUint64());
  case DataTypeId::kFloat:
    return execution::Value::FLOAT(static_cast<float>(value.GetDouble()));
  case DataTypeId::kDouble:
    return execution::Value::DOUBLE(value.GetDouble());
  case DataTypeId::kDate: {
    std::string str =
        value.IsString() ? value.GetString() : rapidjson_stringify(value);
    return execution::Value::CreateValue<execution::date_t>(
        execution::ValueConverter<execution::date_t>::typed_from_string(str));
  }
  case DataTypeId::kTimestampMs: {
    std::string str =
        value.IsString() ? value.GetString() : rapidjson_stringify(value);
    return execution::Value::CreateValue<execution::timestamp_ms_t>(
        execution::ValueConverter<execution::timestamp_ms_t>::typed_from_string(
            str));
  }
  case DataTypeId::kInterval: {
    std::string str =
        value.IsString() ? value.GetString() : rapidjson_stringify(value);
    return execution::Value::CreateValue<execution::interval_t>(
        execution::ValueConverter<execution::interval_t>::typed_from_string(
            str));
  }
  case DataTypeId::kVarchar:
    if (value.IsString()) {
      return execution::Value::STRING(value.GetString());
    }
    return execution::Value::STRING(rapidjson_stringify(value));
  default:
    if (value.IsString()) {
      return execution::Value::STRING(value.GetString());
    }
    return execution::Value::STRING(rapidjson_stringify(value));
  }
}

class JsonChunkSupplier : public IDataChunkSupplier {
 public:
  JsonChunkSupplier(const std::string& file_path, JsonReadConfig config)
      : file_path_(file_path), config_(std::move(config)) {
    if (config_.json_array_input) {
      document_ = parse_json_array_file(file_path_);
      doc_index_ = 0;
      row_num_ = static_cast<int64_t>(document_.Size());
    } else {
      input_ = std::make_unique<std::ifstream>(file_path_);
      if (!input_->is_open()) {
        THROW_IO_EXCEPTION("Failed to open JSON file: " + file_path_);
      }
      std::string line;
      while (std::getline(*input_, line)) {
        if (!line.empty()) {
          ++row_num_;
        }
      }
      input_ = std::make_unique<std::ifstream>(file_path_);
    }
    chunk_size_ = resolve_chunk_size(config_);
  }

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    const auto& selected_names = config_.include_columns.empty()
                                     ? config_.column_names
                                     : config_.include_columns;
    std::vector<DataType> selected_types;
    selected_types.reserve(selected_names.size());
    for (const auto& name : selected_names) {
      auto iter = config_.column_types.find(name);
      if (iter == config_.column_types.end()) {
        selected_types.emplace_back(DataTypeId::kVarchar);
      } else {
        selected_types.push_back(iter->second);
      }
    }

    std::vector<std::shared_ptr<execution::IContextColumnBuilder>> builders;
    builders.reserve(selected_names.size());
    for (const auto& type : selected_types) {
      builders.push_back(execution::ColumnsUtils::create_builder(type));
    }

    size_t rows_in_chunk = 0;
    while (rows_in_chunk < chunk_size_) {
      const rapidjson::Value* obj_ptr = nullptr;
      if (config_.json_array_input) {
        if (doc_index_ >= document_.Size()) {
          break;
        }
        obj_ptr = &document_.GetArray()[doc_index_++];
      } else {
        std::string line;
        if (!input_ || !std::getline(*input_, line)) {
          break;
        }
        if (line.empty()) {
          continue;
        }
        line_doc_.Parse(line.c_str(), line.size());
        if (line_doc_.HasParseError() || !line_doc_.IsObject()) {
          THROW_IO_EXCEPTION("Invalid JSON object in file: " + file_path_);
        }
        obj_ptr = &line_doc_;
      }

      const auto& obj = *obj_ptr;
      for (size_t col = 0; col < selected_names.size(); ++col) {
        const auto& name = selected_names[col];
        if (!obj.HasMember(name.c_str())) {
          THROW_SCHEMA_MISMATCH(
              "Column '" + name +
              "' not found in JSON object in file: " + file_path_);
        }
        builders[col]->push_back_elem(
            parse_json_value(obj[name.c_str()], selected_types[col]));
      }
      ++rows_in_chunk;
    }

    if (rows_in_chunk == 0) {
      return nullptr;
    }

    auto chunk = std::make_shared<execution::DataChunk>();
    for (size_t col = 0; col < builders.size(); ++col) {
      chunk->set(static_cast<int>(col), builders[col]->finish());
    }
    return chunk;
  }

  int64_t RowNum() const override { return row_num_; }

 private:
  std::string file_path_;
  JsonReadConfig config_;
  int64_t row_num_ = 0;
  size_t chunk_size_ = kDefaultJsonChunkRows;
  rapidjson::Document document_;
  rapidjson::SizeType doc_index_ = 0;
  rapidjson::Document line_doc_;  // reusable parse buffer for JSONL mode
  std::unique_ptr<std::ifstream> input_;
};

JsonReadConfig read_config_for_supplier(const JsonReadConfig& config) {
  JsonReadConfig read_config = config;
  read_config.include_columns = config.column_names;
  return read_config;
}

}  // namespace

JsonReader::JsonReader(std::shared_ptr<ReadSharedState> sharedState,
                       std::unique_ptr<JsonOptionsBuilder> optionsBuilder)
    : sharedState_(std::move(sharedState)),
      optionsBuilder_(std::move(optionsBuilder)) {}

JsonReader::~JsonReader() = default;

void JsonReader::read(std::shared_ptr<ReadLocalState> /*localState*/,
                      execution::Context& ctx) {
  if (!sharedState_ || !optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("JsonReader state or builder is null");
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
    suppliers.push_back(std::make_shared<JsonChunkSupplier>(path, read_config));
  }

  if (use_batch_read && !sharedState_->skipRows) {
    batch_read(suppliers, ctx);
  } else {
    full_read(suppliers, ctx, config);
  }
}

void JsonReader::full_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output, const JsonReadConfig& output_config) {
  auto merged = read_all_chunks(suppliers);

  int expected_cols = sharedState_->columnNum();
  if (expected_cols > 0 &&
      static_cast<int>(merged.col_num()) != expected_cols &&
      sharedState_->projectColumns.empty()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and JSON data, schema: " +
        std::to_string(expected_cols) +
        ", data: " + std::to_string(merged.col_num()));
  }

  auto filtered =
      filter_chunk(merged, sharedState_->skipRows, output_config.column_names);
  auto projected = project_chunk(filtered, output_config.column_names,
                                 sharedState_->projectColumns.empty()
                                     ? output_config.include_columns
                                     : sharedState_->projectColumns);
  output.clear();
  output.append_chunk(std::move(projected));
}

void JsonReader::batch_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output) {
  output.clear();
  for (const auto& supplier : suppliers) {
    while (auto chunk = supplier->GetNextChunk()) {
      output.append_chunk(std::move(*chunk));
    }
  }
}

result<std::shared_ptr<EntrySchema>> JsonReader::inferSchema() {
  if (!sharedState_ || !optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "JsonReader state or builder is null");
  }

  auto config = optionsBuilder_->build();
  const auto& paths = sharedState_->schema.file.paths;
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  JsonReadConfig sniff_config = config;
  sniff_config.include_columns.clear();
  if (config.column_names.empty()) {
    std::string sample_line;
    if (config.json_array_input) {
      auto lines = convert_json_array_to_lines(paths[0]);
      if (lines.empty()) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                            "Empty JSON array in file: " + paths[0]);
      }
      sample_line = lines.front();
    } else {
      std::ifstream input(paths[0]);
      while (std::getline(input, sample_line)) {
        if (!sample_line.empty()) {
          break;
        }
      }
    }
    rapidjson::Document doc;
    doc.Parse(sample_line.c_str(), sample_line.size());
    if (doc.HasParseError() || !doc.IsObject()) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to parse JSON object for schema inference");
    }
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
      sniff_config.column_names.push_back(it->name.GetString());
    }
  }

  sniff_config.include_columns = sniff_config.column_names;
  for (const auto& name : sniff_config.column_names) {
    sniff_config.column_types[name] = DataType(DataTypeId::kVarchar);
  }

  // Read sample rows directly via rapidjson to infer types without forcing
  // them through VARCHAR-typed parsing.
  std::vector<std::string> sample_lines;
  const size_t max_sample_rows = 64;
  if (config.json_array_input) {
    auto all_lines = convert_json_array_to_lines(paths[0]);
    for (size_t i = 0; i < std::min(all_lines.size(), max_sample_rows); ++i) {
      sample_lines.push_back(std::move(all_lines[i]));
    }
  } else {
    std::ifstream input(paths[0]);
    std::string line;
    while (std::getline(input, line) && sample_lines.size() < max_sample_rows) {
      if (!line.empty()) {
        sample_lines.push_back(std::move(line));
      }
    }
  }
  if (sample_lines.empty()) {
    // No data rows — default all columns to VARCHAR.
    auto entrySchema = std::make_shared<TableEntrySchema>();
    entrySchema->columnNames = sniff_config.column_names;
    entrySchema->columnTypes.reserve(sniff_config.column_names.size());
    NeuGTypeConverter converter;
    for (size_t i = 0; i < sniff_config.column_names.size(); ++i) {
      entrySchema->columnTypes.push_back(
          converter.inferCommonType(DataType(DataTypeId::kVarchar)));
    }
    return entrySchema;
  }

  const auto& col_names = sniff_config.column_names;
  // Track per-column type flags.
  std::vector<bool> all_int(col_names.size(), true);
  std::vector<bool> all_double(col_names.size(), true);
  std::vector<bool> all_bool(col_names.size(), true);
  std::vector<bool> all_datetime(col_names.size(), true);
  std::vector<bool> all_date(col_names.size(), true);
  std::vector<bool> all_date_or_datetime(col_names.size(), true);
  std::vector<bool> any_datetime(col_names.size(), false);

  std::vector<bool> has_value(col_names.size(), false);

  // Helper lambdas for temporal detection on strings.
  auto is_date_str = [](const std::string& s) -> bool {
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
  auto is_datetime_str = [](const std::string& s) -> bool {
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

  for (const auto& line : sample_lines) {
    rapidjson::Document doc;
    doc.Parse(line.c_str(), line.size());
    if (doc.HasParseError() || !doc.IsObject()) {
      continue;
    }
    for (size_t col = 0; col < col_names.size(); ++col) {
      if (!doc.HasMember(col_names[col].c_str())) {
        continue;
      }
      const auto& val = doc[col_names[col].c_str()];
      if (val.IsNull()) {
        continue;
      }
      has_value[col] = true;
      // Integer check (must fit in int64_t)
      if (!val.IsInt64()) {
        all_int[col] = false;
      }
      // Double check
      if (!val.IsNumber()) {
        all_double[col] = false;
      }
      // Bool check
      if (!val.IsBool()) {
        all_bool[col] = false;
      }
      // Temporal checks (only for string values)
      if (val.IsString()) {
        std::string sv(val.GetString(), val.GetStringLength());
        bool is_dt = is_datetime_str(sv);
        bool is_d = is_date_str(sv);
        if (!is_dt)
          all_datetime[col] = false;
        if (!is_d)
          all_date[col] = false;
        if (!is_dt && !is_d)
          all_date_or_datetime[col] = false;
        if (is_dt)
          any_datetime[col] = true;
      } else {
        all_datetime[col] = false;
        all_date[col] = false;
        all_date_or_datetime[col] = false;
      }
    }
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  entrySchema->columnNames = col_names;
  entrySchema->columnTypes.reserve(col_names.size());

  NeuGTypeConverter converter;
  for (size_t col = 0; col < col_names.size(); ++col) {
    DataType inferred_type(DataTypeId::kVarchar);
    if (has_value[col] && all_int[col]) {
      inferred_type = DataType(DataTypeId::kInt64);
    } else if (has_value[col] && all_double[col]) {
      inferred_type = DataType(DataTypeId::kDouble);
    } else if (has_value[col] && all_bool[col]) {
      inferred_type = DataType(DataTypeId::kBoolean);
    } else if (has_value[col] && all_datetime[col]) {
      inferred_type = DataType(DataTypeId::kTimestampMs);
    } else if (has_value[col] && all_date_or_datetime[col] &&
               any_datetime[col]) {
      inferred_type = DataType(DataTypeId::kTimestampMs);
    } else if (has_value[col] && all_date[col]) {
      inferred_type = DataType(DataTypeId::kDate);
    }
    entrySchema->columnTypes.push_back(
        converter.inferCommonType(inferred_type));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace neug
