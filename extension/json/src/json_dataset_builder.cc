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

#include "json_dataset_builder.h"
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/io/api.h>
#include <arrow/json/options.h>
#include <arrow/json/reader.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <memory>
#include <string>
#include <vector>
#include "json_options.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/service_utils.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::dataset::DatasetFactory> DatasetBuilder::buildFactory(
    std::shared_ptr<ReadSharedState> sharedState,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::FileFormat> fileFormat) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!fs) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }
  if (!fileFormat) {
    THROW_INVALID_ARGUMENT_EXCEPTION("File format is null");
  }

  const auto& fileSchema = sharedState->schema.file;
  const std::vector<std::string>& file_paths = fileSchema.paths;

  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  arrow::dataset::FileSystemFactoryOptions factory_options;
  factory_options.exclude_invalid_files = false;
  auto factory_result = arrow::dataset::FileSystemDatasetFactory::Make(
      fs, file_paths, fileFormat, factory_options);
  if (!factory_result.ok()) {
    THROW_IO_EXCEPTION("Failed to create FileSystemDatasetFactory: " +
                       factory_result.status().message());
  }
  return factory_result.ValueOrDie();
}

/**
 * @brief DatasetFactory for JSON file format
 *
 * This class implements the DatasetFactory interface for parsing JSON ARRAY
 * FORMAT. It reads the JSON file and converts it to a JSONL format, then reads
 * the JSONL content and converts it to a dataset by using arrow table reader.
 * The dataset can be used for filtering push down and column pruning later.
 */
class JsonDatasetFactory : public arrow::dataset::DatasetFactory {
 public:
  JsonDatasetFactory(std::shared_ptr<ReadSharedState> sharedState,
                     std::shared_ptr<arrow::fs::FileSystem> fs,
                     std::shared_ptr<arrow::dataset::FileFormat> fileFormat)
      : sharedState(sharedState), fs(fs), fileFormat(fileFormat) {}
  virtual ~JsonDatasetFactory() = default;

  virtual arrow::Result<std::vector<std::shared_ptr<arrow::Schema>>>
  InspectSchemas(arrow::dataset::InspectOptions options) override;

  virtual arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> Finish(
      arrow::dataset::FinishOptions options) override;

 private:
  std::shared_ptr<ReadSharedState> sharedState;
  std::shared_ptr<arrow::fs::FileSystem> fs;
  std::shared_ptr<arrow::dataset::FileFormat> fileFormat;

 private:
  arrow::Result<std::string> readString(const std::string& path);
  arrow::Result<std::string> convertToJSONL(const std::string& path);
};

arrow::Result<std::string> JsonDatasetFactory::readString(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));
  ARROW_ASSIGN_OR_RAISE(auto file_size, input_file->GetSize());

  std::string content;
  content.resize(file_size);
  ARROW_ASSIGN_OR_RAISE(
      auto bytes_read,
      input_file->Read(file_size, reinterpret_cast<uint8_t*>(content.data())));

  if (bytes_read != file_size) {
    return arrow::Status::IOError("Failed to read entire file: " + path);
  }

  return content;
}

arrow::Result<std::string> JsonDatasetFactory::convertToJSONL(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto jsonContent, readString(path));

  rapidjson::Document document;
  document.Parse(jsonContent.c_str(), jsonContent.size());

  if (document.HasParseError()) {
    return arrow::Status::Invalid(
        "JSON parse error in file " + path + " at offset " +
        std::to_string(document.GetErrorOffset()) + ": " +
        rapidjson::GetParseError_En(document.GetParseError()));
  }

  if (!document.IsArray() || document.Empty()) {
    return arrow::Status::Invalid("Expected non-empty JSON array in file: " +
                                  path);
  }

  // Convert array to JSONL format (one JSON object per line)
  std::string jsonl;
  jsonl.reserve(jsonContent.size());  // Pre-allocate to avoid reallocations

  for (const auto& obj : document.GetArray()) {
    if (!obj.IsObject()) {
      return arrow::Status::Invalid("Expected JSON object in array but got " +
                                    rapidjson_stringify(obj) +
                                    " in file: " + path);
    }
    // Serialize each object to a single line
    jsonl += rapidjson_stringify(obj);
    jsonl += '\n';
  }

  return jsonl;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Schema>>>
JsonDatasetFactory::InspectSchemas(arrow::dataset::InspectOptions) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  auto& file = sharedState->schema.file;
  if (file.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  // Infer schema from the first file only
  auto path = file.paths[0];
  ARROW_ASSIGN_OR_RAISE(auto jsonl, convertToJSONL(path));

  // Create buffer from JSONL content
  auto buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t*>(jsonl.data()), jsonl.size());

  auto input = std::make_shared<arrow::io::BufferReader>(buffer);

  // Configure JSON read options
  arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();
  ReadOptions readOpts;
  auto& options = file.options;
  read_options.use_threads = readOpts.use_threads.get(options);
  read_options.block_size = readOpts.batch_size.get(options);

  // Configure JSON parse options
  arrow::json::ParseOptions parse_options =
      arrow::json::ParseOptions::Defaults();
  JsonParseOptions parseOpts;
  parse_options.newlines_in_values = parseOpts.newlines_in_values.get(options);

  // Create TableReader and read schema
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::json::TableReader::Make(
                                         arrow::default_memory_pool(), input,
                                         read_options, parse_options));

  ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());

  // Return schema as vector (required by DatasetFactory interface)
  std::vector<std::shared_ptr<arrow::Schema>> schemas;
  schemas.push_back(table->schema());
  return schemas;
}

arrow::Result<std::shared_ptr<arrow::dataset::Dataset>>
JsonDatasetFactory::Finish(arrow::dataset::FinishOptions) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }

  auto& file = sharedState->schema.file;
  if (file.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  if (!sharedState->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }

  // Configure JSON read options
  arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();
  ReadOptions readOpts;
  auto& options = file.options;
  read_options.use_threads = readOpts.use_threads.get(options);
  read_options.block_size = readOpts.batch_size.get(options);

  // Configure JSON parse options
  arrow::json::ParseOptions parse_options =
      arrow::json::ParseOptions::Defaults();
  JsonParseOptions parseOpts;
  parse_options.newlines_in_values = parseOpts.newlines_in_values.get(options);

  auto& entry = sharedState->schema.entry;
  if (!entry->columnNames.empty() && !entry->columnTypes.empty()) {
    parse_options.explicit_schema = createSchema(*entry);
  }

  std::vector<std::shared_ptr<arrow::Table>> tables;

  // Read all files and convert to tables
  for (const auto& path : file.paths) {
    ARROW_ASSIGN_OR_RAISE(auto jsonl, convertToJSONL(path));

    // Create buffer from JSONL content
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(jsonl.data()), jsonl.size());

    auto input = std::make_shared<arrow::io::BufferReader>(buffer);

    // Create TableReader and read table
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::json::TableReader::Make(
                                           arrow::default_memory_pool(), input,
                                           read_options, parse_options));

    ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());
    tables.push_back(table);
  }

  ARROW_ASSIGN_OR_RAISE(
      auto combinedTable,
      arrow::ConcatenateTables(tables,
                               arrow::ConcatenateTablesOptions::Defaults()));

  // Create InMemoryDataset from combined table
  auto dataset =
      std::make_shared<arrow::dataset::InMemoryDataset>(combinedTable);
  return dataset;
}

std::shared_ptr<arrow::dataset::DatasetFactory>
JsonDatasetBuilder::buildFactory(
    std::shared_ptr<ReadSharedState> sharedState,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::FileFormat> fileFormat) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  auto& file = sharedState->schema.file;
  JsonParseOptions jsonOpts;
  // if json format is newline_delimited, use the default buildFactory
  if (jsonOpts.newline_delimited.get(file.options)) {
    return DatasetBuilder::buildFactory(sharedState, fs, fileFormat);
  }
  // For JSON_ARRAY format, use custom JsonDatasetFactory
  return std::make_shared<JsonDatasetFactory>(sharedState, fs, fileFormat);
}

}  // namespace reader
}  // namespace neug