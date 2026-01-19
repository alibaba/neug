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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <algorithm>
#include <unordered_map>

#include "json/json_converter.h"
#include "json/json_scan_function.h"
#include "json/json_streaming_state.h"
#include "json/json_vfs_reader.h"
#include "neug/compiler/gopt/g_vfs_holder.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

using namespace gs::common;
using namespace gs::function;
using namespace gs::runtime;
namespace gs {
namespace extension {

const uint8_t* JsonScanFunction::findNextJsonObjectEnd(
    const uint8_t* ptr, uint64_t size, uint64_t& lineCountInJson) {
  lineCountInJson = 0;
  auto end = ptr + size;

  // Determine processing strategy based on the first character
  switch (*ptr) {
  case '{':
  case '[':
  case '"': {
    // Standard JSON object/array/string, need to track nesting levels
    uint64_t parents = 0;
    while (ptr != end) {
      switch (*ptr++) {
      case '{':
      case '[':
        parents++;
        continue;
      case '}':
      case ']':
        parents--;
        break;
      case '"':
        // Handle string content, skip escaped characters
        while (ptr != end) {
          auto strChar = *ptr++;
          if (strChar == '"') {
            break;
          } else if (strChar == '\\') {
            if (ptr != end) {
              ptr++;  // Skip escaped character
            }
          } else if (strChar == '\n') {
            ++lineCountInJson;
          }
        }
        break;
      case '\n':
        ++lineCountInJson;
        // fall through
      default:
        continue;
      }

      if (parents == 0) {
        break;
      }
    }
    break;
  }
  default: {
    // Special case: JSON array containing JSON values without explicit
    // "parents" For example: [1, 2, 3] or [true, false, null]
    while (ptr != end) {
      switch (*ptr++) {
      case ',':
      case ']':
        ptr--;
        break;
      case '\n':
        ++lineCountInJson;
        // fall through
      default:
        continue;
      }
      break;
    }
  }
  }

  return ptr == end ? nullptr : ptr;
}

std::unique_ptr<JsonScanFuncInput> JsonScanFunction::bindFunc(
    const gs::Schema& schema, const gs::runtime::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  LOG(INFO) << "[JsonScan] Binding function from PhysicalPlan";

  const auto& physical_opr = plan.plan(op_idx);
  if (!physical_opr.opr().has_procedure_call()) {
    THROW_EXTENSION_EXCEPTION("Expected ProcedureCall operator");
  }

  const auto& procedure_call = physical_opr.opr().procedure_call();
  const auto& query = procedure_call.query();

  // Obtain filePath
  std::string filePath;

  if (query.arguments_size() > 0) {
    const auto& first_argument = query.arguments(0);
    if (first_argument.has_const_() && first_argument.const_().item_case() ==
                                           ::common::Value::ItemCase::kStr) {
      filePath = first_argument.const_().str();
    }
  }

  // Obtain columnTypes from MetaData
  std::vector<DataTypeId> columnTypes;
  const auto& metadatas = physical_opr.meta_data();
  for (const auto& metadata : metadatas) {
    DataTypeId type;
    if (!gs::data_type_to_property_type(metadata.type().data_type(), type)) {
      THROW_EXTENSION_EXCEPTION("Unrecognized data type: " +
                                metadata.type().DebugString());
    }
    columnTypes.push_back(type);
  }

  // Validate file exists
  auto* vfs = VFSHolder::getVFS();
  if (!vfs->fileOrPathExists(filePath, nullptr)) {
    THROW_EXTENSION_EXCEPTION("JSON file does not exist: " + filePath);
  }

  auto detectResult = autoDetect(filePath, vfs);
  // TODO: Use auto-detected schema if columnTypes is empty

  LOG(INFO) << "[JsonScan] Binding complete:";
  LOG(INFO) << "  - File: " << filePath;
  LOG(INFO) << "  - Format: "
            << (detectResult.format == JsonFormat::ARRAY ? "ARRAY" : "JSONL");
  LOG(INFO) << "  - Columns: " << columnTypes.size();
  LOG(INFO) << "  - Auto-detected column types:";
  for (size_t i = 0; i < detectResult.detectedColumnTypes.size(); ++i) {
    LOG(INFO) << "    * " << detectResult.detectedColumnNames[i] << " -> "
              << std::to_string(detectResult.detectedColumnTypes[i]);
  }

  return std::make_unique<JsonScanFuncInput>(filePath, columnTypes,
                                             detectResult.format);
}

Context JsonScanFunction::execFunc(const JsonScanFuncInput& input) {
  LOG(INFO) << "[JsonScan] Executing function for file: " << input.filePath;

  try {
    auto* vfs = VFSHolder::getVFS();

    std::vector<std::shared_ptr<arrow::Array>> arrowArrays;

    if (input.format == JsonFormat::ARRAY) {
      // JSON Array: Read entire file at once
      LOG(INFO) << "[JsonScan] Using one-shot loading for ARRAY format";
      arrowArrays = parseJsonFile(input.filePath, input.columnTypes, vfs);
    } else {
      // JSONL: Streaming read + incremental Arrow building
      LOG(INFO) << "[JsonScan] Using streaming for JSONL format";
      arrowArrays = parseJsonFileStreaming(input.filePath, input.columnTypes,
                                           vfs, input.format);
    }

    // Create context and populate with arrow data
    Context ctx;
    auto num_columns = static_cast<int>(arrowArrays.size());
    for (int i = 0; i < num_columns; i++) {
      ArrowArrayContextColumnBuilder column_builder;
      column_builder.push_back(arrowArrays[i]);
      ctx.set(i, column_builder.finish());
    }

    auto num_rows = arrowArrays.empty() ? 0 : arrowArrays[0]->length();
    LOG(INFO) << "[JsonScan] Successfully loaded: " << num_rows << " rows with "
              << num_columns << " columns";

    return ctx;

  } catch (const std::exception& e) {
    LOG(ERROR) << "JSON parsing failed: " << e.what();
    THROW_EXTENSION_EXCEPTION("JSON parsing failed: " + std::string(e.what()));
  }
}

std::vector<std::shared_ptr<arrow::Array>>
JsonScanFunction::parseJsonFileStreaming(
    const std::string& filePath, const std::vector<DataTypeId>& columnTypes,
    VirtualFileSystem* vfs, JsonFormat format) {
  if (format != JsonFormat::NEWLINE_DELIMITED) {
    THROW_EXTENSION_EXCEPTION(
        "parseJsonFileStreaming should only be called for JSONL format");
  }

  // Create reader
  auto reader = std::make_unique<JsonVFSReader>(vfs, filePath);

  // Create streaming state (includes Arrow Builders)
  JsonStreamingState streamingState(std::move(reader), columnTypes);

  // Loop to read, parse, and build Arrow (all-in-one)
  size_t batchCount = 0;
  while (!streamingState.isFinished()) {
    size_t rowsParsed = streamingState.readNextBatch();

    if (rowsParsed == 0) {
      break;
    }

    batchCount++;
    LOG(INFO) << "[JsonScan] Batch " << batchCount << ": parsed " << rowsParsed
              << " rows";
  }

  LOG(INFO) << "[JsonScan] Streaming complete: total "
            << streamingState.getTotalRowsParsed() << " rows in " << batchCount
            << " batches";

  // Finish building all Arrow Arrays
  return streamingState.finishArrays();
}

std::vector<std::shared_ptr<arrow::Array>> JsonScanFunction::parseJsonFile(
    const std::string& filePath, const std::vector<DataTypeId>& columnTypes,
    VirtualFileSystem* vfs) {
  // Read entire file at once
  JsonVFSReader reader(vfs, filePath);
  std::string jsonContent = reader.readAll();

  if (jsonContent.empty()) {
    THROW_EXTENSION_EXCEPTION("JSON file is empty: " + filePath);
  }

  // Parse JSON
  rapidjson::Document document;
  document.Parse(jsonContent.c_str(), jsonContent.size());

  if (document.HasParseError()) {
    THROW_EXTENSION_EXCEPTION(
        "JSON parse error at offset " +
        std::to_string(document.GetErrorOffset()) + ": error code " +
        std::to_string(static_cast<int>(document.GetParseError())));
  }

  if (!document.IsArray()) {
    THROW_EXTENSION_EXCEPTION("Expected JSON array format");
  }

  if (document.Empty()) {
    THROW_EXTENSION_EXCEPTION("JSON array is empty");
  }

  // Extract field names
  const auto& first = document[0];
  if (!first.IsObject()) {
    THROW_EXTENSION_EXCEPTION("Expected JSON object in array");
  }

  std::vector<std::string> fieldNames;
  fieldNames.reserve(first.MemberCount());
  for (auto m = first.MemberBegin(); m != first.MemberEnd(); ++m) {
    fieldNames.emplace_back(m->name.GetString());
  }

  if (fieldNames.size() != columnTypes.size()) {
    THROW_EXTENSION_EXCEPTION("JSON object field count (" +
                              std::to_string(fieldNames.size()) +
                              ") does not match expected column count (" +
                              std::to_string(columnTypes.size()) + ")");
  }

  // Create Arrow Builders (one per column)
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> arrowBuilders;
  arrowBuilders.reserve(columnTypes.size());
  for (const auto& type : columnTypes) {
    arrowBuilders.push_back(createArrowBuilder(type));
  }

  // Parse line by line and append directly to Arrow Builders
  for (const auto& obj : document.GetArray()) {
    if (!obj.IsObject()) {
      // Skip non-object elements
      for (auto& builder : arrowBuilders) {
        auto status = builder->AppendNull();
        if (!status.ok()) {
          LOG(WARNING) << "Failed to append null: " << status.ToString();
        }
      }
      continue;
    }

    // Append each field value to the corresponding Builder
    for (size_t i = 0; i < fieldNames.size(); ++i) {
      const auto& fieldName = fieldNames[i];

      if (!obj.HasMember(fieldName.c_str())) {
        // Field missing, append null
        auto status = arrowBuilders[i]->AppendNull();
        if (!status.ok()) {
          LOG(WARNING) << "Failed to append null: " << status.ToString();
        }
        continue;
      }

      const auto& val = obj[fieldName.c_str()];
      appendJsonValueToBuilder(arrowBuilders[i].get(), val, columnTypes[i]);
    }
  }

  // Finish building all Arrow Arrays
  std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
  arrowArrays.reserve(arrowBuilders.size());

  for (auto& builder : arrowBuilders) {
    std::shared_ptr<arrow::Array> array;
    auto status = builder->Finish(&array);
    if (!status.ok()) {
      THROW_EXTENSION_EXCEPTION("Failed to finish Arrow array: " +
                                status.ToString());
    }
    arrowArrays.push_back(std::move(array));
  }

  return arrowArrays;
}

JsonScanFunction::AutoDetectResult JsonScanFunction::autoDetect(
    const std::string& filePath, common::VirtualFileSystem* vfs,
    size_t maxRowsToDetect) {
  LOG(INFO) << "[JsonScan] Auto-detecting format and schema for: " << filePath;

  AutoDetectResult result;

  // --- Step 1: Detect file format (read only the first buffer) ---
  auto reader = std::make_unique<JsonVFSReader>(vfs, filePath);
  std::vector<uint8_t> sampleBuffer(JsonReaderConstants::BUFFER_SIZE +
                                    JsonReaderConstants::PADDING_SIZE);
  uint64_t bytesRead = reader->readNextBuffer(sampleBuffer.data(),
                                              JsonReaderConstants::BUFFER_SIZE);

  if (bytesRead == 0) {
    LOG(WARNING) << "[JsonScan] File is empty, defaulting to ARRAY format.";
    result.format = JsonFormat::ARRAY;
    return result;
  }
  std::memset(sampleBuffer.data() + bytesRead, 0,
              JsonReaderConstants::PADDING_SIZE);

  result.format = detectFormatFromBuffer(sampleBuffer.data(), bytesRead);
  LOG(INFO) << "[JsonScan] Detected format: "
            << (result.format == JsonFormat::ARRAY ? "ARRAY"
                                                   : "NEWLINE_DELIMITED");

  // --- Step 2: Use streaming read to infer column types ---
  reader = std::make_unique<JsonVFSReader>(vfs, filePath);

  std::vector<uint8_t> buffer(JsonReaderConstants::BUFFER_SIZE +
                              JsonReaderConstants::PADDING_SIZE);
  uint64_t bufferSize =
      reader->readNextBuffer(buffer.data(), JsonReaderConstants::BUFFER_SIZE);

  if (bufferSize == 0) {
    LOG(WARNING) << "[JsonScan] File is empty after format detection";
    return result;
  }
  std::memset(buffer.data() + bufferSize, 0, JsonReaderConstants::PADDING_SIZE);

  std::unordered_map<std::string, DataTypeId> columnTypeMap;
  std::vector<std::string>& columnNamesInOrder = result.detectedColumnNames;
  size_t rowsScanned = 0;

  uint64_t bufferOffset = 0;
  bool isArrayStarted = false;

  while (rowsScanned < maxRowsToDetect && bufferOffset < bufferSize) {
    skipWhitespace(buffer.data(), bufferOffset, bufferSize);
    if (bufferOffset >= bufferSize)
      break;

    uint8_t* objStart = buffer.data() + bufferOffset;
    uint64_t remaining = bufferSize - bufferOffset;
    const uint8_t* objEnd = nullptr;
    uint64_t objSize = 0;

    if (result.format == JsonFormat::ARRAY) {
      // Handle array start
      if (!isArrayStarted) {
        if (*objStart == '[') {
          bufferOffset++;
          skipWhitespace(buffer.data(), bufferOffset, bufferSize);
          isArrayStarted = true;
          continue;
        }
      }

      // Check for array end
      if (*objStart == ']') {
        break;
      }

      // Skip comma
      if (*objStart == ',') {
        bufferOffset++;
        skipWhitespace(buffer.data(), bufferOffset, bufferSize);
        continue;
      }

      // Update pointer and remaining size
      objStart = buffer.data() + bufferOffset;
      remaining = bufferSize - bufferOffset;

      // Use our boundary finding function
      uint64_t lineCountInJson = 0;
      objEnd = findNextJsonObjectEnd(objStart, remaining, lineCountInJson);

      if (objEnd == nullptr) {
        // Object incomplete in buffer
        break;  // For autoDetect, stop directly
      }

      objSize = objEnd - objStart;

    } else {  // NEWLINE_DELIMITED
      auto* lineEnd = nextNewLine(objStart, remaining);
      if (lineEnd == nullptr) {
        break;
      }
      objSize = lineEnd - objStart;
    }

    if (objSize == 0) {
      bufferOffset++;
      continue;
    }

    // Parse the found object
    rapidjson::Document doc;
    doc.Parse(reinterpret_cast<char*>(objStart), objSize);

    if (doc.HasParseError()) {
      LOG(WARNING) << "[JsonScan] Parse error: " << doc.GetParseError()
                   << " at offset " << doc.GetErrorOffset();
      bufferOffset += objSize;
      continue;
    }

    if (!doc.IsObject()) {
      LOG(WARNING) << "[JsonScan] Parsed value is not an object, type="
                   << doc.GetType();
      bufferOffset += objSize;
      continue;
    }

    // Infer types
    for (auto m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
      std::string fieldName = m->name.GetString();
      DataTypeId inferredType = inferPropertyTypeFromValue(m->value);

      auto it = columnTypeMap.find(fieldName);
      if (it == columnTypeMap.end()) {
        columnNamesInOrder.push_back(fieldName);
        columnTypeMap[fieldName] = inferredType;
      } else {
        auto mergedType = mergePropertyTypes(it->second, inferredType);
        if (mergedType != it->second) {
          it->second = mergedType;
        }
      }
    }

    rowsScanned++;
    bufferOffset += objSize;

    if (result.format == JsonFormat::NEWLINE_DELIMITED) {
      bufferOffset++;  // Skip '\n'
    }
  }

  // Build result
  result.detectedColumnTypes.reserve(columnNamesInOrder.size());
  for (const auto& name : columnNamesInOrder) {
    result.detectedColumnTypes.push_back(columnTypeMap[name]);
  }

  return result;
}

JsonFormat JsonScanFunction::detectFormatFromBuffer(uint8_t* bufferPtr,
                                                    uint64_t bufferSize) {
  uint64_t bufferOffset = 0;
  skipWhitespace(bufferPtr, bufferOffset, bufferSize);
  if (bufferOffset >= bufferSize) {
    return JsonFormat::ARRAY;  // empty file, default to Array
  }

  // Check if the first line is a complete JSON
  auto* lineEnd =
      nextNewLine(bufferPtr + bufferOffset, bufferSize - bufferOffset);
  if (lineEnd != nullptr) {
    uint64_t lineSize = (lineEnd - bufferPtr) - bufferOffset;
    rapidjson::Document doc;
    doc.Parse<rapidjson::kParseStopWhenDoneFlag>(
        reinterpret_cast<char*>(bufferPtr + bufferOffset), lineSize);

    if (!doc.HasParseError()) {
      // If the first line is a valid JSON object, consider it JSONL
      if (doc.IsObject()) {
        return JsonFormat::NEWLINE_DELIMITED;
      }
      // If the first line is a valid JSON array and there is no other content,
      // consider it a single-line Array
      if (doc.IsArray()) {
        uint64_t endOfArrayOffset = bufferOffset + doc.GetErrorOffset();
        skipWhitespace(bufferPtr, endOfArrayOffset, bufferSize);
        if (endOfArrayOffset >= bufferSize) {
          return JsonFormat::ARRAY;
        }
      }
    }
  }

  // If the first line is not a complete JSON or cannot be determined, check the
  // first non-whitespace character
  bufferOffset = 0;
  skipWhitespace(bufferPtr, bufferOffset, bufferSize);
  if (bufferOffset >= bufferSize) {
    return JsonFormat::ARRAY;
  }

  if (bufferPtr[bufferOffset] == '{') {
    return JsonFormat::NEWLINE_DELIMITED;
  }

  if (bufferPtr[bufferOffset] != '[') {
    // Neither an object nor an array start, possibly non-standard JSONL
    return JsonFormat::NEWLINE_DELIMITED;
  }

  // Starts with '[', try to parse as array
  rapidjson::Document doc;
  doc.Parse<rapidjson::kParseStopWhenDoneFlag>(
      reinterpret_cast<char*>(bufferPtr + bufferOffset),
      bufferSize - bufferOffset);
  if (!doc.HasParseError() && doc.IsArray()) {
    return JsonFormat::ARRAY;
  }

  // Default case
  return JsonFormat::ARRAY;
}

void JsonScanFunction::skipWhitespace(const uint8_t* bufferPtr,
                                      uint64_t& bufferOffset,
                                      const uint64_t& bufferSize) {
  while (bufferOffset < bufferSize && std::isspace(bufferPtr[bufferOffset])) {
    bufferOffset++;
  }
}

const uint8_t* JsonScanFunction::nextNewLine(const uint8_t* ptr,
                                             uint64_t size) {
  return reinterpret_cast<const uint8_t*>(std::memchr(ptr, '\n', size));
}

}  // namespace extension
}  // namespace gs