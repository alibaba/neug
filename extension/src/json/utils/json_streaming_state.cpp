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

#include "json/json_streaming_state.h"
#include "json/json_converter.h" 
#include "neug/utils/exception/exception.h"
#include <cstring>

namespace gs {
namespace extension {

JsonStreamingState::JsonStreamingState(std::unique_ptr<JsonVFSReader> reader,
                                       const std::vector<PropertyType>& columnTypes)
    : reader_(std::move(reader)),
      columnTypes_(columnTypes),
      buffer_(JsonReaderConstants::BUFFER_SIZE + JsonReaderConstants::PADDING_SIZE),
      bufferSize_(0),
      bufferOffset_(0),
      finished_(false),
      reconstructBuffer_(JsonReaderConstants::BUFFER_SIZE),
      reconstructSize_(0),
      totalRowsParsed_(0) {
  
  // Initialize Arrow Builders
  arrowBuilders_.reserve(columnTypes.size());
  for (const auto& type : columnTypes) {
    arrowBuilders_.push_back(createArrowBuilder(type));
  }
}

JsonStreamingState::~JsonStreamingState() = default;

bool JsonStreamingState::readNextBuffer() {
  if (reader_->isEOF() && reconstructSize_ == 0) {
    finished_ = true;
    return false;
  }
  
  // If there is reconstruct data, copy it to the beginning of the buffer
  if (reconstructSize_ > 0) {
    std::memcpy(buffer_.data(), reconstructBuffer_.data(), reconstructSize_);
  }
  
  // Read new data, append after reconstruct data
  uint64_t bytesRead = reader_->readNextBuffer(
      buffer_.data() + reconstructSize_,
      JsonReaderConstants::BUFFER_SIZE - reconstructSize_);
  
  bufferSize_ = reconstructSize_ + bytesRead;
  bufferOffset_ = 0;
  reconstructSize_ = 0;
  
  // Add padding (required by rapidjson)
  std::memset(buffer_.data() + bufferSize_, 0, JsonReaderConstants::PADDING_SIZE);
  
  return bufferSize_ > 0;
}

uint8_t* JsonStreamingState::findNextNewline(uint8_t* start, uint64_t size) {
  return reinterpret_cast<uint8_t*>(std::memchr(start, '\n', size));
}

bool JsonStreamingState::parseAndAppendJsonObject(const rapidjson::Value& obj) {
  if (!obj.IsObject()) {
    return false;
  }
  
  // Extract field names on first parse
  if (fieldNames_.empty()) {
    for (auto m = obj.MemberBegin(); m != obj.MemberEnd(); ++m) {
      fieldNames_.emplace_back(m->name.GetString());
    }
    
    if (fieldNames_.size() != columnTypes_.size()) {
      THROW_EXTENSION_EXCEPTION(
          "JSONL object field count (" + std::to_string(fieldNames_.size()) +
          ") does not match expected column count (" +
          std::to_string(columnTypes_.size()) + ")");
    }
  }
  
  // Append each field value to the corresponding Builder
  for (size_t i = 0; i < fieldNames_.size(); ++i) {
    const auto& fieldName = fieldNames_[i];
    
    if (!obj.HasMember(fieldName.c_str())) {
      auto status = arrowBuilders_[i]->AppendNull();
      if (!status.ok()) {
        LOG(WARNING) << "Failed to append null: " << status.ToString();
      }
      continue;
    }
    
    const auto& val = obj[fieldName.c_str()];
    appendJsonValueToBuilder(arrowBuilders_[i].get(), val, columnTypes_[i]);
  }
  
  return true;
}

size_t JsonStreamingState::parseCurrentBuffer() {
  if (bufferOffset_ >= bufferSize_) {
    return 0;
  }
  
  size_t rowsParsed = 0;
  
  // JSONL format: parse line by line
  while (bufferOffset_ < bufferSize_) {
    // Skip whitespace characters (empty lines, spaces, etc.)
    while (bufferOffset_ < bufferSize_ && 
           std::isspace(buffer_[bufferOffset_])) {
      bufferOffset_++;
    }
    
    if (bufferOffset_ >= bufferSize_) {
      break;
    }
    
    auto* lineStart = buffer_.data() + bufferOffset_;
    auto remaining = bufferSize_ - bufferOffset_;
    
    // Find the end of the line
    auto* lineEnd = findNextNewline(lineStart, remaining);
    
    if (lineEnd == nullptr) {
      // No newline found, indicating the current line spans the buffer boundary
      // Save the remaining content to reconstructBuffer, which will be concatenated on the next read
      reconstructSize_ = remaining;
      std::memcpy(reconstructBuffer_.data(), lineStart, reconstructSize_);
      bufferOffset_ = bufferSize_;
      break;
    }
    
    auto lineSize = lineEnd - lineStart;
    
    // Skip empty lines
    if (lineSize == 0) {
      bufferOffset_++;
      continue;
    }
    
    // Parse this line as JSON
    rapidjson::Document document;
    document.Parse(reinterpret_cast<char*>(lineStart), lineSize);
    
    if (!document.HasParseError() && document.IsObject()) {
      if (parseAndAppendJsonObject(document)) {
        rowsParsed++;
      }
    } else {
      // Parsing failed, log a warning but continue processing
      LOG(WARNING) << "[JsonScan] Failed to parse JSONL line at buffer offset " 
                   << bufferOffset_ << ", skipping";
    }
    
    // Move to the next line (+1 to skip '\n')
    bufferOffset_ += lineSize + 1;
  }
  
  totalRowsParsed_ += rowsParsed;
  return rowsParsed;
}

size_t JsonStreamingState::readNextBatch() {
  if (finished_) {
    return 0;
  }
  
  // If the current buffer has been fully consumed, read the next one
  if (bufferOffset_ >= bufferSize_) {
    if (!readNextBuffer()) {
      return 0;
    }
  }
  
  // Parse the current buffer
  return parseCurrentBuffer();
}

std::vector<std::shared_ptr<arrow::Array>> JsonStreamingState::finishArrays() {
  std::vector<std::shared_ptr<arrow::Array>> result;
  result.reserve(arrowBuilders_.size());
  
  for (auto& builder : arrowBuilders_) {
    std::shared_ptr<arrow::Array> array;
    auto status = builder->Finish(&array);
    if (!status.ok()) {
      THROW_EXTENSION_EXCEPTION("Failed to finish Arrow array: " + 
                               status.ToString());
    }
    result.push_back(std::move(array)); 
  }
  
  return result;
}

}  // namespace extension
}  // namespace gs