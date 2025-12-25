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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <rapidjson/document.h>
#include <memory>
#include <vector>

#include "json/json_vfs_reader.h"
#include "neug/utils/property/types.h"

namespace gs {
namespace extension {

class JsonStreamingState {
 public:
  JsonStreamingState(std::unique_ptr<JsonVFSReader> reader,
                     const std::vector<DataTypeId>& columnTypes);
  ~JsonStreamingState();

  // Read and parse the next batch of data, directly constructing Arrow Arrays
  size_t readNextBatch();

  // Complete the construction of all Arrow Arrays
  std::vector<std::shared_ptr<arrow::Array>> finishArrays();

  bool isFinished() const { return finished_; }
  size_t getTotalRowsParsed() const { return totalRowsParsed_; }

 private:
  bool readNextBuffer();
  size_t parseCurrentBuffer();
  bool parseAndAppendJsonObject(const rapidjson::Value& obj);
  uint8_t* findNextNewline(uint8_t* start, uint64_t size);

  std::unique_ptr<JsonVFSReader> reader_;
  const std::vector<DataTypeId>& columnTypes_;

  // buffer
  std::vector<uint8_t> buffer_;
  uint64_t bufferSize_;
  uint64_t bufferOffset_;
  bool finished_;

  // Buffer used for reconstruction
  std::vector<uint8_t> reconstructBuffer_;
  uint64_t reconstructSize_;

  // Arrow Builders (one per column)
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> arrowBuilders_;

  std::vector<std::string> fieldNames_;
  size_t totalRowsParsed_;
};

}  // namespace extension
}  // namespace gs