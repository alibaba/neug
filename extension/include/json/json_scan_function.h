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

#include <memory>
#include <string>
#include <vector>

#include "json_vfs_reader.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace extension {

struct JsonScanFuncInput : public neug::function::CallFuncInputBase {
  std::string filePath;
  std::vector<DataTypeId> columnTypes;
  JsonFormat format;

  JsonScanFuncInput(const std::string& path,
                    const std::vector<DataTypeId>& types, JsonFormat fmt)
      : filePath(std::move(path)), columnTypes(std::move(types)), format(fmt) {}
};

struct JsonScanFunction {
  static constexpr const char* name = "JSON_SCAN";

  static neug::function::function_set getFunctionSet() {
    auto function = std::make_unique<neug::function::NeugCallFunction>(
        name, std::vector<neug::common::LogicalTypeID>{
                  neug::common::LogicalTypeID::STRING});

    function->bindFunc =
        [](const neug::Schema& schema, const neug::runtime::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<neug::function::CallFuncInputBase> {
      return bindFunc(schema, ctx_meta, plan, op_idx);
    };

    function->execFunc = [](const neug::function::CallFuncInputBase& input)
        -> neug::runtime::Context {
      const auto& jsonInput = static_cast<const JsonScanFuncInput&>(input);
      return execFunc(jsonInput);
    };

    neug::function::function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
  }

  static std::unique_ptr<JsonScanFuncInput> bindFunc(
      const neug::Schema& schema, const neug::runtime::ContextMeta& ctx_meta,
      const ::physical::PhysicalPlan& plan, int op_idx);

  static neug::runtime::Context execFunc(const JsonScanFuncInput& input);

 private:
  struct AutoDetectResult {
    JsonFormat format;
    std::vector<DataTypeId> detectedColumnTypes;
    std::vector<std::string> detectedColumnNames;
  };

  static AutoDetectResult autoDetect(const std::string& filePath,
                                     common::VirtualFileSystem* vfs,
                                     size_t maxRowsToDetect = 2048);

  static JsonFormat detectFormatFromBuffer(uint8_t* bufferPtr,
                                           uint64_t bufferSize);
  static void skipWhitespace(const uint8_t* bufferPtr, uint64_t& bufferOffset,
                             const uint64_t& bufferSize);
  static const uint8_t* nextNewLine(const uint8_t* ptr, uint64_t size);
  static const uint8_t* findNextJsonObjectEnd(const uint8_t* ptr, uint64_t size,
                                              uint64_t& lineCountInJson);

  static std::vector<std::shared_ptr<arrow::Array>> parseJsonFileStreaming(
      const std::string& filePath, const std::vector<DataTypeId>& columnTypes,
      common::VirtualFileSystem* vfs, JsonFormat format);

  static std::vector<std::shared_ptr<arrow::Array>> parseJsonFile(
      const std::string& filePath, const std::vector<DataTypeId>& columnTypes,
      common::VirtualFileSystem* vfs);
};

}  // namespace extension
}  // namespace neug