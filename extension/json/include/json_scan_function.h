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

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/execution/common/context.h"
#include "neug/utils/property/types.h"
#include "neug/utils/proto/plan/physical.pb.h"

namespace gs {
namespace extension {

struct JsonScanFuncInput : public gs::function::CallFuncInputBase {
  std::string filePath;
  std::vector<PropertyType> columnTypes;

  JsonScanFuncInput(const std::string& path,
                    const std::vector<PropertyType>& types)
      : filePath(path), columnTypes(types) {}
};

struct JsonScanFunction {
  static constexpr const char* name = "JSON_SCAN";

  static gs::function::function_set getFunctionSet() {
    auto function = std::make_unique<gs::function::NeugCallFunction>(
        name, std::vector<gs::common::LogicalTypeID>{
                  gs::common::LogicalTypeID::STRING});

    function->bindFunc =
        [](const gs::Schema& schema, const gs::runtime::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<gs::function::CallFuncInputBase> {
      return bindFunc(schema, ctx_meta, plan, op_idx);
    };

    function->execFunc = [](const gs::function::CallFuncInputBase& input)
        -> gs::runtime::Context {
      const auto& jsonInput = static_cast<const JsonScanFuncInput&>(input);
      return execFunc(jsonInput);
    };

    gs::function::function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
  }

  static std::unique_ptr<JsonScanFuncInput> bindFunc(
      const gs::Schema& schema, const gs::runtime::ContextMeta& ctx_meta,
      const ::physical::PhysicalPlan& plan, int op_idx);

  static gs::runtime::Context execFunc(const JsonScanFuncInput& input);

 private:
  // Helper Functions for JSON parsing
  static std::vector<std::shared_ptr<arrow::Array>> parseJsonFile(
      const std::string& filePath,
      const std::vector<PropertyType>& columnTypes);

  static std::shared_ptr<arrow::Array> convertJsonValue(
      const std::vector<std::string>& jsonValues, PropertyType targetType);
};

}  // namespace extension
}  // namespace gs