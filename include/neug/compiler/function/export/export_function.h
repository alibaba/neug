/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/function.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace function {
struct ExportFuncBindData {
  std::vector<std::string> columnNames;
  std::vector<common::LogicalType> types;
  std::string fileName;
  common::case_insensitive_map_t<common::Value> options;

  ExportFuncBindData(
      std::vector<std::string> columnNames, std::string fileName,
      const common::case_insensitive_map_t<common::Value>& options)
      : columnNames{std::move(columnNames)},
        fileName{std::move(fileName)},
        options{options} {}

  virtual ~ExportFuncBindData() = default;

  void setDataType(std::vector<common::LogicalType> types_) {
    types = std::move(types_);
  }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  virtual std::unique_ptr<ExportFuncBindData> copy() const {
    auto bindData =
        std::make_unique<ExportFuncBindData>(columnNames, fileName, options);
    bindData->types = common::LogicalType::copy(types);
    return bindData;
  };
};

struct ExportFuncBindInput {
  std::vector<std::string> columnNames;
  std::string filePath;
  common::case_insensitive_map_t<common::Value> parsingOptions;
};
using export_bind_t = std::function<std::unique_ptr<ExportFuncBindData>(
    function::ExportFuncBindInput& bindInput)>;
using write_exec_func_t = std::function<execution::Context(
    execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const StorageReadInterface& graph)>;

struct NEUG_API ExportFunction : public Function {
  explicit ExportFunction(std::string name) : Function{std::move(name), {}} {}

  export_bind_t bind;
  write_exec_func_t execFunc;
};

struct ExportCSVFunction : public ExportFunction {
  static constexpr const char* name = "COPY_CSV";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
