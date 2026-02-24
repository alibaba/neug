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
#include "neug/compiler/common/copier_config/csv_reader_config.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct ExportFuncLocalState {
  virtual ~ExportFuncLocalState() = default;

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
};

struct ExportFuncBindData;

struct ExportFuncSharedState {
  virtual ~ExportFuncSharedState() = default;

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  virtual void init(main::ClientContext& context,
                    const ExportFuncBindData& bindData) = 0;
};

struct ExportFuncBindData {
  std::vector<std::string> columnNames;
  std::vector<common::LogicalType> types;
  std::string fileName;

  ExportFuncBindData(std::vector<std::string> columnNames, std::string fileName)
      : columnNames{std::move(columnNames)}, fileName{std::move(fileName)} {}

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
    auto bindData = std::make_unique<ExportFuncBindData>(columnNames, fileName);
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
using export_init_local_t = std::function<std::unique_ptr<ExportFuncLocalState>(
    main::ClientContext&, const ExportFuncBindData&, std::vector<bool>)>;
using export_create_shared_t =
    std::function<std::shared_ptr<ExportFuncSharedState>()>;
using export_init_shared_t = std::function<void(
    ExportFuncSharedState&, main::ClientContext&, ExportFuncBindData&)>;
using export_sink_t = std::function<void(
    ExportFuncSharedState&, ExportFuncLocalState&, const ExportFuncBindData&,
    std::vector<std::shared_ptr<common::ValueVector>>)>;
using export_combine_t =
    std::function<void(ExportFuncSharedState&, ExportFuncLocalState&)>;
using export_finalize_t = std::function<void(ExportFuncSharedState&)>;

struct NEUG_API ExportFunction : public Function {
  explicit ExportFunction(std::string name) : Function{std::move(name), {}} {}

  ExportFunction(std::string name, export_init_local_t initLocal,
                 export_create_shared_t createShared,
                 export_init_shared_t initShared, export_sink_t copyToSink,
                 export_combine_t copyToCombine,
                 export_finalize_t copyToFinalize)
      : Function{std::move(name), {}},
        initLocalState{std::move(initLocal)},
        createSharedState{std::move(createShared)},
        initSharedState{std::move(initShared)},
        sink{std::move(copyToSink)},
        combine{std::move(copyToCombine)},
        finalize{std::move(copyToFinalize)} {}

  export_bind_t bind;
  export_init_local_t initLocalState;
  export_create_shared_t createSharedState;
  export_init_shared_t initSharedState;
  export_sink_t sink;
  export_combine_t combine;
  export_finalize_t finalize;
};

struct ExportCSVFunction : public ExportFunction {
  static constexpr const char* name = "COPY_CSV";

  static function_set getFunctionSet();
};

struct ExportParquetFunction : public ExportFunction {
  static constexpr const char* name = "COPY_PARQUET";

  static function_set getFunctionSet();
};

struct ExportCSVBindData : public ExportFuncBindData {
  common::CSVOption exportOption;

  ExportCSVBindData(std::vector<std::string> names, std::string fileName,
                    common::CSVOption exportOption)
      : ExportFuncBindData{std::move(names), std::move(fileName)},
        exportOption{std::move(exportOption)} {}

  std::unique_ptr<ExportFuncBindData> copy() const override {
    auto bindData = std::make_unique<ExportCSVBindData>(columnNames, fileName,
                                                        exportOption.copy());
    bindData->types = common::LogicalType::copy(types);
    return bindData;
  }
};

}  // namespace function
}  // namespace neug
