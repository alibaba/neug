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

#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/value/nested.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/gds/gds.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/standalone_call_function.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/compiler/parser/parser.h"
#include "neug/utils/exception/exception.h"

using namespace gs::binder;
using namespace gs::common;
using namespace gs::catalog;
using namespace gs::graph;
using namespace gs::common;

namespace gs {
namespace function {

struct CreateProjectedGraphBindData final : TableFuncBindData {
  std::string graphName;
  std::vector<GraphEntryTableInfo> nodeInfos;
  std::vector<GraphEntryTableInfo> relInfos;

  explicit CreateProjectedGraphBindData(std::string graphName)
      : graphName{std::move(graphName)} {}
  CreateProjectedGraphBindData(std::string graphName,
                               std::vector<GraphEntryTableInfo> nodeInfos,
                               std::vector<GraphEntryTableInfo> relInfos)
      : TableFuncBindData{0},
        graphName{std::move(graphName)},
        nodeInfos{std::move(nodeInfos)},
        relInfos{std::move(relInfos)} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<CreateProjectedGraphBindData>(graphName, nodeInfos,
                                                          relInfos);
  }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
  return 0;
}

static std::vector<std::string> getAsStringVector(const Value& value) {
  std::vector<std::string> result;
  for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
    auto childVal = NestedVal::getChildVal(&value, i);
    childVal->validateType(LogicalTypeID::STRING);
    result.push_back(childVal->getValue<std::string>());
  }
  return result;
}

static std::string getPredicateStr(Value& val) {
  auto& type = val.getDataType();
  if (type.getLogicalTypeID() != LogicalTypeID::STRUCT) {
    THROW_BINDER_EXCEPTION(
        stringFormat("{} has data type {}. STRUCT was expected.",
                     val.toString(), type.toString()));
  }
  for (auto i = 0u; i < StructType::getNumFields(type); i++) {
    auto fieldName = StructType::getField(type, i).getName();
    if (StringUtils::getUpper(fieldName) == "FILTER") {
      const auto childVal = NestedVal::getChildVal(&val, i);
      childVal->validateType(LogicalTypeID::STRING);
      return childVal->getValue<std::string>();
    }
    THROW_BINDER_EXCEPTION(stringFormat(
        "Unrecognized configuration {}. Supported configuration is 'filter'.",
        fieldName));
  }
  return {};
}

static std::vector<GraphEntryTableInfo> extractGraphEntryTableInfos(
    const Value& value) {
  std::vector<GraphEntryTableInfo> infos;
  switch (value.getDataType().getLogicalTypeID()) {
  case LogicalTypeID::LIST: {
    for (auto name : getAsStringVector(value)) {
      infos.emplace_back(name, "" /* empty predicate */);
    }
  } break;
  case LogicalTypeID::STRUCT: {
    for (auto i = 0u; i < StructType::getNumFields(value.getDataType()); ++i) {
      auto& field = StructType::getField(value.getDataType(), i);
      infos.emplace_back(field.getName(),
                         getPredicateStr(*NestedVal::getChildVal(&value, i)));
    }
  } break;
  default:
    THROW_BINDER_EXCEPTION(stringFormat(
        "Input argument {} has data type {}. STRUCT or LIST was expected.",
        value.toString(), value.getDataType().toString()));
  }
  return infos;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext*, const TableFuncBindInput* input) {
  return nullptr;
}

function_set CreateProjectedGraphFunction::getFunctionSet() {
  function_set functionSet;
  auto func = std::make_unique<TableFunction>(
      name, std::vector{LogicalTypeID::STRING, LogicalTypeID::ANY,
                        LogicalTypeID::ANY});
  func->bindFunc = bindFunc;
  func->tableFunc = tableFunc;
  func->initSharedStateFunc = TableFunction::initEmptySharedState;
  func->initLocalStateFunc = TableFunction::initEmptyLocalState;
  func->canParallelFunc = []() { return false; };
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
