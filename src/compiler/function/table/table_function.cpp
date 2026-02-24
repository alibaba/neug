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

#include "neug/compiler/function/table/table_function.h"

#include "neug/compiler/parser/query/reading_clause/yield_variable.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/sip/logical_semi_masker.h"
#include "neug/compiler/planner/planner.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::planner;
using namespace neug::processor;

namespace neug {
namespace function {

void TableFuncOutput::resetState() {
  dataChunk.state->getSelVectorUnsafe().setSelSize(0);
  dataChunk.resetAuxiliaryBuffer();
  for (auto i = 0u; i < dataChunk.getNumValueVectors(); i++) {
    dataChunk.getValueVectorMutable(i).setAllNonNull();
  }
}

void TableFuncOutput::setOutputSize(offset_t size) const {
  dataChunk.state->getSelVectorUnsafe().setToUnfiltered(size);
}

TableFunction::~TableFunction() = default;

std::unique_ptr<TableFuncLocalState> TableFunction::initEmptyLocalState(
    const TableFuncInitLocalStateInput&) {
  return std::make_unique<TableFuncLocalState>();
}

std::unique_ptr<TableFuncSharedState> TableFunction::initEmptySharedState(
    const TableFuncInitSharedStateInput& /*input*/) {
  return std::make_unique<TableFuncSharedState>();
}

std::unique_ptr<TableFuncOutput> TableFunction::initSingleDataChunkScanOutput(
    const TableFuncInitOutputInput& input) {
  if (input.outColumnPositions.empty()) {
    return std::make_unique<TableFuncOutput>(DataChunk{});
  }
  auto state =
      input.resultSet.getDataChunk(input.outColumnPositions[0].dataChunkPos)
          ->state;
  auto dataChunk = DataChunk(input.outColumnPositions.size(), state);
  for (auto i = 0u; i < input.outColumnPositions.size(); ++i) {
    dataChunk.insert(
        i, input.resultSet.getValueVector(input.outColumnPositions[i]));
  }
  return std::make_unique<TableFuncOutput>(std::move(dataChunk));
}

std::vector<std::string> TableFunction::extractYieldVariables(
    const std::vector<std::string>& names,
    const std::vector<parser::YieldVariable>& yieldVariables) {
  std::vector<std::string> variableNames;
  if (!yieldVariables.empty()) {
    if (yieldVariables.size() < names.size()) {
      THROW_BINDER_EXCEPTION(
          "Output variables must all appear in the yield clause.");
    }
    if (yieldVariables.size() > names.size()) {
      THROW_BINDER_EXCEPTION(
          "The number of variables in the yield clause exceeds the "
          "number of output variables of the table function.");
    }
    for (auto i = 0u; i < names.size(); i++) {
      if (names[i] != yieldVariables[i].name) {
        THROW_BINDER_EXCEPTION(
            stringFormat("Unknown table function output variable name: {}.",
                         yieldVariables[i].name));
      }
      auto variableName = yieldVariables[i].hasAlias() ? yieldVariables[i].alias
                                                       : yieldVariables[i].name;
      variableNames.push_back(variableName);
    }
  } else {
    variableNames = names;
  }
  return variableNames;
}

void TableFunction::getLogicalPlan(
    planner::Planner* planner,
    const binder::BoundReadingClause& boundReadingClause,
    binder::expression_vector predicates,
    std::vector<std::unique_ptr<planner::LogicalPlan>>& plans) {
  for (auto& plan : plans) {
    auto op = planner->getTableFunctionCall(boundReadingClause);
    planner->planReadOp(op, predicates, *plan);
  }
}

offset_t TableFunction::emptyTableFunc(const TableFuncInput&,
                                       TableFuncOutput&) {
  return 0;
}

}  // namespace function
}  // namespace neug
