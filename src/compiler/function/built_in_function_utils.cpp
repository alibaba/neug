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

#include "neug/compiler/function/built_in_function_utils.h"

#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/function/aggregate_function.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace function {

static void validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunction*>& candidateFunctions,
    const std::string& name, const std::vector<DataType>& inputTypes,
    bool isDistinct, const function::function_set& set);
static void validateNonEmptyCandidateFunctions(
    std::vector<Function*>& candidateFunctions, const std::string& name,
    const std::vector<DataType>& inputTypes, const function::function_set& set);

Function* BuiltInFunctionsUtils::matchFunction(
    const std::string& name, const std::vector<DataType>& inputTypes,
    const catalog::FunctionCatalogEntry* functionEntry) {
  auto& functionSet = functionEntry->getFunctionSet();
  std::vector<Function*> candidateFunctions;
  uint32_t minCost = UINT32_MAX;
  for (auto& function : functionSet) {
    auto func = function.get();
    auto cost = getFunctionCost(inputTypes, func);
    if (cost == UINT32_MAX) {
      continue;
    }
    if (cost < minCost) {
      candidateFunctions.clear();
      candidateFunctions.push_back(func);
      minCost = cost;
    } else if (cost == minCost) {
      candidateFunctions.push_back(func);
    }
  }
  validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes,
                                     functionSet);
  if (candidateFunctions.size() > 1) {
    return getBestMatch(candidateFunctions);
  }
  validateSpecialCases(candidateFunctions, name, inputTypes, functionSet);
  return candidateFunctions[0];
}

AggregateFunction* BuiltInFunctionsUtils::matchAggregateFunction(
    const std::string& name, const std::vector<common::DataType>& inputTypes,
    bool isDistinct, const catalog::FunctionCatalogEntry* functionEntry) {
  auto& functionSet = functionEntry->getFunctionSet();
  std::vector<AggregateFunction*> candidateFunctions;
  for (auto& function : functionSet) {
    auto aggregateFunction = function->ptrCast<AggregateFunction>();
    auto cost =
        getAggregateFunctionCost(inputTypes, isDistinct, aggregateFunction);
    if (cost == UINT32_MAX) {
      continue;
    }
    candidateFunctions.push_back(aggregateFunction);
  }
  validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes,
                                     isDistinct, functionSet);
  NEUG_ASSERT(candidateFunctions.size() == 1);
  return candidateFunctions[0];
}

uint32_t BuiltInFunctionsUtils::getCastCost(DataTypeId inputTypeID,
                                            DataTypeId targetTypeID) {
  if (inputTypeID == targetTypeID) {
    return 0;
  }
  if (inputTypeID == DataTypeId::kUnknown ||
      targetTypeID == DataTypeId::kUnknown) {
    return 1;
  }
  if (targetTypeID == DataTypeId::kVarchar) {
    return castFromString(inputTypeID);
  }
  switch (inputTypeID) {
  case DataTypeId::kInt64:
    return castInt64(targetTypeID);
  case DataTypeId::kInt32:
    return castInt32(targetTypeID);
  case DataTypeId::kInt16:
    return castInt16(targetTypeID);
  case DataTypeId::kInt8:
    return castInt8(targetTypeID);
  case DataTypeId::kUInt64:
    return castUInt64(targetTypeID);
  case DataTypeId::kUInt32:
    return castUInt32(targetTypeID);
  case DataTypeId::kUInt16:
    return castUInt16(targetTypeID);
  case DataTypeId::kUInt8:
    return castUInt8(targetTypeID);
  // INT128 removed — no engine equivalent
  case DataTypeId::kDouble:
    return castDouble(targetTypeID);
  case DataTypeId::kFloat:
    return castFloat(targetTypeID);
  case DataTypeId::kDate:
    return castDate(targetTypeID);
  case DataTypeId::kTimestampMs:
    return castTimestamp(targetTypeID);
  case DataTypeId::kList:
    return castList(targetTypeID);
  case DataTypeId::kArray:
    return castArray(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::getTargetTypeCost(DataTypeId typeID) {
  switch (typeID) {
  case DataTypeId::kInt16:
    return 100;
  case DataTypeId::kInt64:
    return 101;
  case DataTypeId::kInt32:
    return 102;
  case DataTypeId::kDouble:
    return 105;
  case DataTypeId::kTimestampMs:
    return 120;
  case DataTypeId::kVarchar:
    return 149;
  case DataTypeId::kStruct:
  case DataTypeId::kMap:
  case DataTypeId::kArray:
  case DataTypeId::kList:
    return 160;
  default:
    return 110;
  }
}

uint32_t BuiltInFunctionsUtils::castInt64(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castInt32(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castInt16(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castInt8(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt16:
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castUInt64(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castUInt32(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt64:
  case DataTypeId::kUInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castUInt16(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castUInt8(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kInt16:
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt64:
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castInt128(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kFloat:
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castDouble(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castFloat(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kDouble:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castDate(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kTimestampMs:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castTimestamp(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kTimestampMs:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castFromString(DataTypeId inputTypeID) {
  switch (inputTypeID) {
  case DataTypeId::kInternalId:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
    return UNDEFINED_CAST_COST;
  default:
    return getTargetTypeCost(DataTypeId::kVarchar);
  }
}

uint32_t BuiltInFunctionsUtils::castList(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kArray:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

uint32_t BuiltInFunctionsUtils::castArray(DataTypeId targetTypeID) {
  switch (targetTypeID) {
  case DataTypeId::kList:
    return getTargetTypeCost(targetTypeID);
  default:
    return UNDEFINED_CAST_COST;
  }
}

Function* BuiltInFunctionsUtils::getBestMatch(
    std::vector<Function*>& functionsToMatch) {
  NEUG_ASSERT(functionsToMatch.size() > 1);
  Function* result = nullptr;
  auto cost = UNDEFINED_CAST_COST;
  for (auto& function : functionsToMatch) {
    auto currentCost = 0u;
    std::unordered_set<DataTypeId> distinctParameterTypes;
    for (auto& parameterType : function->parameterTypes) {
      if (parameterType.id() != DataTypeId::kVarchar) {
        currentCost++;
      }
      auto parameterTypeID = parameterType.id();
      if (!distinctParameterTypes.contains(parameterTypeID)) {
        currentCost++;
        distinctParameterTypes.insert(parameterTypeID);
      }
    }
    if (currentCost < cost) {
      cost = currentCost;
      result = function;
    }
  }
  NEUG_ASSERT(result != nullptr);
  return result;
}

uint32_t BuiltInFunctionsUtils::getFunctionCost(
    const std::vector<DataType>& inputTypes, Function* function) {
  if (function->isVarLength) {
    NEUG_ASSERT(function->parameterTypes.size() == 1);
    return matchVarLengthParameters(inputTypes,
                                    function->parameterTypes[0].id());
  }
  return matchParameters(inputTypes, function->parameterTypes);
}

uint32_t BuiltInFunctionsUtils::getAggregateFunctionCost(
    const std::vector<DataType>& inputTypes, bool isDistinct,
    AggregateFunction* function) {
  if (inputTypes.size() != function->parameterTypes.size() ||
      isDistinct != function->isDistinct) {
    return UINT32_MAX;
  }
  for (auto i = 0u; i < inputTypes.size(); ++i) {
    if (function->parameterTypes[i].id() == DataTypeId::kUnknown) {
      continue;
    } else if (inputTypes[i].id() != function->parameterTypes[i].id()) {
      return UINT32_MAX;
    }
  }
  return 0;
}

uint32_t BuiltInFunctionsUtils::matchParameters(
    const std::vector<DataType>& inputTypes,
    const std::vector<DataType>& targetTypes) {
  if (inputTypes.size() != targetTypes.size()) {
    return UINT32_MAX;
  }
  auto cost = 0u;
  for (auto i = 0u; i < inputTypes.size(); ++i) {
    auto castCost = getCastCost(inputTypes[i].id(), targetTypes[i].id());
    if (castCost == UNDEFINED_CAST_COST) {
      return UINT32_MAX;
    }
    cost += castCost;
  }
  return cost;
}

uint32_t BuiltInFunctionsUtils::matchVarLengthParameters(
    const std::vector<DataType>& inputTypes, DataTypeId targetTypeID) {
  auto cost = 0u;
  for (const auto& inputType : inputTypes) {
    auto castCost = getCastCost(inputType.id(), targetTypeID);
    if (castCost == UNDEFINED_CAST_COST) {
      return UINT32_MAX;
    }
    cost += castCost;
  }
  return cost;
}

void BuiltInFunctionsUtils::validateSpecialCases(
    std::vector<Function*>& candidateFunctions, const std::string& name,
    const std::vector<DataType>& inputTypes,
    const function::function_set& set) {
  if (name == AddFunction::name) {
    auto targetType0 = candidateFunctions[0]->parameterTypes[0].id();
    auto targetType1 = candidateFunctions[0]->parameterTypes[1].id();
    auto inputType0 = inputTypes[0].id();
    auto inputType1 = inputTypes[1].id();
    if ((inputType0 != DataTypeId::kVarchar ||
         inputType1 != DataTypeId::kVarchar) &&
        targetType0 == DataTypeId::kVarchar &&
        targetType1 == DataTypeId::kVarchar) {
      std::string supportedInputsString;
      for (auto& function : set) {
        supportedInputsString += function->signatureToString() + "\n";
      }
      THROW_BINDER_EXCEPTION(
          "Cannot match a built-in function for given function " + name +
          LogicalTypeUtils::toString(inputTypes) + ". Supported inputs are\n" +
          supportedInputsString);
    }
  }
}

static std::string getFunctionMatchFailureMsg(
    const std::string name, const std::vector<DataType>& inputTypes,
    const std::string& supportedInputs, bool isDistinct = false) {
  auto result = stringFormat(
      "Cannot match a built-in function for given function {}{}{}.", name,
      isDistinct ? "DISTINCT " : "", LogicalTypeUtils::toString(inputTypes));
  if (supportedInputs.empty()) {
    result += " Expect empty inputs.";
  } else {
    result += " Supported inputs are\n" + supportedInputs;
  }
  return result;
}

void validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunction*>& candidateFunctions,
    const std::string& name, const std::vector<DataType>& inputTypes,
    bool isDistinct, const function::function_set& set) {
  if (candidateFunctions.empty()) {
    std::string supportedInputsString;
    for (auto& function : set) {
      auto aggregateFunction = function->constPtrCast<AggregateFunction>();
      if (aggregateFunction->isDistinct) {
        supportedInputsString += "DISTINCT ";
      }
      supportedInputsString += aggregateFunction->signatureToString() + "\n";
    }
    THROW_BINDER_EXCEPTION(getFunctionMatchFailureMsg(
        name, inputTypes, supportedInputsString, isDistinct));
  }
}

void validateNonEmptyCandidateFunctions(
    std::vector<Function*>& candidateFunctions, const std::string& name,
    const std::vector<DataType>& inputTypes,
    const function::function_set& set) {
  if (candidateFunctions.empty()) {
    std::string supportedInputsString;
    for (auto& function : set) {
      if (function->parameterTypes.empty()) {
        continue;
      }
      supportedInputsString += function->signatureToString() + "\n";
    }
    THROW_BINDER_EXCEPTION(
        getFunctionMatchFailureMsg(name, inputTypes, supportedInputsString));
  }
}

}  // namespace function
}  // namespace neug
