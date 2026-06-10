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

#include "aggregate_function.h"
#include "neug/compiler/function/function.h"

namespace neug {
namespace transaction {
class Transaction;
}  // namespace transaction

namespace catalog {
class FunctionCatalogEntry;
}  // namespace catalog

namespace function {

class BuiltInFunctionsUtils {
 public:
  // TODO(Ziyi): We should have a unified interface for matching table,
  // aggregate and scalar functions.
  static NEUG_API Function* matchFunction(
      const std::string& name,
      const catalog::FunctionCatalogEntry* catalogEntry) {
    return matchFunction(name, {}, catalogEntry);
  }
  static NEUG_API Function* matchFunction(
      const std::string& name, const std::vector<common::DataType>& inputTypes,
      const catalog::FunctionCatalogEntry* functionEntry);

  static AggregateFunction* matchAggregateFunction(
      const std::string& name, const std::vector<common::DataType>& inputTypes,
      bool isDistinct, const catalog::FunctionCatalogEntry* functionEntry);

  static NEUG_API uint32_t getCastCost(common::DataTypeId inputTypeID,
                                       common::DataTypeId targetTypeID);

 private:
  // TODO(Xiyang): move casting cost related functions to binder.
  static uint32_t getTargetTypeCost(common::DataTypeId typeID);

  static uint32_t castInt64(common::DataTypeId targetTypeID);

  static uint32_t castInt32(common::DataTypeId targetTypeID);

  static uint32_t castInt16(common::DataTypeId targetTypeID);

  static uint32_t castInt8(common::DataTypeId targetTypeID);

  static uint32_t castUInt64(common::DataTypeId targetTypeID);

  static uint32_t castUInt32(common::DataTypeId targetTypeID);

  static uint32_t castUInt16(common::DataTypeId targetTypeID);

  static uint32_t castUInt8(common::DataTypeId targetTypeID);

  static uint32_t castInt128(common::DataTypeId targetTypeID);

  static uint32_t castDouble(common::DataTypeId targetTypeID);

  static uint32_t castFloat(common::DataTypeId targetTypeID);

  static uint32_t castDate(common::DataTypeId targetTypeID);

  static uint32_t castTimestamp(common::DataTypeId targetTypeID);

  static uint32_t castFromString(common::DataTypeId inputTypeID);

  static uint32_t castList(common::DataTypeId targetTypeID);

  static uint32_t castArray(common::DataTypeId targetTypeID);

  static Function* getBestMatch(std::vector<Function*>& functions);

  static uint32_t getFunctionCost(
      const std::vector<common::DataType>& inputTypes, Function* function);
  static uint32_t matchParameters(
      const std::vector<common::DataType>& inputTypes,
      const std::vector<common::DataTypeId>& targetTypeIDs);
  static uint32_t matchVarLengthParameters(
      const std::vector<common::DataType>& inputTypes,
      common::DataTypeId targetTypeID);
  static uint32_t getAggregateFunctionCost(
      const std::vector<common::DataType>& inputTypes, bool isDistinct,
      AggregateFunction* function);

  static void validateSpecialCases(
      std::vector<Function*>& candidateFunctions, const std::string& name,
      const std::vector<common::DataType>& inputTypes,
      const function::function_set& set);
};

}  // namespace function
}  // namespace neug
