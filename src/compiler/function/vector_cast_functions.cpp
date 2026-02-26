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

#include "neug/compiler/function/cast/vector_cast_functions.h"
#include <utility>
#include <vector>

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/cast/functions/cast_array.h"
#include "neug/compiler/function/cast/functions/cast_decimal.h"
#include "neug/compiler/function/cast/functions/cast_from_string_functions.h"
#include "neug/compiler/function/cast/functions/cast_functions.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {

struct CastChildFunctionExecutor {
  template <typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
            typename OP_WRAPPER>
  static void executeSwitch(common::ValueVector& operand,
                            common::SelectionVector*,
                            common::ValueVector& result,
                            common::SelectionVector*, void* dataPtr) {
    auto numOfEntries =
        reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
    for (auto i = 0u; i < numOfEntries; i++) {
      result.setNull(i, operand.isNull(i));
      if (!result.isNull(i)) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
            (void*) (&operand), i, (void*) (&result), i, dataPtr);
      }
    }
  }
};

static void resolveNestedVector(std::shared_ptr<ValueVector> inputVector,
                                ValueVector* resultVector,
                                uint64_t numOfEntries,
                                CastFunctionBindData* dataPtr) {
  const auto* inputType = &inputVector->dataType;
  const auto* resultType = &resultVector->dataType;
  while (true) {
    if ((inputType->getPhysicalType() == PhysicalTypeID::LIST ||
         inputType->getPhysicalType() == PhysicalTypeID::ARRAY) &&
        (resultType->getPhysicalType() == PhysicalTypeID::LIST ||
         resultType->getPhysicalType() == PhysicalTypeID::ARRAY)) {
      // copy data and nullmask from input
      memcpy(resultVector->getData(), inputVector->getData(),
             numOfEntries * resultVector->getNumBytesPerValue());
      resultVector->setNullFromBits(inputVector->getNullMask().getData(), 0, 0,
                                    numOfEntries);

      numOfEntries = ListVector::getDataVectorSize(inputVector.get());
      ListVector::resizeDataVector(resultVector, numOfEntries);

      inputVector = ListVector::getSharedDataVector(inputVector.get());
      resultVector = ListVector::getDataVector(resultVector);
      inputType = &inputVector->dataType;
      resultType = &resultVector->dataType;
    } else if (inputType->getLogicalTypeID() == LogicalTypeID::STRUCT &&
               resultType->getPhysicalType() == PhysicalTypeID::STRUCT) {
      // Check if struct type can be cast
      auto errorMsg =
          stringFormat("Unsupported casting function from {} to {}.",
                       inputType->toString(), resultType->toString());
      // Check if two structs have the same number of fields
      if (::StructType::getNumFields(*inputType) !=
          ::StructType::getNumFields(*resultType)) {
        THROW_CONVERSION_EXCEPTION(errorMsg);
      }

      // Check if two structs have the same field names
      auto inputTypeNames = ::StructType::getFieldNames(*inputType);
      auto resultTypeNames = ::StructType::getFieldNames(*resultType);

      for (auto i = 0u; i < inputTypeNames.size(); i++) {
        if (inputTypeNames[i] != resultTypeNames[i]) {
          THROW_CONVERSION_EXCEPTION(errorMsg);
        }
      }

      // copy data and nullmask from input
      memcpy(resultVector->getData(), inputVector->getData(),
             numOfEntries * resultVector->getNumBytesPerValue());
      resultVector->setNullFromBits(inputVector->getNullMask().getData(), 0, 0,
                                    numOfEntries);

      auto inputFieldVectors = StructVector::getFieldVectors(inputVector.get());
      auto resultFieldVectors = StructVector::getFieldVectors(resultVector);
      for (auto i = 0u; i < inputFieldVectors.size(); i++) {
        resolveNestedVector(inputFieldVectors[i], resultFieldVectors[i].get(),
                            numOfEntries, dataPtr);
      }
      return;
    } else {
      break;
    }
  }

  // non-nested types
  if (inputType->getLogicalTypeID() != resultType->getLogicalTypeID()) {
    auto func = CastFunction::bindCastFunction<CastChildFunctionExecutor>(
                    "CAST", *inputType, *resultType)
                    ->execFunc;
    std::vector<std::shared_ptr<ValueVector>> childParams{inputVector};
    dataPtr->numOfEntries = numOfEntries;
    func(childParams, SelectionVector::fromValueVectors(childParams),
         *resultVector, resultVector->getSelVectorPtr(), (void*) dataPtr);
  } else {
    for (auto i = 0u; i < numOfEntries; i++) {
      resultVector->copyFromVectorData(i, inputVector.get(), i);
    }
  }
}

static void nestedTypesCastExecFunction(
    const std::vector<std::shared_ptr<common::ValueVector>>& params,
    const std::vector<common::SelectionVector*>& paramSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void*) {
  NEUG_ASSERT(params.size() == 1);
  result.resetAuxiliaryBuffer();
  const auto& inputVector = params[0];
  const auto* inputVectorSelVector = paramSelVectors[0];

  // check if all selcted list entry have the requried fixed list size
  if (CastArrayHelper::containsListToArray(inputVector->dataType,
                                           result.dataType)) {
    for (auto i = 0u; i < inputVectorSelVector->getSelSize(); i++) {
      auto pos = (*inputVectorSelVector)[i];
      CastArrayHelper::validateListEntry(inputVector.get(), result.dataType,
                                         pos);
    }
  };

  auto& selVector = *inputVectorSelVector;
  auto bindData = CastFunctionBindData(result.dataType.copy());
  auto numOfEntries = selVector[selVector.getSelSize() - 1] + 1;
  resolveNestedVector(inputVector, &result, numOfEntries, &bindData);
  if (inputVector->state->isFlat()) {
    resultSelVector->setToFiltered();
    (*resultSelVector)[0] = (*inputVectorSelVector)[0];
  }
}

static bool hasImplicitCastList(const LogicalType& srcType,
                                const LogicalType& dstType) {
  return CastFunction::hasImplicitCast(::ListType::getChildType(srcType),
                                       ::ListType::getChildType(dstType));
}

static bool hasImplicitCastArray(const LogicalType& srcType,
                                 const LogicalType& dstType) {
  if (ArrayType::getNumElements(srcType) !=
      ArrayType::getNumElements(dstType)) {
    return false;
  }
  return CastFunction::hasImplicitCast(ArrayType::getChildType(srcType),
                                       ArrayType::getChildType(dstType));
}

static bool hasImplicitCastArrayToList(const LogicalType& srcType,
                                       const LogicalType& dstType) {
  return CastFunction::hasImplicitCast(ArrayType::getChildType(srcType),
                                       ::ListType::getChildType(dstType));
}

static bool hasImplicitCastListToArray(const LogicalType& srcType,
                                       const LogicalType& dstType) {
  return CastFunction::hasImplicitCast(::ListType::getChildType(srcType),
                                       ArrayType::getChildType(dstType));
}

static bool hasImplicitCastStruct(const LogicalType& srcType,
                                  const LogicalType& dstType) {
  const auto& srcFields = ::StructType::getFields(srcType);
  const auto& dstFields = ::StructType::getFields(dstType);
  if (srcFields.size() != dstFields.size()) {
    return false;
  }
  for (auto i = 0u; i < srcFields.size(); i++) {
    if (srcFields[i].getName() != dstFields[i].getName()) {
      return false;
    }
    if (!CastFunction::hasImplicitCast(srcFields[i].getType(),
                                       dstFields[i].getType())) {
      return false;
    }
  }
  return true;
}

static bool hasImplicitCastUnion(const LogicalType& /*srcType*/,
                                 const LogicalType& /*dstType*/) {
  // todo: implement union casting function
  // currently, there seems to be no casting functionality between union types
  return false;
}

static bool hasImplicitCastMap(const LogicalType& srcType,
                               const LogicalType& dstType) {
  const auto& srcKeyType = MapType::getKeyType(srcType);
  const auto& srcValueType = MapType::getValueType(srcType);
  const auto& dstKeyType = MapType::getKeyType(dstType);
  const auto& dstValueType = MapType::getValueType(dstType);
  return CastFunction::hasImplicitCast(srcKeyType, dstKeyType) &&
         CastFunction::hasImplicitCast(srcValueType, dstValueType);
}

bool CastFunction::hasImplicitCast(const LogicalType& srcType,
                                   const LogicalType& dstType) {
  if (LogicalTypeUtils::isNested(srcType) &&
      LogicalTypeUtils::isNested(dstType)) {
    if (srcType.getLogicalTypeID() == LogicalTypeID::ARRAY &&
        dstType.getLogicalTypeID() == LogicalTypeID::LIST) {
      return hasImplicitCastArrayToList(srcType, dstType);
    }
    if (srcType.getLogicalTypeID() == LogicalTypeID::LIST &&
        dstType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
      return hasImplicitCastListToArray(srcType, dstType);
    }
    if (srcType.getLogicalTypeID() != dstType.getLogicalTypeID()) {
      return false;
    }
    switch (srcType.getLogicalTypeID()) {
    case LogicalTypeID::LIST:
      return hasImplicitCastList(srcType, dstType);
    case LogicalTypeID::ARRAY:
      return hasImplicitCastArray(srcType, dstType);
    case LogicalTypeID::STRUCT:
      return hasImplicitCastStruct(srcType, dstType);
    case LogicalTypeID::UNION:
      return hasImplicitCastUnion(srcType, dstType);
    case LogicalTypeID::MAP:
      return hasImplicitCastMap(srcType, dstType);
    default:
      // LCOV_EXCL_START
      NEUG_UNREACHABLE;
      // LCOV_EXCL_END
    }
  }
  if (BuiltInFunctionsUtils::getCastCost(srcType.getLogicalTypeID(),
                                         dstType.getLogicalTypeID()) !=
      UNDEFINED_CAST_COST) {
    return true;
  }
  // TODO(Jiamin): there are still other special cases
  // We allow cast between any numerical types
  if (LogicalTypeUtils::isNumerical(srcType) &&
      LogicalTypeUtils::isNumerical(dstType)) {
    return true;
  }
  return false;
}

template <typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastFromStringFunction(
    const std::string& functionName, const LogicalType& targetType) {
  scalar_func_exec_t execFunc;
  switch (targetType.getLogicalTypeID()) {
  case LogicalTypeID::DATE: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, date_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_SEC: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, timestamp_sec_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_MS: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, timestamp_ms_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_NS: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, timestamp_ns_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_TZ: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, neug::common::timestamp_tz_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, neug::common::timestamp_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::INTERVAL: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, interval_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::BLOB: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, blob_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UUID: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, neug_uuid_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::STRING: {
    execFunc =
        ScalarFunction::UnaryCastExecFunction<neug_string_t, neug_string_t,
                                              CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::BOOL: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, bool,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::DOUBLE: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, double,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::FLOAT: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, float,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::DECIMAL: {
    switch (targetType.getPhysicalType()) {
    case PhysicalTypeID::INT16:
      execFunc =
          ScalarFunction::UnaryExecNestedTypeFunction<neug_string_t, int16_t,
                                                      CastToDecimal>;
      break;
    case PhysicalTypeID::INT32:
      execFunc =
          ScalarFunction::UnaryExecNestedTypeFunction<neug_string_t, int32_t,
                                                      CastToDecimal>;
      break;
    case PhysicalTypeID::INT64:
      execFunc =
          ScalarFunction::UnaryExecNestedTypeFunction<neug_string_t, int64_t,
                                                      CastToDecimal>;
      break;
    case PhysicalTypeID::INT128:
      execFunc =
          ScalarFunction::UnaryExecNestedTypeFunction<neug_string_t, int128_t,
                                                      CastToDecimal>;
      break;
    default:
      NEUG_UNREACHABLE;
    }
  } break;
  case LogicalTypeID::INT128: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, int128_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, int64_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT32: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, int32_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT16: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, int16_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT8: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, int8_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT64: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, uint64_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT32: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, uint32_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT16: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, uint16_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT8: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, uint8_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::ARRAY:
  case LogicalTypeID::LIST: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, list_entry_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::MAP: {
    execFunc =
        ScalarFunction::UnaryCastStringExecFunction<neug_string_t, map_entry_t,
                                                    CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::STRUCT: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, struct_entry_t, CastString, EXECUTOR>;
  } break;
  case LogicalTypeID::UNION: {
    execFunc = ScalarFunction::UnaryCastStringExecFunction<
        neug_string_t, union_entry_t, CastString, EXECUTOR>;
  } break;
  default:
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Unsupported casting function from STRING to {}.",
                     targetType.toString()));
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      targetType.getLogicalTypeID(), execFunc);
}

template <typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToStringFunction(
    const std::string& functionName, const LogicalType& sourceType) {
  scalar_func_exec_t func;
  switch (sourceType.getLogicalTypeID()) {
  case LogicalTypeID::BOOL: {
    func = ScalarFunction::UnaryCastExecFunction<bool, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64: {
    func = ScalarFunction::UnaryCastExecFunction<int64_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT32: {
    func = ScalarFunction::UnaryCastExecFunction<int32_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT16: {
    func = ScalarFunction::UnaryCastExecFunction<int16_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT8: {
    func = ScalarFunction::UnaryCastExecFunction<int8_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT64: {
    func = ScalarFunction::UnaryCastExecFunction<uint64_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT32: {
    func = ScalarFunction::UnaryCastExecFunction<uint32_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT16: {
    func = ScalarFunction::UnaryCastExecFunction<uint16_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INT128: {
    func = ScalarFunction::UnaryCastExecFunction<int128_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT8: {
    func = ScalarFunction::UnaryCastExecFunction<uint8_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::DOUBLE: {
    func = ScalarFunction::UnaryCastExecFunction<double, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::FLOAT: {
    func = ScalarFunction::UnaryCastExecFunction<float, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::DECIMAL: {
    switch (sourceType.getPhysicalType()) {
    case PhysicalTypeID::INT16:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, neug_string_t,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT32:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, neug_string_t,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT64:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, neug_string_t,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT128:
      func =
          ScalarFunction::UnaryExecNestedTypeFunction<int128_t, neug_string_t,
                                                      CastDecimalTo>;
      break;
    default:
      NEUG_UNREACHABLE;
    }
  } break;
  case LogicalTypeID::DATE: {
    func = ScalarFunction::UnaryCastExecFunction<date_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_NS: {
    func = ScalarFunction::UnaryCastExecFunction<timestamp_ns_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_MS: {
    func = ScalarFunction::UnaryCastExecFunction<timestamp_ms_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_SEC: {
    func = ScalarFunction::UnaryCastExecFunction<timestamp_sec_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_TZ: {
    func = ScalarFunction::UnaryCastExecFunction<
        neug::common::timestamp_tz_t, neug_string_t, CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP: {
    func = ScalarFunction::UnaryCastExecFunction<
        neug::common::timestamp_t, neug_string_t, CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INTERVAL: {
    func = ScalarFunction::UnaryCastExecFunction<interval_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::INTERNAL_ID: {
    func = ScalarFunction::UnaryCastExecFunction<internalID_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::BLOB: {
    func = ScalarFunction::UnaryCastExecFunction<blob_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UUID: {
    func = ScalarFunction::UnaryCastExecFunction<neug_uuid_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::ARRAY:
  case LogicalTypeID::LIST: {
    func = ScalarFunction::UnaryCastExecFunction<list_entry_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::MAP: {
    func = ScalarFunction::UnaryCastExecFunction<map_entry_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::NODE: {
    func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, neug_string_t,
                                                 CastNodeToString, EXECUTOR>;
  } break;
  case LogicalTypeID::REL: {
    func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, neug_string_t,
                                                 CastRelToString, EXECUTOR>;
  } break;
  case LogicalTypeID::RECURSIVE_REL:
  case LogicalTypeID::STRUCT: {
    func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  case LogicalTypeID::UNION: {
    func = ScalarFunction::UnaryCastExecFunction<union_entry_t, neug_string_t,
                                                 CastToString, EXECUTOR>;
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
      LogicalTypeID::STRING, func);
}

template <typename DST_TYPE, CastExecutor EXECUTOR>
static std::unique_ptr<ScalarFunction> bindCastToDecimalFunction(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& targetType) {
  scalar_func_exec_t func;
  if (sourceType.getLogicalTypeID() == LogicalTypeID::DECIMAL) {
    TypeUtils::visit(
        sourceType,
        [&]<SignedIntegerTypes T>(T) {
          func = ScalarFunction::UnaryCastExecFunction<
              T, DST_TYPE, CastBetweenDecimal, EXECUTOR>;
        },
        [&](auto) { NEUG_UNREACHABLE; });
  } else {
    TypeUtils::visit(
        sourceType,
        [&]<NumericTypes T>(T) {
          func = ScalarFunction::UnaryCastExecFunction<T, DST_TYPE,
                                                       CastToDecimal, EXECUTOR>;
        },
        [&](auto) { NEUG_UNREACHABLE; });
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
      targetType.getLogicalTypeID(), func);
}

template <typename DST_TYPE, typename OP,
          typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToNumericFunction(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& targetType) {
  scalar_func_exec_t func;
  switch (sourceType.getLogicalTypeID()) {
  case LogicalTypeID::INT8: {
    func = ScalarFunction::UnaryExecFunction<int8_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::INT16: {
    func = ScalarFunction::UnaryExecFunction<int16_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::INT32: {
    func = ScalarFunction::UnaryExecFunction<int32_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64: {
    func = ScalarFunction::UnaryExecFunction<int64_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT8: {
    func = ScalarFunction::UnaryExecFunction<uint8_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT16: {
    func = ScalarFunction::UnaryExecFunction<uint16_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT32: {
    func = ScalarFunction::UnaryExecFunction<uint32_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::UINT64: {
    func = ScalarFunction::UnaryExecFunction<uint64_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::INT128: {
    func = ScalarFunction::UnaryExecFunction<int128_t, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::FLOAT: {
    func = ScalarFunction::UnaryExecFunction<float, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::DOUBLE: {
    func = ScalarFunction::UnaryExecFunction<double, DST_TYPE, OP, EXECUTOR>;
  } break;
  case LogicalTypeID::DECIMAL: {
    switch (sourceType.getPhysicalType()) {
    // note: this cannot handle decimal -> decimal casting.
    case PhysicalTypeID::INT16:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT32:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT64:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE,
                                                         CastDecimalTo>;
      break;
    case PhysicalTypeID::INT128:
      func = ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE,
                                                         CastDecimalTo>;
      break;
    default:
      NEUG_UNREACHABLE;
    }
  } break;
  default:
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Unsupported casting function from {} to {}.",
                     sourceType.toString(), targetType.toString()));
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
      targetType.getLogicalTypeID(), func);
}

static std::unique_ptr<ScalarFunction> bindCastBetweenNested(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& targetType) {
  switch (sourceType.getLogicalTypeID()) {
  case LogicalTypeID::LIST:
  case LogicalTypeID::MAP:
  case LogicalTypeID::STRUCT:
  case LogicalTypeID::ANY:
  case LogicalTypeID::ARRAY: {
    // todo: compile time checking of nested types
    if (CastArrayHelper::checkCompatibleNestedTypes(
            sourceType.getLogicalTypeID(), targetType.getLogicalTypeID())) {
      return std::make_unique<ScalarFunction>(
          functionName,
          std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
          targetType.getLogicalTypeID(), nestedTypesCastExecFunction);
    }
    [[fallthrough]];
  }
  default:
    THROW_CONVERSION_EXCEPTION(stringFormat(
        "Unsupported casting function from {} to {}.",
        LogicalTypeUtils::toString(sourceType.getLogicalTypeID()),
        LogicalTypeUtils::toString(targetType.getLogicalTypeID())));
  }
}

template <typename EXECUTOR = UnaryFunctionExecutor, typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastToDateFunction(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& dstType) {
  scalar_func_exec_t func;
  switch (sourceType.getLogicalTypeID()) {
  case LogicalTypeID::TIMESTAMP_MS:
    func = ScalarFunction::UnaryExecFunction<timestamp_ms_t, DST_TYPE,
                                             CastToDate, EXECUTOR>;
    break;
  case LogicalTypeID::TIMESTAMP_NS:
    func = ScalarFunction::UnaryExecFunction<timestamp_ns_t, DST_TYPE,
                                             CastToDate, EXECUTOR>;
    break;
  case LogicalTypeID::TIMESTAMP_SEC:
    func = ScalarFunction::UnaryExecFunction<timestamp_sec_t, DST_TYPE,
                                             CastToDate, EXECUTOR>;
    break;
  case LogicalTypeID::TIMESTAMP_TZ:
  case LogicalTypeID::TIMESTAMP:
    func = ScalarFunction::UnaryExecFunction<neug::common::timestamp_t, DST_TYPE,
                                             CastToDate, EXECUTOR>;
    break;
  // LCOV_EXCL_START
  default:
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Unsupported casting function from {} to {}.",
                     sourceType.toString(), dstType.toString()));
    // LCOV_EXCL_END
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
      LogicalTypeID::DATE, func);
}

template <typename EXECUTOR = UnaryFunctionExecutor, typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastToTimestampFunction(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& dstType) {
  scalar_func_exec_t func;
  switch (sourceType.getLogicalTypeID()) {
  case LogicalTypeID::DATE: {
    func = ScalarFunction::UnaryExecFunction<date_t, DST_TYPE,
                                             CastDateToTimestamp, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_MS: {
    func = ScalarFunction::UnaryExecFunction<timestamp_ms_t, DST_TYPE,
                                             CastBetweenTimestamp, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_NS: {
    func = ScalarFunction::UnaryExecFunction<timestamp_ns_t, DST_TYPE,
                                             CastBetweenTimestamp, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_SEC: {
    func = ScalarFunction::UnaryExecFunction<timestamp_sec_t, DST_TYPE,
                                             CastBetweenTimestamp, EXECUTOR>;
  } break;
  case LogicalTypeID::TIMESTAMP_TZ:
  case LogicalTypeID::TIMESTAMP: {
    func = ScalarFunction::UnaryExecFunction<neug::common::timestamp_t, DST_TYPE,
                                             CastBetweenTimestamp, EXECUTOR>;
  } break;
  default:
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Unsupported casting function from {} to {}.",
                     sourceType.toString(), dstType.toString()));
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
      LogicalTypeID::TIMESTAMP, func);
}

template <typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastBetweenDecimalFunction(
    const std::string& functionName, const LogicalType& sourceType) {
  scalar_func_exec_t func;
  switch (sourceType.getPhysicalType()) {
  case PhysicalTypeID::INT16:
    func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE,
                                                       CastBetweenDecimal>;
    break;
  case PhysicalTypeID::INT32:
    func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE,
                                                       CastBetweenDecimal>;
    break;
  case PhysicalTypeID::INT64:
    func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE,
                                                       CastBetweenDecimal>;
    break;
  case PhysicalTypeID::INT128:
    func = ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE,
                                                       CastBetweenDecimal>;
    break;
  default:
    NEUG_UNREACHABLE;
  }
  return std::make_unique<ScalarFunction>(
      functionName, std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL},
      LogicalTypeID::DECIMAL, func);
}

template <CastExecutor EXECUTOR>
std::unique_ptr<ScalarFunction> CastFunction::bindCastFunction(
    const std::string& functionName, const LogicalType& sourceType,
    const LogicalType& targetType) {
  auto sourceTypeID = sourceType.getLogicalTypeID();
  auto targetTypeID = targetType.getLogicalTypeID();
  if (sourceTypeID == LogicalTypeID::STRING) {
    return bindCastFromStringFunction<EXECUTOR>(functionName, targetType);
  }
  switch (targetTypeID) {
  case LogicalTypeID::STRING: {
    return bindCastToStringFunction<EXECUTOR>(functionName, sourceType);
  }
  case LogicalTypeID::DOUBLE: {
    return bindCastToNumericFunction<double, CastToDouble, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::FLOAT: {
    return bindCastToNumericFunction<float, CastToFloat, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::DECIMAL: {
    std::unique_ptr<ScalarFunction> scalarFunc;
    TypeUtils::visit(
        targetType.getPhysicalType(),
        [&]<IntegerTypes T>(T) {
          scalarFunc = bindCastToDecimalFunction<T, EXECUTOR>(
              functionName, sourceType, targetType);
        },
        [](auto) { NEUG_UNREACHABLE; });
    return scalarFunc;
  }
  case LogicalTypeID::INT128: {
    return bindCastToNumericFunction<int128_t, CastToInt128, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::SERIAL: {
    return bindCastToNumericFunction<int64_t, CastToSerial, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::INT64: {
    return bindCastToNumericFunction<int64_t, CastToInt64, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::INT32: {
    return bindCastToNumericFunction<int32_t, CastToInt32, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::INT16: {
    return bindCastToNumericFunction<int16_t, CastToInt16, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::INT8: {
    return bindCastToNumericFunction<int8_t, CastToInt8, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::UINT64: {
    return bindCastToNumericFunction<uint64_t, CastToUInt64, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::UINT32: {
    return bindCastToNumericFunction<uint32_t, CastToUInt32, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::UINT16: {
    return bindCastToNumericFunction<uint16_t, CastToUInt16, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::UINT8: {
    return bindCastToNumericFunction<uint8_t, CastToUInt8, EXECUTOR>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::DATE: {
    return bindCastToDateFunction<EXECUTOR, date_t>(functionName, sourceType,
                                                    targetType);
  }
  case LogicalTypeID::TIMESTAMP_NS: {
    return bindCastToTimestampFunction<EXECUTOR, timestamp_ns_t>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::TIMESTAMP_MS: {
    return bindCastToTimestampFunction<EXECUTOR, timestamp_ms_t>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::TIMESTAMP_SEC: {
    return bindCastToTimestampFunction<EXECUTOR, timestamp_sec_t>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::TIMESTAMP_TZ:
  case LogicalTypeID::TIMESTAMP: {
    return bindCastToTimestampFunction<EXECUTOR, neug::common::timestamp_t>(
        functionName, sourceType, targetType);
  }
  case LogicalTypeID::LIST:
  case LogicalTypeID::ARRAY:
  case LogicalTypeID::MAP:
  case LogicalTypeID::STRUCT:
  case LogicalTypeID::UNION: {
    return bindCastBetweenNested(functionName, sourceType, targetType);
  }
  default: {
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Unsupported casting function from {} to {}.",
                     sourceType.toString(), targetType.toString()));
  }
  }
}

function_set CastToDateFunction::getFunctionSet() {
  function_set result;
  result.push_back(CastFunction::bindCastFunction(name, LogicalType::STRING(),
                                                  LogicalType::DATE()));
  return result;
}

function_set CastToTimestampFunction::getFunctionSet() {
  function_set result;
  result.push_back(CastFunction::bindCastFunction(name, LogicalType::STRING(),
                                                  LogicalType::TIMESTAMP()));
  return result;
}

function_set CastToIntervalFunction::getFunctionSet() {
  function_set result;
  result.push_back(CastFunction::bindCastFunction(name, LogicalType::STRING(),
                                                  LogicalType::INTERVAL()));
  return result;
}

static std::unique_ptr<FunctionBindData> castBindFunc(
    ScalarBindFuncInput input) {
  NEUG_ASSERT(input.arguments.size() == 2);
  // Bind target type.
  if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Second parameter of CAST function must be a literal."));
  }
  auto literalExpr = input.arguments[1]->constPtrCast<LiteralExpression>();
  auto targetTypeStr = literalExpr->getValue().getValue<std::string>();
  auto func = input.definition->ptrCast<ScalarFunction>();
  auto targetType =
      LogicalType::convertFromString(targetTypeStr, input.context);
  // For STRUCT type, we will need to check its field name in later stage
  // Otherwise, there will be bug for: RETURN cast({'a': 12, 'b': 12} AS
  // struct(c int64, d int64)); being allowed.
  if (targetType == input.arguments[0]->getDataType() &&
      targetType.getLogicalTypeID() !=
          LogicalTypeID::STRUCT) {  // No need to cast.
    return nullptr;
  }
  if (ExpressionUtil::canCastStatically(*input.arguments[0], targetType) &&
      targetType.getLogicalTypeID() != LogicalTypeID::STRUCT) {
    input.arguments[0]->cast(targetType);
    return nullptr;
  }
  try {
    func->execFunc = CastFunction::bindCastFunction(
                         "CAST_TO_" + targetTypeStr,
                         input.arguments[0]->getDataType(), targetType)
                         ->execFunc;
  } catch (...) {}
  auto bindData =
      std::make_unique<function::CastFunctionBindData>(targetType.copy());
  auto inputTypes = ExpressionUtil::getDataTypes(input.arguments);
  bindData->paramTypes = std::move(inputTypes);
  return bindData;
}

static execution::Value castFunc(const std::vector<execution::Value>& args) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR("CAST(VAL, TYPE): expect exactly 2 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& arg0 = args[0];
  const auto& arg1 = args[1];
  auto type = execution::StringValue::Get(arg1);

  if (type == "INT64") {
    return performCast<int64_t>(arg0);
  } else if (type == "INT32") {
    return performCast<int32_t>(arg0);
  } else if (type == "FLOAT") {
    return performCast<float>(arg0);
  } else if (type == "DOUBLE") {
    return performCast<double>(arg0);
  } else if (type == "STRING") {
    return performCastToString(arg0);
  } else if (type == "DATE") {
    return performCast<neug::Date>(arg0);
  } else if (type == "TIMESTAMP") {
    return performCast<neug::DateTime>(arg0);
  } else if (type == "UINT32") {
    return performCast<uint32_t>(arg0);
  } else if (type == "UINT64") {
    return performCast<uint64_t>(arg0);
  } else {
    THROW_RUNTIME_ERROR(std::string("Unsupported target type for CAST: ") +
                        std::string(type));
  }
  return execution::Value(DataType::SQLNULL);
}

function_set CastAnyFunction::getFunctionSet() {
  function_set result;
  // todo(engine): support cast execution function in NeugScalarFunction
  auto func = std::make_unique<NeugScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::STRING},
      LogicalTypeID::ANY, std::move(castFunc));
  func->bindFunc = castBindFunc;
  result.push_back(std::move(func));
  return result;
}

}  // namespace function
}  // namespace neug
