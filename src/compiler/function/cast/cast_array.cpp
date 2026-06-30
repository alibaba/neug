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

#include "neug/compiler/function/cast/functions/cast_array.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

bool CastArrayHelper::checkCompatibleNestedTypes(DataTypeId sourceTypeID,
                                                 DataTypeId targetTypeID) {
  switch (sourceTypeID) {
  case DataTypeId::kUnknown: {
    return true;
  }
  case DataTypeId::kList:
  case DataTypeId::kArray:
  case DataTypeId::kMap:
  case DataTypeId::kStruct: {
    return sourceTypeID == targetTypeID;
  } break;
  default:
    return false;
  }
  return false;
}

namespace {

const DataType& getListLikeChildType(const DataType& type) {
  if (type.id() == DataTypeId::kList) {
    return ListType::GetChildType(type);
  }
  if (type.id() == DataTypeId::kArray) {
    return ArrayType::GetChildType(type);
  }
  NEUG_UNREACHABLE;
}

}  // namespace

bool CastArrayHelper::requiresArrayEntryValidation(const DataType& srcType,
                                                   const DataType& dstType) {
  if (srcType.id() == DataTypeId::kArray &&
      dstType.id() == DataTypeId::kArray) {
    return true;
  }

  if (checkCompatibleNestedTypes(srcType.id(), dstType.id())) {
    switch (getPhysicalType(srcType.id())) {
    case PhysicalTypeID::LIST: {
      return requiresArrayEntryValidation(getListLikeChildType(srcType),
                                          getListLikeChildType(dstType));
    }
    case PhysicalTypeID::ARRAY: {
      return requiresArrayEntryValidation(getListLikeChildType(srcType),
                                          getListLikeChildType(dstType));
    }
    case PhysicalTypeID::STRUCT: {
      const auto& srcFieldTypes = StructType::GetChildTypes(srcType);
      const auto& dstFieldTypes = StructType::GetChildTypes(dstType);
      if (srcFieldTypes.size() != dstFieldTypes.size()) {
        THROW_CONVERSION_EXCEPTION(
            stringFormat("Unsupported casting function from {} to {}.",
                         srcType.ToString(), dstType.ToString()));
      }

      for (auto i = 0u; i < srcFieldTypes.size(); i++) {
        if (requiresArrayEntryValidation(srcFieldTypes[i], dstFieldTypes[i])) {
          return true;
        }
      }
    } break;
    default:
      return false;
    }
  }
  return false;
}

void CastArrayHelper::validateArrayEntries(ValueVector* inputVector,
                                           const DataType& resultType,
                                           uint64_t pos) {
  if (inputVector->isNull(pos)) {
    return;
  }
  const auto& inputType = inputVector->dataType;

  switch (getPhysicalType(resultType.id())) {
  case PhysicalTypeID::ARRAY: {
    if (getPhysicalType(inputType.id()) != PhysicalTypeID::ARRAY ||
        ArrayType::GetNumElements(inputType) !=
            ArrayType::GetNumElements(resultType)) {
      THROW_CONVERSION_EXCEPTION(
          stringFormat("Unsupported casting function from {} to {}.",
                       inputType.ToString(), resultType.ToString()));
    }
    auto listEntry = inputVector->getValue<list_entry_t>(pos);
    auto inputChildVector = ListVector::getDataVector(inputVector);
    for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size;
         i++) {
      validateArrayEntries(inputChildVector,
                           ArrayType::GetChildType(resultType), i);
    }
  } break;
  case PhysicalTypeID::LIST: {
    if (getPhysicalType(inputType.id()) == PhysicalTypeID::LIST) {
      auto listEntry = inputVector->getValue<list_entry_t>(pos);
      auto inputChildVector = ListVector::getDataVector(inputVector);
      for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size;
           i++) {
        validateArrayEntries(inputChildVector,
                             ListType::GetChildType(resultType), i);
      }
    }
  } break;
  case PhysicalTypeID::STRUCT: {
    if (getPhysicalType(inputType.id()) == PhysicalTypeID::STRUCT) {
      auto fieldVectors = StructVector::getFieldVectors(inputVector);
      const auto& fieldTypes = StructType::GetChildTypes(resultType);

      auto structEntry = inputVector->getValue<struct_entry_t>(pos);
      for (auto i = 0u; i < fieldVectors.size(); i++) {
        validateArrayEntries(fieldVectors[i].get(), fieldTypes[i],
                             structEntry.pos);
      }
    }
  } break;
  default: {
    return;
  }
  }
}

}  // namespace function
}  // namespace neug
