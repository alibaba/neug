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
 * This file is originally from the Kuzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/types/types.h"

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/null_buffer.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/neug_list.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

using neug::function::BuiltInFunctionsUtils;

namespace neug {
namespace common {

// ============================================================================
// internalID_t implementations
// ============================================================================

internalID_t::internalID_t()
    : offset{INVALID_OFFSET}, tableID{INVALID_TABLE_ID} {}

internalID_t::internalID_t(offset_t offset, table_id_t tableID)
    : offset(offset), tableID(tableID) {}

bool internalID_t::operator==(const internalID_t& rhs) const {
  return offset == rhs.offset && tableID == rhs.tableID;
}

bool internalID_t::operator!=(const internalID_t& rhs) const {
  return offset != rhs.offset || tableID != rhs.tableID;
}

bool internalID_t::operator>(const internalID_t& rhs) const {
  return (tableID > rhs.tableID) ||
         (tableID == rhs.tableID && offset > rhs.offset);
}

bool internalID_t::operator>=(const internalID_t& rhs) const {
  return (tableID > rhs.tableID) ||
         (tableID == rhs.tableID && offset >= rhs.offset);
}

bool internalID_t::operator<(const internalID_t& rhs) const {
  return (tableID < rhs.tableID) ||
         (tableID == rhs.tableID && offset < rhs.offset);
}

bool internalID_t::operator<=(const internalID_t& rhs) const {
  return (tableID < rhs.tableID) ||
         (tableID == rhs.tableID && offset <= rhs.offset);
}

// ============================================================================
// PhysicalTypeUtils implementations
// ============================================================================

std::string PhysicalTypeUtils::toString(PhysicalTypeID physicalType) {
  switch (physicalType) {
  case PhysicalTypeID::BOOL:
    return "BOOL";
  case PhysicalTypeID::INT64:
    return "INT64";
  case PhysicalTypeID::INT32:
    return "INT32";
  case PhysicalTypeID::INT16:
    return "INT16";
  case PhysicalTypeID::INT8:
    return "INT8";
  case PhysicalTypeID::UINT64:
    return "UINT64";
  case PhysicalTypeID::UINT32:
    return "UINT32";
  case PhysicalTypeID::UINT16:
    return "UINT16";
  case PhysicalTypeID::UINT8:
    return "UINT8";
  case PhysicalTypeID::INT128:
    return "INT128";
  case PhysicalTypeID::DOUBLE:
    return "DOUBLE";
  case PhysicalTypeID::FLOAT:
    return "FLOAT";
  case PhysicalTypeID::INTERVAL:
    return "INTERVAL";
  case PhysicalTypeID::INTERNAL_ID:
    return "INTERNAL_ID";
  case PhysicalTypeID::STRING:
    return "STRING";
  case PhysicalTypeID::STRUCT:
    return "STRUCT";
  case PhysicalTypeID::LIST:
    return "LIST";
  case PhysicalTypeID::ARRAY:
    return "ARRAY";
  case PhysicalTypeID::ALP_EXCEPTION_FLOAT:
    return "ALP_EXCEPTION_FLOAT";
  case PhysicalTypeID::ALP_EXCEPTION_DOUBLE:
    return "ALP_EXCEPTION_DOUBLE";
  default:
    NEUG_UNREACHABLE;
  }
}

uint32_t PhysicalTypeUtils::getFixedTypeSize(PhysicalTypeID physicalType) {
  switch (physicalType) {
  case PhysicalTypeID::BOOL:
    return sizeof(bool);
  case PhysicalTypeID::INT64:
    return sizeof(int64_t);
  case PhysicalTypeID::INT32:
    return sizeof(int32_t);
  case PhysicalTypeID::INT16:
    return sizeof(int16_t);
  case PhysicalTypeID::INT8:
    return sizeof(int8_t);
  case PhysicalTypeID::UINT64:
    return sizeof(uint64_t);
  case PhysicalTypeID::UINT32:
    return sizeof(uint32_t);
  case PhysicalTypeID::UINT16:
    return sizeof(uint16_t);
  case PhysicalTypeID::UINT8:
    return sizeof(uint8_t);
  case PhysicalTypeID::INT128:
    return sizeof(int128_t);
  case PhysicalTypeID::DOUBLE:
    return sizeof(double);
  case PhysicalTypeID::FLOAT:
    return sizeof(float);
  case PhysicalTypeID::INTERVAL:
    return sizeof(compiler_impl::interval_t);
  case PhysicalTypeID::INTERNAL_ID:
    return sizeof(internalID_t);
  default:
    NEUG_UNREACHABLE;
  }
}

// ============================================================================
// getPhysicalType - maps DataTypeId to PhysicalTypeID
// ============================================================================

PhysicalTypeID getPhysicalType(DataTypeId typeId) {
  switch (typeId) {
  case DataTypeId::kBoolean:
    return PhysicalTypeID::BOOL;
  case DataTypeId::kInt8:
    return PhysicalTypeID::INT8;
  case DataTypeId::kInt16:
    return PhysicalTypeID::INT16;
  case DataTypeId::kInt32:
    return PhysicalTypeID::INT32;
  case DataTypeId::kInt64:
    return PhysicalTypeID::INT64;
  case DataTypeId::kUInt8:
    return PhysicalTypeID::UINT8;
  case DataTypeId::kUInt16:
    return PhysicalTypeID::UINT16;
  case DataTypeId::kUInt32:
    return PhysicalTypeID::UINT32;
  case DataTypeId::kUInt64:
    return PhysicalTypeID::UINT64;
  case DataTypeId::kFloat:
    return PhysicalTypeID::FLOAT;
  case DataTypeId::kDouble:
    return PhysicalTypeID::DOUBLE;
  case DataTypeId::kDate:
    return PhysicalTypeID::INT32;
  case DataTypeId::kTimestampMs:
    return PhysicalTypeID::INT64;
  case DataTypeId::kInterval:
    return PhysicalTypeID::INTERVAL;
  case DataTypeId::kInternalId:
    return PhysicalTypeID::INTERNAL_ID;
  case DataTypeId::kVarchar:
    return PhysicalTypeID::STRING;
  case DataTypeId::kList:
  case DataTypeId::kMap:
    return PhysicalTypeID::LIST;
  case DataTypeId::kArray:
    return PhysicalTypeID::ARRAY;
  case DataTypeId::kStruct:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
    return PhysicalTypeID::STRUCT;
  case DataTypeId::kUnknown:
  default:
    return PhysicalTypeID::ANY;
  }
}

// ============================================================================
// Forward declarations for parse helpers (used by convertFromString /
// isBuiltInType)
// ============================================================================

static bool tryGetIDFromString(const std::string& trimmedStr, DataTypeId& id);
static std::vector<std::string> parseStructFields(
    const std::string& structTypeStr);

// Forward-declare convertFromString so parse helpers can call it recursively.
DataType convertFromString(const std::string& str,
                           main::ClientContext* context);

static DataType parseListType(const std::string& trimmedStr,
                              main::ClientContext* context = nullptr);
static DataType parseArrayType(const std::string& trimmedStr,
                               main::ClientContext* context = nullptr);
static DataType parseStructType(const std::string& trimmedStr,
                                main::ClientContext* context = nullptr);
static DataType parseMapType(const std::string& trimmedStr,
                             main::ClientContext* context = nullptr);
static DataType parseStringType(const std::string& trimmedStr);

// ============================================================================
// LogicalTypeUtils implementations
// ============================================================================

std::string LogicalTypeUtils::toString(DataTypeId dataTypeID) {
  switch (dataTypeID) {
  case DataTypeId::kUnknown:
    return "ANY";
  case DataTypeId::kVertex:
    return "NODE";
  case DataTypeId::kEdge:
    return "REL";
  case DataTypeId::kPath:
    return "RECURSIVE_REL";
  case DataTypeId::kInternalId:
    return "INTERNAL_ID";
  case DataTypeId::kBoolean:
    return "BOOL";
  case DataTypeId::kInt64:
    return "INT64";
  case DataTypeId::kInt32:
    return "INT32";
  case DataTypeId::kInt16:
    return "INT16";
  case DataTypeId::kInt8:
    return "INT8";
  case DataTypeId::kUInt64:
    return "UINT64";
  case DataTypeId::kUInt32:
    return "UINT32";
  case DataTypeId::kUInt16:
    return "UINT16";
  case DataTypeId::kUInt8:
    return "UINT8";
  case DataTypeId::kDouble:
    return "DOUBLE";
  case DataTypeId::kFloat:
    return "FLOAT";
  case DataTypeId::kDate:
    return "DATE";
  case DataTypeId::kTimestampMs:
    return "TIMESTAMP_MS";
  case DataTypeId::kInterval:
    return "INTERVAL";
  case DataTypeId::kVarchar:
    return "STRING";
  case DataTypeId::kList:
    return "LIST";
  case DataTypeId::kArray:
    return "ARRAY";
  case DataTypeId::kStruct:
    return "STRUCT";
  case DataTypeId::kMap:
    return "MAP";
  default:
    NEUG_UNREACHABLE;
  }
}

std::string LogicalTypeUtils::toString(const std::vector<DataType>& dataTypes) {
  if (dataTypes.empty()) {
    return {""};
  }
  std::string result = "(" + dataTypes[0].ToString();
  for (auto i = 1u; i < dataTypes.size(); ++i) {
    result += "," + dataTypes[i].ToString();
  }
  result += ")";
  return result;
}

std::string LogicalTypeUtils::toString(
    const std::vector<DataTypeId>& dataTypeIDs) {
  if (dataTypeIDs.empty()) {
    return {"()"};
  }
  std::string result = "(" + LogicalTypeUtils::toString(dataTypeIDs[0]);
  for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
    result += "," + LogicalTypeUtils::toString(dataTypeIDs[i]);
  }
  result += ")";
  return result;
}

uint32_t LogicalTypeUtils::getRowLayoutSize(const DataType& type) {
  auto physType = getPhysicalType(type.id());
  switch (physType) {
  case PhysicalTypeID::STRING: {
    return sizeof(neug_string_t);
  }
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST: {
    return sizeof(neug_list_t);
  }
  case PhysicalTypeID::STRUCT: {
    uint32_t size = 0;
    const auto& fieldsTypes = StructType::GetChildTypes(type);
    for (const auto& fieldType : fieldsTypes) {
      size += getRowLayoutSize(fieldType);
    }
    size += NullBuffer::getNumBytesForNullValues(fieldsTypes.size());
    return size;
  }
  default:
    return PhysicalTypeUtils::getFixedTypeSize(physType);
  }
}

bool LogicalTypeUtils::isDate(const DataType& dataType) {
  return isDate(dataType.id());
}

bool LogicalTypeUtils::isDate(DataTypeId dataType) {
  return dataType == DataTypeId::kDate;
}

bool LogicalTypeUtils::isTimestamp(const DataType& dataType) {
  return isTimestamp(dataType.id());
}

bool LogicalTypeUtils::isTimestamp(DataTypeId dataType) {
  switch (dataType) {
  case DataTypeId::kTimestampMs:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::isUnsigned(const DataType& dataType) {
  return isUnsigned(dataType.id());
}

bool LogicalTypeUtils::isUnsigned(DataTypeId dataType) {
  switch (dataType) {
  case DataTypeId::kUInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt8:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::isIntegral(const DataType& dataType) {
  return isIntegral(dataType.id());
}

bool LogicalTypeUtils::isIntegral(DataTypeId dataType) {
  switch (dataType) {
  case DataTypeId::kInt64:
  case DataTypeId::kInt32:
  case DataTypeId::kInt16:
  case DataTypeId::kInt8:
  case DataTypeId::kUInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt8:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::isNumerical(const DataType& dataType) {
  return isNumerical(dataType.id());
}

bool LogicalTypeUtils::isNumerical(DataTypeId dataType) {
  switch (dataType) {
  case DataTypeId::kInt64:
  case DataTypeId::kInt32:
  case DataTypeId::kInt16:
  case DataTypeId::kInt8:
  case DataTypeId::kUInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt8:
  case DataTypeId::kDouble:
  case DataTypeId::kFloat:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::isFloatingPoint(DataTypeId dataType) {
  switch (dataType) {
  case DataTypeId::kDouble:
  case DataTypeId::kFloat:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::isNested(const DataType& dataType) {
  return isNested(dataType.id());
}

bool LogicalTypeUtils::isNested(DataTypeId logicalTypeID) {
  switch (logicalTypeID) {
  case DataTypeId::kStruct:
  case DataTypeId::kList:
  case DataTypeId::kArray:
  case DataTypeId::kMap:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
    return true;
  default:
    return false;
  }
}

std::vector<DataTypeId> LogicalTypeUtils::getAllValidComparableLogicalTypes() {
  return std::vector<DataTypeId>{
      DataTypeId::kBoolean,     DataTypeId::kInt64,    DataTypeId::kInt32,
      DataTypeId::kInt16,       DataTypeId::kInt8,     DataTypeId::kUInt64,
      DataTypeId::kUInt32,      DataTypeId::kUInt16,   DataTypeId::kUInt8,
      DataTypeId::kDouble,      DataTypeId::kFloat,    DataTypeId::kDate,
      DataTypeId::kTimestampMs, DataTypeId::kInterval, DataTypeId::kVarchar};
}

std::vector<DataTypeId> LogicalTypeUtils::getIntegerTypeIDs() {
  return std::vector<DataTypeId>{DataTypeId::kInt64,  DataTypeId::kInt32,
                                 DataTypeId::kInt16,  DataTypeId::kInt8,
                                 DataTypeId::kUInt64, DataTypeId::kUInt32,
                                 DataTypeId::kUInt16, DataTypeId::kUInt8};
}

std::vector<DataTypeId> LogicalTypeUtils::getFloatingPointTypeIDs() {
  return std::vector<DataTypeId>{DataTypeId::kDouble, DataTypeId::kFloat};
}

std::vector<DataTypeId> LogicalTypeUtils::getNumericalDataTypeIds() {
  auto integerTypes = getIntegerTypeIDs();
  auto floatingPointTypes = getFloatingPointTypeIDs();
  integerTypes.insert(integerTypes.end(), floatingPointTypes.begin(),
                      floatingPointTypes.end());
  return integerTypes;
}

std::vector<DataTypeId> LogicalTypeUtils::getAllValidLogicTypeIDs() {
  return std::vector<DataTypeId>{
      DataTypeId::kInternalId, DataTypeId::kBoolean,     DataTypeId::kInt64,
      DataTypeId::kInt32,      DataTypeId::kInt16,       DataTypeId::kInt8,
      DataTypeId::kUInt64,     DataTypeId::kUInt32,      DataTypeId::kUInt16,
      DataTypeId::kUInt8,      DataTypeId::kDouble,      DataTypeId::kVarchar,
      DataTypeId::kDate,       DataTypeId::kTimestampMs, DataTypeId::kInterval,
      DataTypeId::kList,       DataTypeId::kArray,       DataTypeId::kMap,
      DataTypeId::kFloat,      DataTypeId::kVertex,      DataTypeId::kEdge,
      DataTypeId::kPath,       DataTypeId::kStruct};
}

std::vector<DataType> LogicalTypeUtils::getAllValidLogicTypes() {
  std::vector<DataType> typeVec;
  typeVec.push_back(DataType(DataTypeId::kInternalId));
  typeVec.push_back(DataType(DataTypeId::kBoolean));
  typeVec.push_back(DataType(DataTypeId::kInt32));
  typeVec.push_back(DataType(DataTypeId::kInt64));
  typeVec.push_back(DataType(DataTypeId::kInt16));
  typeVec.push_back(DataType(DataTypeId::kInt8));
  typeVec.push_back(DataType(DataTypeId::kUInt64));
  typeVec.push_back(DataType(DataTypeId::kUInt32));
  typeVec.push_back(DataType(DataTypeId::kUInt16));
  typeVec.push_back(DataType(DataTypeId::kUInt8));
  typeVec.push_back(DataType(DataTypeId::kDouble));
  typeVec.push_back(DataType(DataTypeId::kVarchar));
  typeVec.push_back(DataType(DataTypeId::kDate));
  typeVec.push_back(DataType(DataTypeId::kTimestampMs));
  typeVec.push_back(DataType(DataTypeId::kInterval));
  typeVec.push_back(DataType::List(DataType(DataTypeId::kUnknown)));
  typeVec.push_back(DataType::Array(DataType(DataTypeId::kUnknown), 0));
  typeVec.push_back(DataType::Map(DataType(DataTypeId::kUnknown),
                                  DataType(DataTypeId::kUnknown)));
  typeVec.push_back(DataType(DataTypeId::kFloat));
  typeVec.push_back(
      DataType(DataTypeId::kVertex,
               std::make_shared<StructTypeInfo>(std::vector<DataType>{})));
  typeVec.push_back(
      DataType(DataTypeId::kEdge,
               std::make_shared<StructTypeInfo>(std::vector<DataType>{})));
  typeVec.push_back(DataType::Struct(std::vector<DataType>{}));
  return typeVec;
}

// ============================================================================
// tryGetIDFromString - maps type name strings to DataTypeId
// ============================================================================

bool tryGetIDFromString(const std::string& str, DataTypeId& id) {
  auto upperStr = StringUtils::getUpper(str);
  if ("INTERNAL_ID" == upperStr) {
    id = DataTypeId::kInternalId;
  } else if ("INT64" == upperStr) {
    id = DataTypeId::kInt64;
  } else if ("INT32" == upperStr || "INT" == upperStr) {
    id = DataTypeId::kInt32;
  } else if ("INT16" == upperStr) {
    id = DataTypeId::kInt16;
  } else if ("INT8" == upperStr) {
    id = DataTypeId::kInt8;
  } else if ("UINT64" == upperStr) {
    id = DataTypeId::kUInt64;
  } else if ("UINT32" == upperStr) {
    id = DataTypeId::kUInt32;
  } else if ("UINT16" == upperStr) {
    id = DataTypeId::kUInt16;
  } else if ("UINT8" == upperStr) {
    id = DataTypeId::kUInt8;
  } else if ("INT128" == upperStr) {
    id = DataTypeId::kInt64;
  } else if ("DOUBLE" == upperStr || "FLOAT8" == upperStr) {
    id = DataTypeId::kDouble;
  } else if ("FLOAT" == upperStr || "FLOAT4" == upperStr ||
             "REAL" == upperStr) {
    id = DataTypeId::kFloat;
  } else if ("BOOLEAN" == upperStr || "BOOL" == upperStr) {
    id = DataTypeId::kBoolean;
  } else if ("STRING" == upperStr) {
    id = DataTypeId::kVarchar;
  } else if ("DATE" == upperStr) {
    id = DataTypeId::kDate;
  } else if ("TIMESTAMP" == upperStr) {
    id = DataTypeId::kTimestampMs;
  } else if ("TIMESTAMP_MS" == upperStr) {
    id = DataTypeId::kTimestampMs;
  } else if ("INTERVAL" == upperStr || "DURATION" == upperStr) {
    id = DataTypeId::kInterval;
  } else {
    return false;
  }
  return true;
}

// ============================================================================
// Parse helpers for convertFromString
// ============================================================================

std::vector<std::string> parseStructFields(const std::string& structTypeStr) {
  std::vector<std::string> structFieldsStr;
  auto startPos = 0u;
  auto curPos = 0u;
  auto numOpenBrackets = 0u;
  while (curPos < structTypeStr.length()) {
    switch (structTypeStr[curPos]) {
    case '(': {
      numOpenBrackets++;
    } break;
    case ')': {
      numOpenBrackets--;
    } break;
    case ',': {
      if (numOpenBrackets == 0) {
        structFieldsStr.push_back(StringUtils::ltrim(
            structTypeStr.substr(startPos, curPos - startPos)));
        startPos = curPos + 1;
      }
    } break;
    default: {
    }
    }
    curPos++;
  }
  structFieldsStr.push_back(
      StringUtils::ltrim(structTypeStr.substr(startPos, curPos - startPos)));
  return structFieldsStr;
}

DataType parseListType(const std::string& trimmedStr,
                       main::ClientContext* context) {
  return DataType::List(
      convertFromString(trimmedStr.substr(0, trimmedStr.size() - 2), context));
}

DataType parseArrayType(const std::string& trimmedStr,
                        main::ClientContext* context) {
  auto leftBracketPos = trimmedStr.find_last_of('[');
  auto rightBracketPos = trimmedStr.find_last_of(']');
  auto childType =
      convertFromString(trimmedStr.substr(0, leftBracketPos), context);
  auto numElements = std::strtoll(
      trimmedStr
          .substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1)
          .c_str(),
      nullptr, 0 /* base */);
  if (numElements <= 0) {
    THROW_BINDER_EXCEPTION(
        "The number of elements in an array must be greater than 0. Given: " +
        std::to_string(numElements) + ".");
  }
  return DataType::Array(std::move(childType), numElements);
}

DataType parseStructType(const std::string& trimmedStr,
                         main::ClientContext* context) {
  auto leftBracketPos = trimmedStr.find('(');
  auto rightBracketPos = trimmedStr.find_last_of(')');
  if (leftBracketPos == std::string::npos ||
      rightBracketPos == std::string::npos) {
    THROW_EXCEPTION_WITH_FILE_LINE("Cannot parse struct type: " + trimmedStr);
  }
  auto structFieldsStr = trimmedStr.substr(
      leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
  std::vector<std::string> fieldNames;
  std::vector<DataType> fieldTypes;
  auto structFieldStrs = parseStructFields(structFieldsStr);
  for (auto& structFieldStr : structFieldStrs) {
    auto pos = structFieldStr.find(' ');
    fieldNames.push_back(structFieldStr.substr(0, pos));
    fieldTypes.push_back(
        convertFromString(structFieldStr.substr(pos + 1), context));
  }
  return DataType::Struct(std::move(fieldNames), std::move(fieldTypes));
}

DataType parseMapType(const std::string& trimmedStr,
                      main::ClientContext* context) {
  auto leftBracketPos = trimmedStr.find('(');
  auto rightBracketPos = trimmedStr.find_last_of(')');
  if (leftBracketPos == std::string::npos ||
      rightBracketPos == std::string::npos) {
    THROW_EXCEPTION_WITH_FILE_LINE("Cannot parse map type: " + trimmedStr);
  }
  auto mapTypeStr = trimmedStr.substr(leftBracketPos + 1,
                                      rightBracketPos - leftBracketPos - 1);
  auto keyValueTypes = StringUtils::splitComma(mapTypeStr);
  return DataType::Map(convertFromString(keyValueTypes[0], context),
                       convertFromString(keyValueTypes[1], context));
}

DataType parseStringType(const std::string& trimmedStr) {
  auto leftBracketPos = trimmedStr.find('(');
  auto rightBracketPos = trimmedStr.find_last_of(')');
  if (leftBracketPos == std::string::npos ||
      rightBracketPos == std::string::npos) {
    // Bare "VARCHAR" without max_length — use default
    return DataType(DataTypeId::kVarchar);
  }
  auto maxLenStr = StringUtils::ltrim(StringUtils::rtrim(trimmedStr.substr(
      leftBracketPos + 1, rightBracketPos - leftBracketPos - 1)));
  char* endPtr = nullptr;
  auto maxLen = std::strtoll(maxLenStr.c_str(), &endPtr, 10);
  if (endPtr == maxLenStr.c_str() || *endPtr != '\0') {
    THROW_BINDER_EXCEPTION(
        "The max length of string must be a positive integer. Given: " +
        maxLenStr);
  }
  return DataType::Varchar(maxLen);
}

// ============================================================================
// isBuiltInType / convertFromString - standalone functions
// ============================================================================

bool isBuiltInType(const std::string& str) {
  auto trimmedStr = StringUtils::ltrim(StringUtils::rtrim(str));
  auto upperDataTypeString = StringUtils::getUpper(trimmedStr);
  auto id = DataTypeId::kUnknown;
  try {
    if (upperDataTypeString.ends_with("[]")) {
      parseListType(trimmedStr);
    } else if (upperDataTypeString.starts_with("LIST<") &&
               upperDataTypeString.ends_with(">")) {
      // LIST<T> format
    } else if (upperDataTypeString.ends_with("]")) {
      parseArrayType(trimmedStr);
    } else if (upperDataTypeString.starts_with("STRUCT")) {
      parseStructType(trimmedStr);
    } else if (upperDataTypeString.starts_with("MAP")) {
      parseMapType(trimmedStr);
    } else if (!tryGetIDFromString(upperDataTypeString, id)) {
      return false;
    }
  } catch (...) { return false; }
  return true;
}

DataType convertFromString(const std::string& str,
                           main::ClientContext* context) {
  auto trimmedStr = StringUtils::ltrim(StringUtils::rtrim(str));
  auto upperDataTypeString = StringUtils::getUpper(trimmedStr);
  if (upperDataTypeString.ends_with("[]")) {
    return parseListType(trimmedStr, context);
  } else if (upperDataTypeString.starts_with("LIST<") &&
             upperDataTypeString.ends_with(">")) {
    auto innerStr = trimmedStr.substr(5, trimmedStr.size() - 6);
    return DataType::List(convertFromString(innerStr, context));
  } else if (upperDataTypeString.ends_with("]")) {
    return parseArrayType(trimmedStr, context);
  } else if (upperDataTypeString.starts_with("STRUCT")) {
    return parseStructType(trimmedStr, context);
  } else if (upperDataTypeString.starts_with("MAP")) {
    return parseMapType(trimmedStr, context);
  } else if (upperDataTypeString == "STRING") {
    return DataType::Varchar();
  } else if (upperDataTypeString.starts_with("VARCHAR")) {
    return parseStringType(trimmedStr);
  } else {
    DataTypeId id;
    if (tryGetIDFromString(upperDataTypeString, id)) {
      return DataType(id);
    } else if (context != nullptr) {
      return context->getCatalog()->getType(context->getTransaction(),
                                            upperDataTypeString);
    } else {
      THROW_RUNTIME_ERROR("Invalid datatype string: " + str);
    }
  }
}

// ============================================================================
// tryGetMaxLogicalType support functions
// ============================================================================

static bool tryCombineListTypes(const DataType& left, const DataType& right,
                                DataType& result) {
  DataType childType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(ListType::GetChildType(left),
                                              ListType::GetChildType(right),
                                              childType)) {
    return false;
  }
  result = DataType::List(std::move(childType));
  return true;
}

static bool tryCombineArrayTypes(const DataType& left, const DataType& right,
                                 DataType& result) {
  if (ArrayType::GetNumElements(left) != ArrayType::GetNumElements(right)) {
    return tryCombineListTypes(left, right, result);
  }
  DataType childType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(ArrayType::GetChildType(left),
                                              ArrayType::GetChildType(right),
                                              childType)) {
    return false;
  }
  result =
      DataType::Array(std::move(childType), ArrayType::GetNumElements(left));
  return true;
}

static bool tryCombineListArrayTypes(const DataType& left,
                                     const DataType& right, DataType& result) {
  DataType childType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(ListType::GetChildType(left),
                                              ArrayType::GetChildType(right),
                                              childType)) {
    return false;
  }
  result = DataType::List(std::move(childType));
  return true;
}

static bool tryCombineStructTypes(const DataType& left, const DataType& right,
                                  DataType& result) {
  const auto& leftNames = StructType::GetFieldNames(left);
  const auto& leftTypes = StructType::GetChildTypes(left);
  const auto& rightNames = StructType::GetFieldNames(right);
  const auto& rightTypes = StructType::GetChildTypes(right);
  if (leftNames.size() != rightNames.size()) {
    return false;
  }
  std::vector<std::string> newNames;
  std::vector<DataType> newTypes;
  for (auto i = 0u; i < leftNames.size(); i++) {
    if (leftNames[i] != rightNames[i]) {
      return false;
    }
    DataType combinedType;
    if (LogicalTypeUtils::tryGetMaxLogicalType(leftTypes[i], rightTypes[i],
                                               combinedType)) {
      newNames.push_back(leftNames[i]);
      newTypes.push_back(std::move(combinedType));
    } else {
      return false;
    }
  }
  result = DataType::Struct(std::move(newNames), std::move(newTypes));
  return true;
}

static bool tryCombineMapTypes(const DataType& left, const DataType& right,
                               DataType& result) {
  const auto& leftKeyType = MapType::GetKeyType(left);
  const auto& leftValueType = MapType::GetValueType(left);
  const auto& rightKeyType = MapType::GetKeyType(right);
  const auto& rightValueType = MapType::GetValueType(right);
  DataType resultKeyType, resultValueType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(leftKeyType, rightKeyType,
                                              resultKeyType) ||
      !LogicalTypeUtils::tryGetMaxLogicalType(leftValueType, rightValueType,
                                              resultValueType)) {
    return false;
  }
  result = DataType::Map(std::move(resultKeyType), std::move(resultValueType));
  return true;
}

static DataTypeId joinToWiderType(DataTypeId left, DataTypeId right) {
  NEUG_ASSERT(LogicalTypeUtils::isIntegral(left));
  NEUG_ASSERT(LogicalTypeUtils::isIntegral(right));
  if (PhysicalTypeUtils::getFixedTypeSize(getPhysicalType(left)) >
      PhysicalTypeUtils::getFixedTypeSize(getPhysicalType(right))) {
    return left;
  } else {
    return right;
  }
}

static bool tryUnsignedToSigned(DataTypeId input, DataTypeId& result) {
  switch (input) {
  case DataTypeId::kUInt8:
    result = DataTypeId::kInt16;
    break;
  case DataTypeId::kUInt16:
    result = DataTypeId::kInt32;
    break;
  case DataTypeId::kUInt32:
    result = DataTypeId::kInt64;
    break;
  case DataTypeId::kUInt64:
    result = DataTypeId::kInt64;
    break;
  default:
    return false;
  }
  return true;
}

static DataTypeId joinDifferentSignIntegrals(DataTypeId signedType,
                                             DataTypeId unsignedType) {
  auto unsignedToSigned = DataTypeId::kUnknown;
  if (!tryUnsignedToSigned(unsignedType, unsignedToSigned)) {
    return DataTypeId::kDouble;
  } else {
    return joinToWiderType(signedType, unsignedToSigned);
  }
}

static uint32_t internalTimeOrder(DataTypeId type) {
  switch (type) {
  case DataTypeId::kDate:
    return 50;
  case DataTypeId::kTimestampMs:
    return 52;
  default:
    return 0;
  }
}

static int alwaysCastOrder(DataTypeId typeID) {
  switch (typeID) {
  case DataTypeId::kUnknown:
    return 0;
  case DataTypeId::kVarchar:
    return 2;
  default:
    return -1;
  }
}

static bool canAlwaysCast(DataTypeId typeID) {
  switch (typeID) {
  case DataTypeId::kUnknown:
  case DataTypeId::kVarchar:
    return true;
  default:
    return false;
  }
}

bool LogicalTypeUtils::tryGetMaxDataTypeId(DataTypeId left, DataTypeId right,
                                           DataTypeId& result) {
  if (canAlwaysCast(left) && canAlwaysCast(right)) {
    if (alwaysCastOrder(left) > alwaysCastOrder(right)) {
      result = left;
    } else {
      result = right;
    }
    return true;
  }
  if (left == right || canAlwaysCast(left)) {
    result = right;
    return true;
  }
  if (canAlwaysCast(right)) {
    result = left;
    return true;
  }
  auto leftToRight = BuiltInFunctionsUtils::getCastCost(left, right);
  auto rightToLeft = BuiltInFunctionsUtils::getCastCost(right, left);
  if (leftToRight != UNDEFINED_CAST_COST ||
      rightToLeft != UNDEFINED_CAST_COST) {
    if (leftToRight < rightToLeft) {
      result = right;
    } else {
      result = left;
    }
    return true;
  }
  if (isIntegral(left) && isIntegral(right)) {
    if (isUnsigned(left) && !isUnsigned(right)) {
      result = joinDifferentSignIntegrals(right, left);
      return true;
    } else if (isUnsigned(right) && !isUnsigned(left)) {
      result = joinDifferentSignIntegrals(left, right);
      return true;
    }
  }

  auto leftOrder = internalTimeOrder(left);
  auto rightOrder = internalTimeOrder(right);
  if (leftOrder && rightOrder) {
    if (leftOrder > rightOrder) {
      result = left;
    } else {
      result = right;
    }
    return true;
  }

  return false;
}

static inline bool isSemanticallyNested(DataTypeId ID) {
  return LogicalTypeUtils::isNested(ID);
}

bool LogicalTypeUtils::tryGetMaxLogicalType(const DataType& left,
                                            const DataType& right,
                                            DataType& result) {
  if (canAlwaysCast(left.id()) && canAlwaysCast(right.id())) {
    if (alwaysCastOrder(left.id()) > alwaysCastOrder(right.id())) {
      result = left.copy();
    } else {
      result = right.copy();
    }
    return true;
  }
  if (left == right || canAlwaysCast(left.id())) {
    result = right.copy();
    return true;
  }
  if (canAlwaysCast(right.id())) {
    result = left.copy();
    return true;
  }
  if (isSemanticallyNested(left.id()) || isSemanticallyNested(right.id())) {
    if (left.id() == DataTypeId::kList && right.id() == DataTypeId::kArray) {
      return tryCombineListArrayTypes(left, right, result);
    } else if (left.id() == DataTypeId::kArray &&
               right.id() == DataTypeId::kList) {
      return tryCombineListArrayTypes(right, left, result);
    } else if (left.id() != right.id()) {
      return false;
    }
    switch (left.id()) {
    case DataTypeId::kList:
      return tryCombineListTypes(left, right, result);
    case DataTypeId::kArray:
      return tryCombineArrayTypes(left, right, result);
    case DataTypeId::kStruct:
      return tryCombineStructTypes(left, right, result);
    case DataTypeId::kMap:
      return tryCombineMapTypes(left, right, result);
    default:
      NEUG_UNREACHABLE;
    }
  }
  auto resultID = DataTypeId::kUnknown;
  if (!tryGetMaxDataTypeId(left.id(), right.id(), resultID)) {
    return false;
  }
  if (resultID == left.id()) {
    result = left.copy();
  } else if (resultID == right.id()) {
    result = right.copy();
  } else {
    result = DataType(resultID);
  }
  return true;
}

bool LogicalTypeUtils::tryGetMaxLogicalType(const std::vector<DataType>& types,
                                            DataType& result) {
  DataType combinedType(DataTypeId::kUnknown);
  for (auto& type : types) {
    if (!tryGetMaxLogicalType(combinedType, type, combinedType)) {
      return false;
    }
  }
  result = combinedType.copy();
  return true;
}

DataType LogicalTypeUtils::combineTypes(const DataType& lft,
                                        const DataType& rit) {
  if (lft.id() == DataTypeId::kVarchar || rit.id() == DataTypeId::kVarchar) {
    return DataType(DataTypeId::kVarchar);
  }
  if (isSemanticallyNested(lft.id()) && isSemanticallyNested(rit.id())) {}
  if (lft.id() == rit.id() && lft.id() == DataTypeId::kStruct) {
    const auto& lftNames = StructType::GetFieldNames(lft);
    const auto& lftTypes = StructType::GetChildTypes(lft);
    std::vector<std::string> resultNames;
    std::vector<DataType> resultTypes;
    for (size_t i = 0; i < lftNames.size(); i++) {
      auto name = lftNames[i];
      if (StructType::HasField(rit, name)) {
        auto idx = StructType::GetFieldIdx(rit, name);
        resultNames.push_back(name);
        resultTypes.push_back(
            combineTypes(lftTypes[i], StructType::GetChildType(rit, idx)));
      } else {
        resultNames.push_back(name);
        resultTypes.push_back(lftTypes[i]);
      }
    }
    const auto& ritNames = StructType::GetFieldNames(rit);
    const auto& ritTypes = StructType::GetChildTypes(rit);
    for (size_t i = 0; i < ritNames.size(); i++) {
      if (!StructType::HasField(lft, ritNames[i])) {
        resultNames.push_back(ritNames[i]);
        resultTypes.push_back(ritTypes[i]);
      }
    }
    return DataType::Struct(std::move(resultNames), std::move(resultTypes));
  }
  if (lft.id() == rit.id() && lft.id() == DataTypeId::kList) {
    const auto& lftChild = ListType::GetChildType(lft);
    const auto& ritChild = ListType::GetChildType(rit);
    return DataType::List(combineTypes(lftChild, ritChild));
  }
  if (lft.id() == rit.id() && lft.id() == DataTypeId::kMap) {
    const auto& lftKey = MapType::GetKeyType(lft);
    const auto& lftValue = MapType::GetValueType(lft);
    const auto& ritKey = MapType::GetKeyType(rit);
    const auto& ritValue = MapType::GetValueType(rit);
    return DataType::Map(combineTypes(lftKey, ritKey),
                         combineTypes(lftValue, ritValue));
  }
  DataType result;
  if (!tryGetMaxLogicalType(lft, rit, result)) {
    return DataType(DataTypeId::kVarchar);
  }
  return result;
}

DataType LogicalTypeUtils::purgeAny(const DataType& type,
                                    const DataType& replacement) {
  switch (type.id()) {
  case DataTypeId::kUnknown:
    return replacement.copy();
  case DataTypeId::kList:
    return DataType::List(purgeAny(ListType::GetChildType(type), replacement));
  case DataTypeId::kArray:
    return DataType::Array(purgeAny(ArrayType::GetChildType(type), replacement),
                           ArrayType::GetNumElements(type));
  case DataTypeId::kMap:
    return DataType::Map(purgeAny(MapType::GetKeyType(type), replacement),
                         purgeAny(MapType::GetValueType(type), replacement));
  case DataTypeId::kStruct: {
    const auto& names = StructType::GetFieldNames(type);
    const auto& types = StructType::GetChildTypes(type);
    std::vector<std::string> newNames;
    std::vector<DataType> newTypes;
    for (size_t i = 0; i < names.size(); i++) {
      newNames.push_back(names[i]);
      newTypes.push_back(purgeAny(types[i], replacement));
    }
    return DataType::Struct(std::move(newNames), std::move(newTypes));
  }
  default:
    return type.copy();
  }
}

}  // namespace common
}  // namespace neug
