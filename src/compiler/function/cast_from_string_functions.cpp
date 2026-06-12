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

#include "neug/compiler/function/cast/functions/cast_from_string_functions.h"

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/function/list/functions/list_unique_function.h"
#include "neug/utils/exception/exception.h"
#include "utf8proc_wrapper.h"

using namespace neug::common;

namespace neug {
namespace function {

// ---------------------- cast String Helper ------------------------------ //
struct CastStringHelper {
  template <typename T>
  static void cast(const char* input, uint64_t len, T& result,
                   ValueVector* /*vector*/ = nullptr, uint64_t /*rowToAdd*/ = 0,
                   const CSVOption* /*option*/ = nullptr) {
    simpleIntegerCast<int64_t>(input, len, result, DataTypeId::kInt64);
  }
};

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   int128_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleInt128Cast(input, len, result);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   int32_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<int32_t>(input, len, result, DataTypeId::kInt32);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   int16_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<int16_t>(input, len, result, DataTypeId::kInt16);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   int8_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<int8_t>(input, len, result, DataTypeId::kInt8);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   uint64_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<uint64_t, false>(input, len, result, DataTypeId::kUInt64);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   uint32_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<uint32_t, false>(input, len, result, DataTypeId::kUInt32);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   uint16_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<uint16_t, false>(input, len, result, DataTypeId::kUInt16);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   uint8_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  simpleIntegerCast<uint8_t, false>(input, len, result, DataTypeId::kUInt8);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   float& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  doubleCast<float>(input, len, result, DataTypeId::kFloat);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   double& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  doubleCast<double>(input, len, result, DataTypeId::kDouble);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   bool& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  castStringToBool(input, len, result);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   date_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  result = Date::fromCString(input, len);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   timestamp_ms_t& result,
                                   ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  TryCastStringToTimestamp::cast<timestamp_ms_t>(input, len, result,
                                                 DataTypeId::kTimestampMs);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   neug::common::timestamp_t& result,
                                   ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  result = Timestamp::fromCString(input, len);
}

template <>
inline void CastStringHelper::cast(const char* input, uint64_t len,
                                   interval_t& result, ValueVector* /*vector*/,
                                   uint64_t /*rowToAdd*/,
                                   const CSVOption* /*option*/) {
  result = neug::common::Interval::fromCString(input, len);
}

// ---------------------- cast String to nested types
// ------------------------------ //
static void skipWhitespace(const char*& input, const char* end) {
  while (input < end) {
    if (*input & 0x80) {
      // We only skip ASCII white spaces there.
      break;
    } else {
      NEUG_ASSERT(*input >= -1);
      if (!isspace(*input)) {
        break;
      }
    }
    input++;
  }
}

static void trimRightWhitespace(const char* input, const char*& end) {
  while (input < end && isspace(*(end - 1))) {
    end--;
  }
}

static void trimQuotes(const char*& keyStart, const char*& keyEnd) {
  // Skip quotations on struct keys.
  if ((keyStart[0] == '\'' && (keyEnd - 1)[0] == '\'') ||
      (keyStart[0] == '\"' && (keyEnd - 1)[0] == '\"')) {
    keyStart++;
    keyEnd--;
  }
}

static bool skipToCloseQuotes(const char*& input, const char* end) {
  auto ch = *input;
  input++;  // skip the first " '
  // TODO: escape char
  while (input != end) {
    if (*input == ch) {
      return true;
    }
    input++;
  }
  return false;
}

static bool skipToClose(const char*& input, const char* end, uint64_t& lvl,
                        char target, const CSVOption* option) {
  input++;
  while (input != end) {
    if (*input == '\'') {
      if (!skipToCloseQuotes(input, end)) {
        return false;
      }
    } else if (*input ==
               '{') {  // must have closing brackets {, ] if they are not quoted
      if (!skipToClose(input, end, lvl, '}', option)) {
        return false;
      }
    } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
      if (!skipToClose(input, end, lvl,
                       CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
        return false;
      }
      lvl++;  // nested one more level
    } else if (*input == target) {
      if (target == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {
        lvl--;
      }
      return true;
    }
    input++;
  }
  return false;  // no corresponding closing bracket
}

static bool isNull(std::string_view& str) {
  auto start = str.data();
  auto end = start + str.length();
  skipWhitespace(start, end);
  if (start == end) {
    return true;
  }
  if (end - start >= 4 && (*start == 'N' || *start == 'n') &&
      (*(start + 1) == 'U' || *(start + 1) == 'u') &&
      (*(start + 2) == 'L' || *(start + 2) == 'l') &&
      (*(start + 3) == 'L' || *(start + 3) == 'l')) {
    start += 4;
    skipWhitespace(start, end);
    if (start == end) {
      return true;
    }
  }
  return false;
}

// ---------------------- cast String to List Helper
// ------------------------------ //
struct CountPartOperation {
  uint64_t count = 0;

  static inline bool handleKey(const char* /*start*/, const char* /*end*/,
                               const CSVOption* /*config*/) {
    return true;
  }
  inline void handleValue(const char* /*start*/, const char* /*end*/,
                          const CSVOption* /*config*/) {
    count++;
  }
};

struct SplitStringListOperation {
  SplitStringListOperation(uint64_t& offset, ValueVector* resultVector)
      : offset(offset), resultVector(resultVector) {}

  uint64_t& offset;
  ValueVector* resultVector;

  void handleValue(const char* start, const char* end,
                   const CSVOption* option) {
    skipWhitespace(start, end);
    trimRightWhitespace(start, end);
    CastString::copyStringToVector(
        resultVector, offset, std::string_view{start, (uint32_t)(end - start)},
        option);
    offset++;
  }
};

template <typename T>
static bool splitCStringList(const char* input, uint64_t len, T& state,
                             const CSVOption* option) {
  auto end = input + len;
  uint64_t lvl = 1;
  bool seen_value = false;

  // locate [
  skipWhitespace(input, end);
  if (input == end || *input != CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
    return false;
  }
  skipWhitespace(++input, end);

  auto start_ptr = input;
  while (input < end) {
    auto ch = *input;
    if (ch == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
      if (!skipToClose(input, end, ++lvl,
                       CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
        return false;
      }
    } else if (ch == '\'' || ch == '"') {
      if (!skipToCloseQuotes(input, end)) {
        return false;
      }
    } else if (ch == '{') {
      uint64_t struct_lvl = 0;
      skipToClose(input, end, struct_lvl, '}', option);
    } else if (ch == ',' ||
               ch == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {  // split
      if (ch != CopyConstants::DEFAULT_CSV_LIST_END_CHAR || start_ptr < input ||
          seen_value) {
        state.handleValue(start_ptr, input, option);
        seen_value = true;
      }
      if (ch == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {  // last ]
        lvl--;
        break;
      }
      skipWhitespace(++input, end);
      start_ptr = input;
      continue;
    }
    input++;
  }
  skipWhitespace(++input, end);
  return (input == end && lvl == 0);
}

template <typename T>
static bool splitPossibleUnbracedList(std::string_view input, T& state,
                                      const CSVOption* option) {
  input = StringUtils::ltrim(StringUtils::rtrim(input));
  auto split = StringUtils::smartSplit(input, ';');
  if (split.size() == 1 && input.front() == '[' && input.back() == ']') {
    split = StringUtils::smartSplit(input.substr(1, input.size() - 2), ';');
  }
  for (auto& i : split) {
    state.handleValue(i.data(), i.data() + i.length(), option);
  }
  return true;
}

template <typename T>
static inline void startListCast(const char* input, uint64_t len, T split,
                                 const CSVOption* option, ValueVector* vector) {
  auto validList = option->allowUnbracedList
                       ? splitPossibleUnbracedList(std::string_view(input, len),
                                                   split, option)
                       : splitCStringList(input, len, split, option);
  if (!validList) {
    THROW_CONVERSION_EXCEPTION(
        "Cast failed. " + (std::string{input, (size_t) len}) + " is not in " +
        vector->dataType.ToString() + " range.");
  }
}

// ---------------------- cast String to Array Helper
// ------------------------------ //
static void validateNumElementsInArray(uint64_t numElementsRead,
                                       const DataType& type) {
  auto numElementsInArray = ArrayType::GetNumElements(type);
  if (numElementsRead != numElementsInArray) {
    THROW_CONVERSION_EXCEPTION(
        stringFormat("Each array should have fixed number of elements. "
                     "Expected: {}, Actual: {}.",
                     numElementsInArray, numElementsRead));
  }
}

// ---------------------- cast String to List/Array
// ------------------------------ //
template <>
void CastStringHelper::cast(const char* input, uint64_t len,
                            list_entry_t& /*result*/, ValueVector* vector,
                            uint64_t rowToAdd, const CSVOption* option) {
  auto logicalTypeID = vector->dataType.id();

  // calculate the number of elements in array
  CountPartOperation state;
  if (option->allowUnbracedList) {
    splitPossibleUnbracedList(std::string_view(input, len), state, option);
  } else {
    splitCStringList(input, len, state, option);
  }
  if (logicalTypeID == DataTypeId::kArray) {
    validateNumElementsInArray(state.count, vector->dataType);
  }

  auto list_entry = ListVector::addList(vector, state.count);
  vector->setValue<list_entry_t>(rowToAdd, list_entry);
  auto listDataVector = ListVector::getDataVector(vector);

  SplitStringListOperation split{list_entry.offset, listDataVector};
  startListCast(input, len, split, option, vector);
}

template <>
void CastString::operation(const neug_string_t& input, list_entry_t& result,
                           ValueVector* resultVector, uint64_t rowToAdd,
                           const CSVOption* option) {
  CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()),
                         input.len, result, resultVector, rowToAdd, option);
}

// ---------------------- cast String to Map ------------------------------ //
struct SplitStringMapOperation {
  SplitStringMapOperation(uint64_t& offset, ValueVector* resultVector)
      : offset{offset}, resultVector{resultVector} {}

  uint64_t& offset;
  ValueVector* resultVector;
  ValueSet uniqueKeys;

  // NOLINTNEXTLINE(readability-make-member-function-const): Semantically
  // non-const.
  bool handleKey(const char* start, const char* end, const CSVOption* option);

  void handleValue(const char* start, const char* end, const CSVOption* option);
};

bool SplitStringMapOperation::handleKey(const char* start, const char* end,
                                        const CSVOption* option) {
  trimRightWhitespace(start, end);
  auto fieldVector = StructVector::getFieldVector(resultVector, 0).get();
  CastString::copyStringToVector(
      fieldVector, offset, std::string_view{start, (uint32_t)(end - start)},
      option);
  if (fieldVector->isNull(offset)) {
    THROW_CONVERSION_EXCEPTION("Map does not allow null as key.");
  }
  auto val = common::Value::createDefaultValue(fieldVector->dataType);
  val.copyFromColLayout(
      fieldVector->getData() + fieldVector->getNumBytesPerValue() * offset,
      fieldVector);
  auto uniqueKey = uniqueKeys.insert(val).second;
  if (!uniqueKey) {
    THROW_CONVERSION_EXCEPTION("Map does not allow duplicate keys.");
  }
  return true;
}

void SplitStringMapOperation::handleValue(const char* start, const char* end,
                                          const CSVOption* option) {
  trimRightWhitespace(start, end);
  CastString::copyStringToVector(
      StructVector::getFieldVector(resultVector, 1).get(), offset++,
      std::string_view{start, (uint32_t)(end - start)}, option);
}

template <typename T>
static bool parseKeyOrValue(const char*& input, const char* end, T& state,
                            bool isKey, bool& closeBracket,
                            const CSVOption* option) {
  auto start = input;
  uint64_t lvl = 0;

  while (input < end) {
    if (*input == '"' || *input == '\'') {
      if (!skipToCloseQuotes(input, end)) {
        return false;
      }
    } else if (*input == '{') {
      if (!skipToClose(input, end, lvl, '}', option)) {
        return false;
      }
    } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
      if (!skipToClose(input, end, lvl,
                       CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
        return false;
      }
    } else if (isKey && *input == '=') {
      return state.handleKey(start, input, option);
    } else if (!isKey && (*input == ',' || *input == '}')) {
      state.handleValue(start, input, option);
      if (*input == '}') {
        closeBracket = true;
      }
      return true;
    }
    input++;
  }
  return false;
}

// Split map of format: {a=12,b=13}
template <typename T>
static bool splitCStringMap(const char* input, uint64_t len, T& state,
                            const CSVOption* option) {
  auto end = input + len;
  bool closeBracket = false;

  skipWhitespace(input, end);
  if (input == end || *input != '{') {  // start with {
    return false;
  }
  skipWhitespace(++input, end);
  if (input == end) {
    return false;
  }
  if (*input == '}') {
    skipWhitespace(++input, end);  // empty
    return input == end;
  }

  while (input < end) {
    if (!parseKeyOrValue(input, end, state, true, closeBracket, option)) {
      return false;
    }
    skipWhitespace(++input, end);
    if (!parseKeyOrValue(input, end, state, false, closeBracket, option)) {
      return false;
    }
    skipWhitespace(++input, end);
    if (closeBracket) {
      return (input == end);
    }
  }
  return false;
}

template <>
void CastStringHelper::cast(const char* input, uint64_t len,
                            map_entry_t& /*result*/, ValueVector* vector,
                            uint64_t rowToAdd, const CSVOption* option) {
  // count the number of maps in map
  CountPartOperation state;
  splitCStringMap(input, len, state, option);

  auto list_entry = ListVector::addList(vector, state.count);
  vector->setValue<list_entry_t>(rowToAdd, list_entry);
  auto structVector = ListVector::getDataVector(vector);

  SplitStringMapOperation split{list_entry.offset, structVector};
  if (!splitCStringMap(input, len, split, option)) {
    THROW_CONVERSION_EXCEPTION(
        "Cast failed. " + (std::string{input, (size_t) len}) + " is not in " +
        vector->dataType.ToString() + " range.");
  }
}

template <>
void CastString::operation(const neug_string_t& input, map_entry_t& result,
                           ValueVector* resultVector, uint64_t rowToAdd,
                           const CSVOption* option) {
  CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()),
                         input.len, result, resultVector, rowToAdd, option);
}

// ---------------------- cast String to Struct ------------------------------
static bool parseStructFieldName(const char*& input, const char* end) {
  while (input < end) {
    if (*input == ':') {
      return true;
    }
    input++;
  }
  return false;
}

static bool parseStructFieldValue(const char*& input, const char* end,
                                  const CSVOption* option, bool& closeBrack) {
  uint64_t lvl = 0;
  while (input < end) {
    if (*input == '"' || *input == '\'') {
      if (!skipToCloseQuotes(input, end)) {
        return false;
      }
    } else if (*input == '{') {
      if (!skipToClose(input, end, lvl, '}', option)) {
        return false;
      }
    } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
      if (!skipToClose(input, end, ++lvl,
                       CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
        return false;
      }
    } else if (*input == ',' || *input == '}') {
      if (*input == '}') {
        closeBrack = true;
      }
      return (lvl == 0);
    }
    input++;
  }
  return false;
}

static bool tryCastStringToStruct(const char* input, uint64_t len,
                                  ValueVector* vector, uint64_t rowToAdd,
                                  const CSVOption* option) {
  // default values to NULL
  auto fieldVectors = StructVector::getFieldVectors(vector);
  for (auto& fieldVector : fieldVectors) {
    fieldVector->setNull(rowToAdd, true);
  }

  // check if start with {
  auto end = input + len;
  const auto& type = vector->dataType;
  skipWhitespace(input, end);
  if (input == end || *input != '{') {
    return false;
  }
  skipWhitespace(++input, end);

  if (input == end) {  // no closing bracket
    return false;
  }
  if (*input == '}') {
    skipWhitespace(++input, end);
    return input == end;
  }

  bool closeBracket = false;
  while (input < end) {
    auto keyStart = input;
    if (!parseStructFieldName(input, end)) {  // find key
      return false;
    }
    auto keyEnd = input;
    trimRightWhitespace(keyStart, keyEnd);
    trimQuotes(keyStart, keyEnd);
    auto fieldIdx =
        StructType::GetFieldIdx(type, std::string{keyStart, keyEnd});
    if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
      THROW_PARSER_EXCEPTION("Invalid struct field name: " +
                             (std::string{keyStart, keyEnd}));
    }

    skipWhitespace(++input, end);
    auto valStart = input;
    if (!parseStructFieldValue(input, end, option,
                               closeBracket)) {  // find value
      return false;
    }
    auto valEnd = input;
    trimRightWhitespace(valStart, valEnd);
    trimQuotes(valStart, valEnd);
    skipWhitespace(++input, end);

    auto fieldVector = StructVector::getFieldVector(vector, fieldIdx).get();
    fieldVector->setNull(rowToAdd, false);
    CastString::copyStringToVector(
        fieldVector, rowToAdd,
        std::string_view{valStart, (uint32_t)(valEnd - valStart)}, option);

    if (closeBracket) {
      return (input == end);
    }
  }
  return false;
}

template <>
void CastStringHelper::cast(const char* input, uint64_t len,
                            struct_entry_t& /*result*/, ValueVector* vector,
                            uint64_t rowToAdd, const CSVOption* option) {
  if (!tryCastStringToStruct(input, len, vector, rowToAdd, option)) {
    THROW_CONVERSION_EXCEPTION(
        "Cast failed. " + (std::string{input, (size_t) len}) + " is not in " +
        vector->dataType.ToString() + " range.");
  }
}

template <>
void CastString::operation(const neug_string_t& input, struct_entry_t& result,
                           ValueVector* resultVector, uint64_t rowToAdd,
                           const CSVOption* option) {
  CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()),
                         input.len, result, resultVector, rowToAdd, option);
}

void CastString::copyStringToVector(ValueVector* vector, uint64_t vectorPos,
                                    std::string_view strVal,
                                    const CSVOption* option) {
  auto& type = vector->dataType;
  if (strVal.empty() || isNull(strVal) || isAnyType(strVal)) {
    vector->setNull(vectorPos, true /* isNull */);
    return;
  }
  vector->setNull(vectorPos, false /* isNull */);
  switch (type.id()) {
  // INT128 removed — no engine equivalent
  case DataTypeId::kInt64: {
    int64_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kInt32: {
    int32_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kInt16: {
    int16_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kInt8: {
    int8_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kUInt64: {
    uint64_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kUInt32: {
    uint32_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kUInt16: {
    uint16_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kUInt8: {
    uint8_t val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kFloat: {
    float val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kDouble: {
    double val = 0;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kBoolean: {
    bool val = false;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kVarchar: {
    if (!utf8proc::Utf8Proc::isValid(strVal.data(), strVal.length())) {
      THROW_CONVERSION_EXCEPTION("Invalid UTF8-encoded string.");
    }
    StringVector::addString(vector, vectorPos, strVal.data(), strVal.length());
  } break;
  case DataTypeId::kDate: {
    date_t val;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kTimestampMs: {
    timestamp_ms_t val;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  // TIMESTAMP removed — merged into kTimestampMs
  case DataTypeId::kInterval: {
    interval_t val;
    CastStringHelper::cast(strVal.data(), strVal.length(), val);
    vector->setValue(vectorPos, val);
  } break;
  case DataTypeId::kMap: {
    map_entry_t val;
    CastStringHelper::cast(strVal.data(), strVal.length(), val, vector,
                           vectorPos, option);
  } break;
  case DataTypeId::kArray:
  case DataTypeId::kList: {
    list_entry_t val;
    CastStringHelper::cast(strVal.data(), strVal.length(), val, vector,
                           vectorPos, option);
  } break;
  case DataTypeId::kStruct: {
    struct_entry_t val{};
    CastStringHelper::cast(strVal.data(), strVal.length(), val, vector,
                           vectorPos, option);
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

}  // namespace function
}  // namespace neug
