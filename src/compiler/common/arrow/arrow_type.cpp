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

#include "neug/compiler/common/arrow/arrow_converter.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

// Pyarrow format string specifications can be found here
// https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings

DataType ArrowConverter::fromArrowSchema(const ArrowSchema* schema) {
  const char* arrowType = schema->format;
  std::vector<std::string> structFieldNames;
  std::vector<DataType> structFieldTypes;
  // If we have a dictionary, then the logical type of the column is dependent
  // upon the logical type of the dict
  if (schema->dictionary != nullptr) {
    return fromArrowSchema(schema->dictionary);
  }
  switch (arrowType[0]) {
  case 'n':
    return DataType(DataTypeId::kUnknown);
  case 'b':
    return DataType(DataTypeId::kBoolean);
  case 'c':
    return DataType(DataTypeId::kInt8);
  case 'C':
    return DataType(DataTypeId::kUInt8);
  case 's':
    return DataType(DataTypeId::kInt16);
  case 'S':
    return DataType(DataTypeId::kUInt16);
  case 'i':
    return DataType(DataTypeId::kInt32);
  case 'I':
    return DataType(DataTypeId::kUInt32);
  case 'l':
    return DataType(DataTypeId::kInt64);
  case 'L':
    return DataType(DataTypeId::kUInt64);
  case 'e':
    THROW_NOT_IMPLEMENTED_EXCEPTION("16 bit floats are not supported");
  case 'f':
    return DataType(DataTypeId::kFloat);
  case 'g':
    return DataType(DataTypeId::kDouble);
  case 'u':
  case 'U':
    return DataType(DataTypeId::kVarchar);
  case 'v':
    switch (arrowType[1]) {
    case 'u':
      return DataType(DataTypeId::kVarchar);
    default:
      NEUG_UNREACHABLE;
    }
  case 't':
    switch (arrowType[1]) {
    case 'd':
      if (arrowType[2] == 'D') {
        return DataType(DataTypeId::kDate);
      } else {
        return DataType(DataTypeId::kTimestampMs);
      }
    case 't':
      // TODO implement pure time type
      THROW_NOT_IMPLEMENTED_EXCEPTION("Pure time types are not supported");
    case 's':
      // TODO maxwell: timezone support
      switch (arrowType[2]) {
      case 'm':
        return DataType(DataTypeId::kTimestampMs);
      case 'u':
        return DataType(DataTypeId::kTimestampMs);
      default:
        NEUG_UNREACHABLE;
      }
    case 'D':
      // duration
    case 'i':
      // interval
      return DataType(DataTypeId::kInterval);
    default:
      NEUG_UNREACHABLE;
    }
  case '+':
    NEUG_ASSERT(schema->n_children > 0);
    switch (arrowType[1]) {
    // complex types need a complementary ExtraTypeInfo object
    case 'l':
    case 'L':
      return DataType::List(DataType(fromArrowSchema(schema->children[0])));
    case 'w':
      return DataType::Array(DataType(fromArrowSchema(schema->children[0])),
                             std::stoul(arrowType + 3));
    case 's':
      for (int64_t i = 0; i < schema->n_children; i++) {
        structFieldNames.push_back(std::string(schema->children[i]->name));
        structFieldTypes.push_back(
            DataType(fromArrowSchema(schema->children[i])));
      }
      return DataType::Struct(std::move(structFieldNames),
                              std::move(structFieldTypes));
    case 'm':
      return DataType::Map(
          DataType(fromArrowSchema(schema->children[0]->children[0])),
          DataType(fromArrowSchema(schema->children[0]->children[1])));
    case 'v':
      switch (arrowType[2]) {
      case 'l':
      case 'L':
        return DataType::List(DataType(fromArrowSchema(schema->children[0])));
      default:
        NEUG_UNREACHABLE;
      }
    case 'r':
      // logical type corresponds to second child
      return fromArrowSchema(schema->children[1]);
    default:
      NEUG_UNREACHABLE;
    }
  default:
    NEUG_UNREACHABLE;
  }
  // refer to arrow_converted.cpp:65
}

}  // namespace common
}  // namespace neug
