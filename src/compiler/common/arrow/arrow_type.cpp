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

LogicalType ArrowConverter::fromArrowSchema(const ArrowSchema* schema) {
  const char* arrowType = schema->format;
  std::vector<StructField> structFields;
  // If we have a dictionary, then the logical type of the column is dependent
  // upon the logical type of the dict
  if (schema->dictionary != nullptr) {
    return fromArrowSchema(schema->dictionary);
  }
  switch (arrowType[0]) {
  case 'n':
    return LogicalType(LogicalTypeID::ANY);
  case 'b':
    return LogicalType(LogicalTypeID::BOOL);
  case 'c':
    return LogicalType(LogicalTypeID::INT8);
  case 'C':
    return LogicalType(LogicalTypeID::UINT8);
  case 's':
    return LogicalType(LogicalTypeID::INT16);
  case 'S':
    return LogicalType(LogicalTypeID::UINT16);
  case 'i':
    return LogicalType(LogicalTypeID::INT32);
  case 'I':
    return LogicalType(LogicalTypeID::UINT32);
  case 'l':
    return LogicalType(LogicalTypeID::INT64);
  case 'L':
    return LogicalType(LogicalTypeID::UINT64);
  case 'e':
    THROW_NOT_IMPLEMENTED_EXCEPTION("16 bit floats are not supported");
  case 'f':
    return LogicalType(LogicalTypeID::FLOAT);
  case 'g':
    return LogicalType(LogicalTypeID::DOUBLE);
  case 'u':
  case 'U':
    return LogicalType(LogicalTypeID::STRING);
  case 'v':
    switch (arrowType[1]) {
    case 'u':
      return LogicalType(LogicalTypeID::STRING);
    default:
      NEUG_UNREACHABLE;
    }
  case 't':
    switch (arrowType[1]) {
    case 'd':
      if (arrowType[2] == 'D') {
        return LogicalType(LogicalTypeID::DATE);
      } else {
        return LogicalType(LogicalTypeID::TIMESTAMP_MS);
      }
    case 't':
      // TODO implement pure time type
      THROW_NOT_IMPLEMENTED_EXCEPTION("Pure time types are not supported");
    case 's':
      // TODO maxwell: timezone support
      switch (arrowType[2]) {
      case 'm':
        return LogicalType(LogicalTypeID::TIMESTAMP_MS);
      case 'u':
        return LogicalType(LogicalTypeID::TIMESTAMP);
      default:
        NEUG_UNREACHABLE;
      }
    case 'D':
      // duration
    case 'i':
      // interval
      return LogicalType(LogicalTypeID::INTERVAL);
    default:
      NEUG_UNREACHABLE;
    }
  case '+':
    NEUG_ASSERT(schema->n_children > 0);
    switch (arrowType[1]) {
    // complex types need a complementary ExtraTypeInfo object
    case 'l':
    case 'L':
      return LogicalType::LIST(
          LogicalType(fromArrowSchema(schema->children[0])));
    case 'w':
      return LogicalType::ARRAY(
          LogicalType(fromArrowSchema(schema->children[0])),
          std::stoul(arrowType + 3));
    case 's':
      for (int64_t i = 0; i < schema->n_children; i++) {
        structFields.emplace_back(
            std::string(schema->children[i]->name),
            LogicalType(fromArrowSchema(schema->children[i])));
      }
      return LogicalType::STRUCT(std::move(structFields));
    case 'm':
      return LogicalType::MAP(
          LogicalType(fromArrowSchema(schema->children[0]->children[0])),
          LogicalType(fromArrowSchema(schema->children[0]->children[1])));
    case 'v':
      switch (arrowType[2]) {
      case 'l':
      case 'L':
        return LogicalType::LIST(
            LogicalType(fromArrowSchema(schema->children[0])));
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
