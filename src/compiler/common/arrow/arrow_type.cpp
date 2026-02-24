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
#include "neug/compiler/common/string_utils.h"
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
  case 'z':
  case 'Z':
    return LogicalType(LogicalTypeID::BLOB);
  case 'u':
  case 'U':
    return LogicalType(LogicalTypeID::STRING);
  case 'v':
    switch (arrowType[1]) {
    case 'z':
      return LogicalType(LogicalTypeID::BLOB);
    case 'u':
      return LogicalType(LogicalTypeID::STRING);
    default:
      NEUG_UNREACHABLE;
    }

  case 'd': {
    auto split = StringUtils::splitComma(std::string(arrowType + 2));
    if (split.size() > 2 && split[2] != "128") {
      THROW_NOT_IMPLEMENTED_EXCEPTION(
          "Decimal bitwidths other than 128 are not implemented");
    }
    return LogicalType::DECIMAL(stoul(split[0]), stoul(split[1]));
  }
  case 'w':
    return LogicalType(LogicalTypeID::BLOB);  // fixed width binary
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
      case 's':
        return LogicalType(LogicalTypeID::TIMESTAMP_SEC);
      case 'm':
        return LogicalType(LogicalTypeID::TIMESTAMP_MS);
      case 'u':
        return LogicalType(LogicalTypeID::TIMESTAMP);
      case 'n':
        return LogicalType(LogicalTypeID::TIMESTAMP_NS);
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
    case 'u': {
      for (int64_t i = 0; i < schema->n_children; i++) {
        structFields.emplace_back(
            std::to_string(i),
            LogicalType(fromArrowSchema(schema->children[i])));
      }
      return LogicalType::UNION(std::move(structFields));
    }
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
