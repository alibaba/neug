/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>

#include "neug/generated/proto/plan/basic_type.pb.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace neug {
namespace reader {

/**
 * @brief Template base class for converting types to target format
 *
 * This template class provides a generic interface for converting internal
 * common::DataType (protobuf) to target type systems (e.g., Arrow DataType).
 * Derived classes implement the conversion logic for specific target formats.
 */
template <class TargetType>
class TypeConverter {
 public:
  virtual std::shared_ptr<TargetType> convert(
      const ::common::DataType& type) = 0;

  virtual std::shared_ptr<::common::DataType> convert(
      const TargetType& type) = 0;
};

/**
 * @brief Converts common::DataType (protobuf) to Arrow DataType
 *
 * This class converts protobuf DataType definitions to Arrow DataType
 * instances. Supported types include:
 * - Primitive types: BOOL, INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE
 * - String types: all string variants map to Arrow utf8()
 * - Temporal types: DATE (date64), TIMESTAMP (timestamp with millisecond),
 *   INTERVAL (duration with millisecond)
 * - Complex types: ARRAY (list/fixed_size_list), MAP (map)
 *
 * Unsupported types (e.g., DECIMAL) will throw conversion exceptions.
 */
/**
 * @brief Converts between common::DataType (protobuf) and Arrow DataType
 *
 * This class provides bidirectional conversion between protobuf DataType
 * definitions and Arrow DataType instances. Supported types include:
 * - Primitive types: BOOL, INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE
 * - String types: all string variants map to Arrow utf8()
 * - Temporal types: DATE (date64), TIMESTAMP (timestamp with millisecond),
 *   INTERVAL (duration with millisecond)
 * - Complex types: ARRAY (list/fixed_size_list), MAP (map)
 *
 * Unsupported types (e.g., DECIMAL) will throw conversion exceptions.
 */
class ArrowTypeConverter : public TypeConverter<arrow::DataType> {
 public:
  /**
   * @brief Convert common::DataType to Arrow DataType
   * @param type Protobuf DataType to convert
   * @return Arrow DataType instance
   */
  std::shared_ptr<arrow::DataType> convert(
      const ::common::DataType& type) override;

  /**
   * @brief Convert Arrow DataType to common::DataType
   * @param type Arrow DataType to convert
   * @return Protobuf DataType instance
   */
  std::shared_ptr<::common::DataType> convert(
      const arrow::DataType& type) override;
};

}  // namespace reader
}  // namespace neug
