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
#include "neug/utils/property/types.h"

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

/// Converts between common::DataType (protobuf) and neug::DataType without
/// Arrow.
class NeuGTypeConverter {
 public:
  DataType convert(const ::common::DataType& type) const;
  std::shared_ptr<::common::DataType> convert(const DataType& type) const;
  std::shared_ptr<::common::DataType> inferCommonType(
      const DataType& type) const;
};

}  // namespace reader
}  // namespace neug
