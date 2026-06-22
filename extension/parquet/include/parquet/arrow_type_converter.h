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
#include "neug/utils/reader/type_converter.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace neug {
namespace reader {

class ArrowTypeConverter : public TypeConverter<arrow::DataType> {
 public:
  std::shared_ptr<arrow::DataType> convert(
      const ::common::DataType& type) override;
  std::shared_ptr<::common::DataType> convert(
      const arrow::DataType& type) override;
};

}  // namespace reader
}  // namespace neug
