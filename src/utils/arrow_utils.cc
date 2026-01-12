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
#include "neug/utils/arrow_utils.h"

#include <vector>
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"

namespace gs {

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(DataTypeId type) {
  if (type == DataTypeId::BOOLEAN) {
    return arrow::boolean();
  } else if (type == DataTypeId::INTEGER) {
    return arrow::int32();
  } else if (type == DataTypeId::BIGINT) {
    return arrow::int64();
  } else if (type == DataTypeId::UINTEGER) {
    return arrow::uint32();
  } else if (type == DataTypeId::UBIGINT) {
    return arrow::uint64();
  } else if (type == DataTypeId::DOUBLE) {
    return arrow::float64();
  } else if (type == DataTypeId::FLOAT) {
    return arrow::float32();
  } else if (type == DataTypeId::DATE) {
    return arrow::date32();
  } else if (type == DataTypeId::VARCHAR) {
    return arrow::large_utf8();
  } else if (type == DataTypeId::EMPTY) {
    return arrow::null();
  } else if (type == DataTypeId::TIMESTAMP_MS) {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  } else if (type == DataTypeId::INTERVAL) {
    return arrow::large_utf8();  // Use large_utf8 for interval, use
                                 // PropUtils to handle it
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected property type: " +
                                  std::to_string(type));
    return nullptr;
  }
}

template <typename T>
void emplace_into_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                         std::vector<Property>& vec) {
  using arrow_array_type = typename gs::TypeConverter<T>::ArrowArrayType;
  for (int32_t i = 0; i < array->num_chunks(); ++i) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(PropUtils<T>::to_prop(casted->Value(k)));
    }
  }
}

}  // namespace gs
