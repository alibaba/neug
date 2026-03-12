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

namespace neug {

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(DataTypeId type) {
  switch (type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return TypeConverter<type>::ArrowTypeValue();
    FOR_EACH_DATA_TYPE_PRIMITIVE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    return TypeConverter<std::string_view>::ArrowTypeValue();
  case DataTypeId::kDate:
    return arrow::date64();
  case DataTypeId::kTimestampMs:
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  case DataTypeId::kInterval:
    return arrow::large_utf8();
  case DataTypeId::kEmpty:
    return arrow::null();
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected property type: " +
                                  std::to_string(type));
  }
}

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(DataType type) {
  auto id = type.id();
  switch (id) {
  case DataTypeId::kStruct: {
    std::vector<std::shared_ptr<arrow::Field>> field_types;
    auto casted = dynamic_cast<const StructTypeInfo*>(type.RawExtraTypeInfo());
    assert(casted != nullptr);
    for (int i = 0; i < casted->child_types.size(); ++i) {
      field_types.push_back(
          arrow::field("f" + std::to_string(i),
                       PropertyTypeToArrowType(casted->child_types[i])));
    }
    return arrow::struct_(field_types);
  }
  case DataTypeId::kList: {
    auto casted = dynamic_cast<const ListTypeInfo*>(type.RawExtraTypeInfo());
    assert(casted != nullptr);
    auto value_type = PropertyTypeToArrowType(casted->child_type);
    return arrow::list(value_type);
  }
  default:
    return PropertyTypeToArrowType(id);
  }
  return nullptr;
}

template <typename T>
void emplace_into_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                         std::vector<Property>& vec) {
  using arrow_array_type = typename neug::TypeConverter<T>::ArrowArrayType;
  for (int32_t i = 0; i < array->num_chunks(); ++i) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(PropUtils<T>::to_prop(casted->Value(k)));
    }
  }
}

}  // namespace neug
