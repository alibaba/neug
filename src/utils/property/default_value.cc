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

#include "neug/utils/property/default_value.h"
#include "neug/common/extra_type_info.h"
#include "neug/common/types/value.h"

namespace neug {

Value get_default_value(const DataType& type) {
  switch (type.id()) {
  case DataTypeId::kEmpty:
    return Value(type);
  case DataTypeId::kBoolean:
    return Value::BOOLEAN(false);
  case DataTypeId::kInt32:
    return Value::INT32(0);
  case DataTypeId::kUInt32:
    return Value::UINT32(0);
  case DataTypeId::kInt64:
    return Value::INT64(0);
  case DataTypeId::kUInt64:
    return Value::UINT64(0);
  case DataTypeId::kFloat:
    return Value::FLOAT(0.0);
  case DataTypeId::kDouble:
    return Value::DOUBLE(0.0);
  case DataTypeId::kVarchar: {
    int32_t width =
        type.getExtraTypeInfo()
            ? type.getExtraTypeInfo()->Cast<StringTypeInfo>().max_length
            : STRING_DEFAULT_MAX_LENGTH;
    return Value::VARCHAR("", width);
  }
  case DataTypeId::kDate:
    return Value::DATE(Date(0));
  case DataTypeId::kTimestampMs:
    return Value::TIMESTAMPMS(DateTime(0));
  case DataTypeId::kInterval:
    return Value::INTERVAL(Interval());
  case DataTypeId::kInternalId:
    return Value(type);
  case DataTypeId::kList:
    return Value::LIST(ListType::GetChildType(type), {});
  case DataTypeId::kArray: {
    auto child_type = ArrayType::GetChildType(type);
    auto child_default = get_default_value(child_type);
    uint64_t size = ArrayType::GetNumElements(type);
    std::vector<Value> values(size, child_default);
    return Value::ARRAY(type, std::move(values));
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported property type for default value: " + type.ToString());
  }
}

}  // namespace neug
