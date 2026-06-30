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
#include "neug/execution/common/types/value.h"

namespace neug {

execution::Value get_default_value(const DataType& type) {
  switch (type.id()) {
  case DataTypeId::kEmpty:
    return execution::Value(type);
  case DataTypeId::kBoolean:
    return execution::Value::BOOLEAN(false);
  case DataTypeId::kInt32:
    return execution::Value::INT32(0);
  case DataTypeId::kUInt32:
    return execution::Value::UINT32(0);
  case DataTypeId::kInt64:
    return execution::Value::INT64(0);
  case DataTypeId::kUInt64:
    return execution::Value::UINT64(0);
  case DataTypeId::kFloat:
    return execution::Value::FLOAT(0.0);
  case DataTypeId::kDouble:
    return execution::Value::DOUBLE(0.0);
  case DataTypeId::kVarchar: {
    int32_t width =
        type.getExtraTypeInfo()
            ? type.getExtraTypeInfo()->Cast<StringTypeInfo>().max_length
            : STRING_DEFAULT_MAX_LENGTH;
    return execution::Value::VARCHAR("", width);
  }
  case DataTypeId::kDate:
    return execution::Value::DATE(Date(0));
  case DataTypeId::kTimestampMs:
    return execution::Value::TIMESTAMPMS(DateTime(0));
  case DataTypeId::kInterval:
    return execution::Value::INTERVAL(Interval());
  case DataTypeId::kArray: {
    auto child_type = ArrayType::GetChildType(type);
    auto child_default = get_default_value(child_type);
    uint64_t size = ArrayType::GetNumElements(type);
    std::vector<execution::Value> values(size, child_default);
    return execution::Value::ARRAY(type, std::move(values));
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported property type for default value: " + type.ToString());
  }
}

}  // namespace neug
