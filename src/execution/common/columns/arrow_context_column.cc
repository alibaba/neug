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

#include "neug/execution/common/columns/arrow_context_column.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_time.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <unordered_map>
#include "neug/common/extra_type_info.h"
#include "neug/execution/common/columns/columns_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {

std::pair<size_t, size_t> locate_array_and_offset(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    size_t idx) {
  CHECK(idx < size) << "Index out of range: " << idx << " >= " << size;

  size_t accumulated_size = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    size_t array_length = columns[i]->length();
    if (idx < accumulated_size + array_length) {
      size_t offset = idx - accumulated_size;
      return {i, offset};
    }
    accumulated_size += array_length;
  }
  THROW_INTERNAL_EXCEPTION("Should not reach here");
  return {0, 0};
}

DataType arrow_type_to_rt_type(const std::shared_ptr<arrow::DataType>& type) {
  if (type->Equals(arrow::int64())) {
    return DataType(DataTypeId::kInt64);
  } else if (type->Equals(arrow::int32())) {
    return DataType(DataTypeId::kInt32);
  } else if (type->Equals(arrow::uint32())) {
    return DataType(DataTypeId::kUInt32);
  } else if (type->Equals(arrow::uint64())) {
    return DataType(DataTypeId::kUInt64);
  } else if (type->Equals(arrow::float32())) {
    return DataType(DataTypeId::kFloat);
  } else if (type->Equals(arrow::float64())) {
    return DataType(DataTypeId::kDouble);
  } else if (type->Equals(arrow::boolean())) {
    return DataType(DataTypeId::kBoolean);
  } else if (type->Equals(arrow::utf8()) || type->Equals(arrow::large_utf8())) {
    return DataType(DataTypeId::kVarchar);
  } else if (type->Equals(arrow::date32())) {
    return DataType(DataTypeId::kDate);
  } else if (type->Equals(arrow::date64())) {
    return DataType(DataTypeId::kDate);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    return DataType(DataTypeId::kTimestampMs);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    return DataType(DataTypeId::kTimestampMs);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
    return DataType(DataTypeId::kTimestampMs);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::NANO))) {
    return DataType(DataTypeId::kTimestampMs);
  } else if (type->id() == arrow::Type::LIST ||
             type->id() == arrow::Type::LARGE_LIST ||
             type->id() == arrow::Type::FIXED_SIZE_LIST) {
    auto valueType = type->field(0)->type();
    auto childType = arrow_type_to_rt_type(valueType);
    auto typeInfo = std::make_shared<ListTypeInfo>(childType);
    return DataType(DataTypeId::kList, typeInfo);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected arrow type: " + type->ToString());
  }
}

// Template function to shuffle Arrow arrays based on type
template <typename ArrowArrayType, typename ArrowBuilderType>
static std::shared_ptr<arrow::Array> shuffle_impl(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    const std::vector<size_t>& offsets,
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  // Create builder
  auto builder_result = arrow::MakeBuilder(arrow_type);
  if (!builder_result.ok()) {
    THROW_RUNTIME_ERROR("Failed to create Arrow builder: " +
                        builder_result.status().ToString());
  }
  auto builder = std::move(builder_result.ValueOrDie());
  auto* typed_builder = static_cast<ArrowBuilderType*>(builder.get());

  // Reserve space for better performance
  auto reserve_status = builder->Reserve(offsets.size());
  if (!reserve_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to reserve space in Arrow builder: " +
                        reserve_status.ToString());
  }

  // Append values according to offsets, directly from Arrow arrays
  for (auto offset : offsets) {
    auto [array_idx, local_offset] =
        locate_array_and_offset(columns, size, offset);
    const auto& array = columns[array_idx];
    auto casted = std::static_pointer_cast<ArrowArrayType>(array);

    if (casted->IsNull(local_offset)) {
      auto status = builder->AppendNull();
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append null to Arrow builder: " +
                            status.ToString());
      }
    } else {
      auto status = typed_builder->Append(casted->Value(local_offset));
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append value to Arrow builder: " +
                            status.ToString());
      }
    }
  }

  // Finish building the array
  std::shared_ptr<arrow::Array> result_array;
  auto finish_status = builder->Finish(&result_array);
  if (!finish_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish Arrow array: " +
                        finish_status.ToString());
  }
  return result_array;
}

// Specialization for string types (StringArray)
template <>
std::shared_ptr<arrow::Array>
shuffle_impl<arrow::StringArray, arrow::StringBuilder>(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    const std::vector<size_t>& offsets,
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  auto builder_result = arrow::MakeBuilder(arrow_type);
  if (!builder_result.ok()) {
    THROW_RUNTIME_ERROR("Failed to create Arrow builder: " +
                        builder_result.status().ToString());
  }
  auto builder = std::move(builder_result.ValueOrDie());
  auto* typed_builder = static_cast<arrow::StringBuilder*>(builder.get());

  auto reserve_status = builder->Reserve(offsets.size());
  if (!reserve_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to reserve space in Arrow builder: " +
                        reserve_status.ToString());
  }

  for (auto offset : offsets) {
    auto [array_idx, local_offset] =
        locate_array_and_offset(columns, size, offset);
    const auto& array = columns[array_idx];
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);

    if (casted->IsNull(local_offset)) {
      auto status = builder->AppendNull();
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append null to Arrow builder: " +
                            status.ToString());
      }
    } else {
      auto str_view = casted->GetView(local_offset);
      auto status = typed_builder->Append(str_view.data(), str_view.size());
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append value to Arrow builder: " +
                            status.ToString());
      }
    }
  }

  std::shared_ptr<arrow::Array> result_array;
  auto finish_status = builder->Finish(&result_array);
  if (!finish_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish Arrow array: " +
                        finish_status.ToString());
  }
  return result_array;
}

// Specialization for large string types (LargeStringArray)
template <>
std::shared_ptr<arrow::Array>
shuffle_impl<arrow::LargeStringArray, arrow::LargeStringBuilder>(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    const std::vector<size_t>& offsets,
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  auto builder_result = arrow::MakeBuilder(arrow_type);
  if (!builder_result.ok()) {
    THROW_RUNTIME_ERROR("Failed to create Arrow builder: " +
                        builder_result.status().ToString());
  }
  auto builder = std::move(builder_result.ValueOrDie());
  auto* typed_builder = static_cast<arrow::LargeStringBuilder*>(builder.get());

  auto reserve_status = builder->Reserve(offsets.size());
  if (!reserve_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to reserve space in Arrow builder: " +
                        reserve_status.ToString());
  }

  for (auto offset : offsets) {
    auto [array_idx, local_offset] =
        locate_array_and_offset(columns, size, offset);
    const auto& array = columns[array_idx];
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);

    if (casted->IsNull(local_offset)) {
      auto status = builder->AppendNull();
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append null to Arrow builder: " +
                            status.ToString());
      }
    } else {
      auto str_view = casted->GetView(local_offset);
      auto status = typed_builder->Append(str_view.data(), str_view.size());
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append value to Arrow builder: " +
                            status.ToString());
      }
    }
  }

  std::shared_ptr<arrow::Array> result_array;
  auto finish_status = builder->Finish(&result_array);
  if (!finish_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish Arrow array: " +
                        finish_status.ToString());
  }
  return result_array;
}

std::shared_ptr<IContextColumn> ArrowArrayContextColumnBuilder::finish() {
  return std::make_shared<ArrowArrayContextColumn>(columns_);
}

void ArrowArrayContextColumnBuilder::push_back(
    const std::shared_ptr<arrow::Array>& column) {
  if (columns_.size() > 0) {
    if (columns_[0]->type()->Equals(column->type())) {
      columns_.push_back(column);
      return;
    } else {
      THROW_INTERNAL_EXCEPTION("Expect the same type of columns, but got " +
                               columns_[0]->type()->ToString() + " and " +
                               column->type()->ToString());
    }
  }
  columns_.push_back(column);
}

std::shared_ptr<IContextColumn> ArrowArrayContextColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  if (columns_.empty()) {
    return std::make_shared<ArrowArrayContextColumn>(
        std::vector<std::shared_ptr<arrow::Array>>());
  }

  auto arrow_type = columns_[0]->type();
  std::shared_ptr<arrow::Array> array;

  // Dispatch to template function based on Arrow type
  switch (arrow_type->id()) {
#define ARROW_TYPE_DISPATCHER_SHUFFLE(arrow_type_id, arrow_array_type, \
                                      arrow_builder_type)              \
  case arrow::Type::arrow_type_id: {                                   \
    array = shuffle_impl<arrow_array_type, arrow_builder_type>(        \
        columns_, size_, offsets, arrow_type);                         \
    break;                                                             \
  }

    ARROW_TYPE_DISPATCHER_SHUFFLE(BOOL, arrow::BooleanArray,
                                  arrow::BooleanBuilder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(INT64, arrow::Int64Array, arrow::Int64Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(INT32, arrow::Int32Array, arrow::Int32Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(UINT32, arrow::UInt32Array,
                                  arrow::UInt32Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(UINT64, arrow::UInt64Array,
                                  arrow::UInt64Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(FLOAT, arrow::FloatArray, arrow::FloatBuilder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(DOUBLE, arrow::DoubleArray,
                                  arrow::DoubleBuilder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(STRING, arrow::StringArray,
                                  arrow::StringBuilder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(LARGE_STRING, arrow::LargeStringArray,
                                  arrow::LargeStringBuilder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(DATE32, arrow::Date32Array,
                                  arrow::Date32Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(DATE64, arrow::Date64Array,
                                  arrow::Date64Builder)
    ARROW_TYPE_DISPATCHER_SHUFFLE(TIMESTAMP, arrow::TimestampArray,
                                  arrow::TimestampBuilder)

#undef ARROW_TYPE_DISPATCHER_SHUFFLE

  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type for shuffle: " +
                                  arrow_type->ToString());
  }

  // Create new ArrowArrayContextColumn with the shuffled array
  ArrowArrayContextColumnBuilder col_builder;
  col_builder.push_back(array);
  return col_builder.finish();
}

static Value get_arrow_scalar_value(const std::shared_ptr<arrow::Array>& array,
                                    int64_t idx) {
  auto arrow_type = array->type();
  if (arrow_type->Equals(arrow::int64())) {
    return Value::INT64(
        std::static_pointer_cast<arrow::Int64Array>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::int32())) {
    return Value::INT32(
        std::static_pointer_cast<arrow::Int32Array>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::uint32())) {
    return Value::UINT32(
        std::static_pointer_cast<arrow::UInt32Array>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::uint64())) {
    return Value::UINT64(
        std::static_pointer_cast<arrow::UInt64Array>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::float32())) {
    return Value::FLOAT(
        std::static_pointer_cast<arrow::FloatArray>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::float64())) {
    return Value::DOUBLE(
        std::static_pointer_cast<arrow::DoubleArray>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::boolean())) {
    return Value::BOOLEAN(
        std::static_pointer_cast<arrow::BooleanArray>(array)->Value(idx));
  } else if (arrow_type->Equals(arrow::utf8())) {
    auto str_view =
        std::static_pointer_cast<arrow::StringArray>(array)->GetView(idx);
    return Value::STRING(std::string(str_view));
  } else if (arrow_type->Equals(arrow::large_utf8())) {
    auto str_view =
        std::static_pointer_cast<arrow::LargeStringArray>(array)->GetView(idx);
    return Value::STRING(std::string(str_view));
  } else if (arrow_type->Equals(arrow::date32())) {
    Date d;
    d.from_num_days(
        std::static_pointer_cast<arrow::Date32Array>(array)->Value(idx));
    return Value::DATE(d);
  } else if (arrow_type->Equals(arrow::date64())) {
    return Value::DATE(
        Date(std::static_pointer_cast<arrow::Date64Array>(array)->Value(idx)));
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    return Value::TIMESTAMPMS(DateTime(
        std::static_pointer_cast<arrow::TimestampArray>(array)->Value(idx)));
  } else if (arrow_type->id() == arrow::Type::LIST) {
    auto list_arr = std::static_pointer_cast<arrow::ListArray>(array);
    auto child_type = arrow_type_to_rt_type(list_arr->value_type());
    auto values_arr = list_arr->values();
    int32_t start = list_arr->value_offset(idx);
    int32_t length = list_arr->value_length(idx);
    std::vector<Value> list_values;
    list_values.reserve(length);
    for (int32_t j = start; j < start + length; ++j) {
      list_values.push_back(get_arrow_scalar_value(values_arr, j));
    }
    return Value::LIST(child_type, std::move(list_values));
  } else if (arrow_type->id() == arrow::Type::FIXED_SIZE_LIST) {
    auto list_arr = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    auto child_type = arrow_type_to_rt_type(list_arr->value_type());
    auto values_arr = list_arr->values();
    int32_t start = list_arr->value_offset(idx);
    int32_t length = list_arr->value_length(idx);
    std::vector<Value> list_values;
    list_values.reserve(length);
    for (int32_t j = start; j < start + length; ++j) {
      list_values.push_back(get_arrow_scalar_value(values_arr, j));
    }
    return Value::LIST(child_type, std::move(list_values));
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type in list element: " +
                                  arrow_type->ToString());
  }
}

Value ArrowArrayContextColumn::get_elem(size_t idx) const {
  CHECK(idx < size_) << "Index out of range: " << idx << " >= " << size_;

  // Locate the array and offset for the given index.
  auto [array_idx, offset] = locate_array_and_offset(columns_, size_, idx);
  const auto& array = columns_[array_idx];

  // Get value according to arrow type and convert to RTAny.
  auto arrow_type = array->type();

  if (arrow_type->id() == arrow::Type::LIST) {
    auto list_arr = std::static_pointer_cast<arrow::ListArray>(array);
    auto child_type = arrow_type_to_rt_type(list_arr->value_type());
    auto values_arr = list_arr->values();
    int32_t start = list_arr->value_offset(offset);
    int32_t length = list_arr->value_length(offset);
    std::vector<Value> list_values;
    list_values.reserve(length);
    for (int32_t j = start; j < start + length; ++j) {
      list_values.push_back(get_arrow_scalar_value(values_arr, j));
    }
    return Value::LIST(child_type, std::move(list_values));
  } else if (arrow_type->id() == arrow::Type::FIXED_SIZE_LIST) {
    auto list_arr = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    auto child_type = arrow_type_to_rt_type(list_arr->value_type());
    auto values_arr = list_arr->values();
    int32_t start = list_arr->value_offset(offset);
    int32_t length = list_arr->value_length(offset);
    std::vector<Value> list_values;
    list_values.reserve(length);
    for (int32_t j = start; j < start + length; ++j) {
      list_values.push_back(get_arrow_scalar_value(values_arr, j));
    }
    return Value::LIST(child_type, std::move(list_values));
  } else {
    return get_arrow_scalar_value(array, offset);
  }
}

std::shared_ptr<IContextColumn> ArrowArrayContextColumn::cast_to_value_column()
    const {
  auto builder = ColumnsUtils::create_builder(elem_type());
  for (size_t i = 0; i < size_; ++i) {
    builder->push_back_elem(get_elem(i));
  }
  return builder->finish();
}

}  // namespace execution
}  // namespace neug
