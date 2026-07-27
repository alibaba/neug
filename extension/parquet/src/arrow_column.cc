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

#include "parquet/arrow_column.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/type.h>

#include "neug/common/columns/columns_utils.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/types/value.h"
#include "neug/utils/exception/exception.h"

namespace neug {

static int64_t arrow_time_to_milliseconds(int64_t value,
                                          arrow::TimeUnit::type unit) {
  switch (unit) {
  case arrow::TimeUnit::SECOND:
    return value * Interval::MSECS_PER_SEC;
  case arrow::TimeUnit::MILLI:
    return value;
  case arrow::TimeUnit::MICRO:
    return value / Interval::MICROS_PER_MSEC;
  case arrow::TimeUnit::NANO:
    return value / (Interval::MICROS_PER_MSEC * 1000);
  }
  THROW_NOT_SUPPORTED_EXCEPTION("Unsupported Arrow time unit");
}

// Keep this mapping in sync with arrow_value_at when adding Arrow types.
static DataType arrow_type_to_neug_type(const arrow::DataType& type) {
  switch (type.id()) {
  case arrow::Type::BOOL:
    return DataType::BOOLEAN;
  case arrow::Type::INT32:
    return DataType::INT32;
  case arrow::Type::INT64:
    return DataType::INT64;
  case arrow::Type::UINT32:
    return DataType::UINT32;
  case arrow::Type::UINT64:
    return DataType::UINT64;
  case arrow::Type::FLOAT:
    return DataType::FLOAT;
  case arrow::Type::DOUBLE:
    return DataType::DOUBLE;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return DataType::VARCHAR;
  case arrow::Type::DATE32:
  case arrow::Type::DATE64:
    return DataType::DATE;
  case arrow::Type::TIMESTAMP:
    return DataType::TIMESTAMP_MS;
  case arrow::Type::DURATION:
    return DataType::INTERVAL;
  case arrow::Type::FIXED_SIZE_LIST: {
    const auto& array_type = static_cast<const arrow::FixedSizeListType&>(type);
    return DataType::Array(arrow_type_to_neug_type(*array_type.value_type()),
                           array_type.list_size());
  }
  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Parquet LIST is not supported as ARRAY. Specify a fixed-size Arrow "
        "list / NeuG ARRAY type instead.");
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type: " + type.ToString());
  }
}

static Value arrow_value_at(const arrow::Array& array, int64_t index,
                            const DataType& type) {
  if (array.IsNull(index)) {
    return Value(type);
  }
  switch (array.type_id()) {
  case arrow::Type::BOOL:
    return Value::BOOLEAN(
        static_cast<const arrow::BooleanArray&>(array).Value(index));
  case arrow::Type::INT32:
    return Value::INT32(
        static_cast<const arrow::Int32Array&>(array).Value(index));
  case arrow::Type::INT64:
    return Value::INT64(
        static_cast<const arrow::Int64Array&>(array).Value(index));
  case arrow::Type::UINT32:
    return Value::UINT32(
        static_cast<const arrow::UInt32Array&>(array).Value(index));
  case arrow::Type::UINT64:
    return Value::UINT64(
        static_cast<const arrow::UInt64Array&>(array).Value(index));
  case arrow::Type::FLOAT:
    return Value::FLOAT(
        static_cast<const arrow::FloatArray&>(array).Value(index));
  case arrow::Type::DOUBLE:
    return Value::DOUBLE(
        static_cast<const arrow::DoubleArray&>(array).Value(index));
  case arrow::Type::STRING:
    return Value::STRING(std::string(
        static_cast<const arrow::StringArray&>(array).GetView(index)));
  case arrow::Type::LARGE_STRING:
    return Value::STRING(std::string(
        static_cast<const arrow::LargeStringArray&>(array).GetView(index)));
  case arrow::Type::DATE32: {
    Date date;
    date.from_num_days(
        static_cast<const arrow::Date32Array&>(array).Value(index));
    return Value::DATE(date);
  }
  case arrow::Type::DATE64: {
    constexpr int64_t MILLIS_PER_DAY = 24LL * 60 * 60 * 1000;
    Date date;
    date.from_num_days(static_cast<int32_t>(
        static_cast<const arrow::Date64Array&>(array).Value(index) /
        MILLIS_PER_DAY));
    return Value::DATE(date);
  }
  case arrow::Type::TIMESTAMP: {
    const auto value =
        static_cast<const arrow::TimestampArray&>(array).Value(index);
    const auto& timestamp_type =
        static_cast<const arrow::TimestampType&>(*array.type());
    return Value::TIMESTAMPMS(
        DateTime(arrow_time_to_milliseconds(value, timestamp_type.unit())));
  }
  case arrow::Type::DURATION: {
    const auto value =
        static_cast<const arrow::DurationArray&>(array).Value(index);
    const auto& duration_type =
        static_cast<const arrow::DurationType&>(*array.type());
    Interval interval;
    interval.from_mill_seconds(
        arrow_time_to_milliseconds(value, duration_type.unit()));
    return Value::INTERVAL(interval);
  }
  case arrow::Type::FIXED_SIZE_LIST: {
    const auto& list = static_cast<const arrow::FixedSizeListArray&>(array);
    const auto& array_type =
        static_cast<const arrow::FixedSizeListType&>(*array.type());
    const auto child_type = ArrayType::GetChildType(type);
    std::vector<Value> values;
    values.reserve(array_type.list_size());
    const auto offset = list.value_offset(index);
    for (int32_t i = 0; i < array_type.list_size(); ++i) {
      values.push_back(arrow_value_at(*list.values(), offset + i, child_type));
    }
    return Value::ARRAY(type, std::move(values));
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type: " +
                                  array.type()->ToString());
  }
}

static std::shared_ptr<IContextColumn> convert_fixed_size_list_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  const auto type = arrow_type_to_neug_type(*arrays.front()->type());
  auto builder = ColumnsUtils::create_builder(type);
  size_t size = 0;
  for (const auto& array : arrays) {
    size += array->length();
  }
  builder->reserve(size);
  for (const auto& array : arrays) {
    if (!array->type()->Equals(*arrays.front()->type())) {
      THROW_SCHEMA_MISMATCH("Parquet ARRAY chunks have different types");
    }
    for (int64_t i = 0; i < array->length(); ++i) {
      // Arrow's Parquet writer cannot currently consume null fixed-size lists.
      // This is an upstream Arrow limitation.
      if (array->IsNull(i)) {
        builder->push_back_null();
        continue;
      }
      builder->push_back_elem(arrow_value_at(*array, i, type));
    }
  }
  return builder->finish();
}

/// Convert numeric arrow arrays directly to ValueColumn<CppT>.
template <typename ArrowArrayT, typename CppT>
static std::shared_ptr<IContextColumn> convert_numeric_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<CppT> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<ArrowArrayT>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(static_cast<CppT>(typed->Value(j)));
      }
    }
  }
  return builder.finish();
}

/// Convert string-typed arrow arrays to ValueColumn<std::string>.
template <typename ArrowStringArrayT>
static std::shared_ptr<IContextColumn> convert_string_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<std::string> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<ArrowStringArrayT>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        auto sv = typed->GetView(j);
        builder.push_back_opt(std::string(sv));
      }
    }
  }
  return builder.finish();
}

/// Convert date32 arrow arrays (days since epoch) to ValueColumn<date_t>.
static std::shared_ptr<IContextColumn> convert_date32_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<Date> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<arrow::Date32Array>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        Date d;
        d.from_num_days(typed->Value(j));
        builder.push_back_opt(d);
      }
    }
  }
  return builder.finish();
}

/// Convert date64 arrow arrays (ms since epoch) to ValueColumn<date_t>.
static std::shared_ptr<IContextColumn> convert_date64_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<Date> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<arrow::Date64Array>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        constexpr int64_t MILLIS_PER_DAY = 24LL * 60 * 60 * 1000;
        Date date;
        date.from_num_days(
            static_cast<int32_t>(typed->Value(j) / MILLIS_PER_DAY));
        builder.push_back_opt(date);
      }
    }
  }
  return builder.finish();
}

/// Convert timestamp arrow arrays to ValueColumn<timestamp_ms_t>.
static std::shared_ptr<IContextColumn> convert_timestamp_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<DateTime> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<arrow::TimestampArray>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        const auto& timestamp_type =
            static_cast<const arrow::TimestampType&>(*typed->type());
        builder.push_back_opt(DateTime(arrow_time_to_milliseconds(
            typed->Value(j), timestamp_type.unit())));
      }
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> arrow_arrays_to_value_column(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  if (arrays.empty()) {
    return ValueColumnBuilder<int64_t>().finish();
  }
  auto arrow_type = arrays[0]->type();
  switch (arrow_type->id()) {
  case arrow::Type::BOOL:
    return convert_numeric_arrays<arrow::BooleanArray, bool>(arrays);
  case arrow::Type::INT32:
    return convert_numeric_arrays<arrow::Int32Array, int32_t>(arrays);
  case arrow::Type::INT64:
    return convert_numeric_arrays<arrow::Int64Array, int64_t>(arrays);
  case arrow::Type::UINT32:
    return convert_numeric_arrays<arrow::UInt32Array, uint32_t>(arrays);
  case arrow::Type::UINT64:
    return convert_numeric_arrays<arrow::UInt64Array, uint64_t>(arrays);
  case arrow::Type::FLOAT:
    return convert_numeric_arrays<arrow::FloatArray, float>(arrays);
  case arrow::Type::DOUBLE:
    return convert_numeric_arrays<arrow::DoubleArray, double>(arrays);
  case arrow::Type::STRING:
    return convert_string_arrays<arrow::StringArray>(arrays);
  case arrow::Type::LARGE_STRING:
    return convert_string_arrays<arrow::LargeStringArray>(arrays);
  case arrow::Type::DATE32:
    return convert_date32_arrays(arrays);
  case arrow::Type::DATE64:
    return convert_date64_arrays(arrays);
  case arrow::Type::TIMESTAMP:
    return convert_timestamp_arrays(arrays);
  case arrow::Type::FIXED_SIZE_LIST:
    return convert_fixed_size_list_arrays(arrays);
  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Parquet LIST columns are not supported; use a fixed-size ARRAY "
        "type such as FLOAT[3]");
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type: " +
                                  arrow_type->ToString());
  }
}

std::shared_ptr<IContextColumn> arrow_array_to_value_column(
    const std::shared_ptr<arrow::Array>& array) {
  return arrow_arrays_to_value_column({array});
}

std::shared_ptr<DataChunk> recordbatch_to_value_datachunk(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!batch) {
    return nullptr;
  }
  auto chunk = std::make_shared<DataChunk>();
  for (int i = 0; i < batch->num_columns(); ++i) {
    chunk->set(i, arrow_array_to_value_column(batch->column(i)));
  }
  return chunk;
}

}  // namespace neug
