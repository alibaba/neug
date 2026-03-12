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
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_time.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <unordered_map>
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
  LOG(FATAL) << "Should not reach here";
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

template <typename ArrowArrayType>
static bool less_than(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                      size_t size, size_t offset_a, size_t offset_b) {
  auto [array_idx_a, local_offset_a] =
      locate_array_and_offset(columns, size, offset_a);
  auto [array_idx_b, local_offset_b] =
      locate_array_and_offset(columns, size, offset_b);
  auto casted_a =
      std::static_pointer_cast<ArrowArrayType>(columns[array_idx_a]);
  auto casted_b =
      std::static_pointer_cast<ArrowArrayType>(columns[array_idx_b]);
  return casted_a->Value(local_offset_a) < casted_b->Value(local_offset_b);
}

template <typename ArrowArrayType>
static bool equal(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                  size_t size, size_t offset_a, size_t offset_b) {
  auto [array_idx_a, local_offset_a] =
      locate_array_and_offset(columns, size, offset_a);
  auto [array_idx_b, local_offset_b] =
      locate_array_and_offset(columns, size, offset_b);
  auto casted_a =
      std::static_pointer_cast<ArrowArrayType>(columns[array_idx_a]);
  auto casted_b =
      std::static_pointer_cast<ArrowArrayType>(columns[array_idx_b]);
  return casted_a->Value(local_offset_a) == casted_b->Value(local_offset_b);
}

// Template function to generate dedup offsets
template <typename ArrowArrayType>
static void generate_dedup_offset(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    std::vector<size_t>& offsets) {
  // Create vector of all offsets
  std::vector<size_t> row_indices(size);
  std::iota(row_indices.begin(), row_indices.end(), 0);

  // Create comparator that directly uses Arrow arrays
  auto compare = [&](size_t a, size_t b) -> bool {
    if (equal<ArrowArrayType>(columns, size, a, b)) {
      return a < b;
    }
    return less_than<ArrowArrayType>(columns, size, a, b);
  };

  // Sort indices using stable_sort to maintain order for equal values
  std::stable_sort(row_indices.begin(), row_indices.end(), compare);

  // Extract deduplicated offsets
  offsets.clear();
  if (row_indices.empty()) {
    return;
  }

  offsets.push_back(row_indices[0]);

  for (size_t i = 1; i < row_indices.size(); ++i) {
    if (!equal<ArrowArrayType>(columns, size, row_indices[i],
                               row_indices[i - 1])) {
      offsets.push_back(row_indices[i]);
    }
  }
}

// Define DISPATCH macro to generate dedup offsets based on Arrow type
void dispatch_generate_dedup_offset(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    const std::shared_ptr<arrow::DataType>& arrow_type,
    std::vector<size_t>& offsets) {
  // Use Arrow type ID for dispatch
  switch (arrow_type->id()) {
#define ARROW_TYPE_DISPATCHER_DEDUP(arrow_type_id, arrow_array_type) \
  case arrow::Type::arrow_type_id: {                                 \
    generate_dedup_offset<arrow_array_type>(columns, size, offsets); \
    break;                                                           \
  }

    ARROW_TYPE_DISPATCHER_DEDUP(BOOL, arrow::BooleanArray)
    ARROW_TYPE_DISPATCHER_DEDUP(INT64, arrow::Int64Array)
    ARROW_TYPE_DISPATCHER_DEDUP(INT32, arrow::Int32Array)
    ARROW_TYPE_DISPATCHER_DEDUP(UINT32, arrow::UInt32Array)
    ARROW_TYPE_DISPATCHER_DEDUP(UINT64, arrow::UInt64Array)
    ARROW_TYPE_DISPATCHER_DEDUP(FLOAT, arrow::FloatArray)
    ARROW_TYPE_DISPATCHER_DEDUP(DOUBLE, arrow::DoubleArray)
    ARROW_TYPE_DISPATCHER_DEDUP(STRING, arrow::StringArray)
    ARROW_TYPE_DISPATCHER_DEDUP(LARGE_STRING, arrow::LargeStringArray)
    ARROW_TYPE_DISPATCHER_DEDUP(DATE32, arrow::Date32Array)
    ARROW_TYPE_DISPATCHER_DEDUP(DATE64, arrow::Date64Array)
    ARROW_TYPE_DISPATCHER_DEDUP(TIMESTAMP, arrow::TimestampArray)
    // Interval type has been converted to arrow string type

#undef ARROW_TYPE_DISPATCHER_DEDUP

  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type for dedup: " +
                                  arrow_type->ToString());
  }
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
      LOG(FATAL) << "Expect the same type of columns, but got "
                 << columns_[0]->type()->ToString() << " and "
                 << column->type()->ToString();
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

Value ArrowArrayContextColumn::get_elem(size_t idx) const {
  CHECK(idx < size_) << "Index out of range: " << idx << " >= " << size_;

  // Locate the array and offset for the given index.
  auto [array_idx, offset] = locate_array_and_offset(columns_, size_, idx);
  const auto& array = columns_[array_idx];

  // Get value according to arrow type and convert to RTAny.
  auto arrow_type = array->type();

  if (arrow_type->Equals(arrow::int64())) {
    auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
    return Value::INT64(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::int32())) {
    auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
    return Value::INT32(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::uint32())) {
    auto casted = std::static_pointer_cast<arrow::UInt32Array>(array);
    return Value::UINT32(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::uint64())) {
    auto casted = std::static_pointer_cast<arrow::UInt64Array>(array);
    return Value::UINT64(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::float32())) {
    auto casted = std::static_pointer_cast<arrow::FloatArray>(array);
    return Value::FLOAT(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::float64())) {
    auto casted = std::static_pointer_cast<arrow::DoubleArray>(array);
    return Value::DOUBLE(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::boolean())) {
    auto casted = std::static_pointer_cast<arrow::BooleanArray>(array);
    return Value::BOOLEAN(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::utf8())) {
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);
    auto str_view = casted->GetView(offset);
    return Value::STRING(std::string(str_view));
  } else if (arrow_type->Equals(arrow::large_utf8())) {
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
    auto str_view = casted->GetView(offset);
    return Value::STRING(std::string(str_view));
  } else if (arrow_type->Equals(arrow::date32())) {
    auto casted = std::static_pointer_cast<arrow::Date32Array>(array);
    Date d;
    d.from_num_days(casted->Value(offset));
    return Value::DATE(d);
  } else if (arrow_type->Equals(arrow::date64())) {
    auto casted = std::static_pointer_cast<arrow::Date64Array>(array);
    return Value::DATE(Date(casted->Value(offset)));
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
    return Value::TIMESTAMPMS(DateTime(casted->Value(offset)));
  } else {  // todo: support interval type
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type: " +
                                  arrow_type->ToString());
  }
}

void ArrowArrayContextColumn::generate_dedup_offset(
    std::vector<size_t>& offsets) const {
  if (columns_.empty()) {
    offsets.clear();
    return;
  }

  auto arrow_type = columns_[0]->type();
  dispatch_generate_dedup_offset(columns_, size_, arrow_type, offsets);
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
