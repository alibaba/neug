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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_time.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {
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

namespace {

// Helper function to create an Arrow builder based on Arrow type
std::unique_ptr<arrow::ArrayBuilder> createArrowBuilder(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  if (arrow_type->Equals(arrow::int64())) {
    return std::make_unique<arrow::Int64Builder>();
  } else if (arrow_type->Equals(arrow::int32())) {
    return std::make_unique<arrow::Int32Builder>();
  } else if (arrow_type->Equals(arrow::uint32())) {
    return std::make_unique<arrow::UInt32Builder>();
  } else if (arrow_type->Equals(arrow::uint64())) {
    return std::make_unique<arrow::UInt64Builder>();
  } else if (arrow_type->Equals(arrow::float32())) {
    return std::make_unique<arrow::FloatBuilder>();
  } else if (arrow_type->Equals(arrow::float64())) {
    return std::make_unique<arrow::DoubleBuilder>();
  } else if (arrow_type->Equals(arrow::boolean())) {
    return std::make_unique<arrow::BooleanBuilder>();
  } else if (arrow_type->Equals(arrow::utf8())) {
    return std::make_unique<arrow::StringBuilder>();
  } else if (arrow_type->Equals(arrow::large_utf8())) {
    return std::make_unique<arrow::LargeStringBuilder>();
  } else if (arrow_type->Equals(arrow::date32())) {
    return std::make_unique<arrow::Date32Builder>();
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    auto timestamp_type =
        std::static_pointer_cast<arrow::TimestampType>(arrow_type);
    return std::make_unique<arrow::TimestampBuilder>(
        timestamp_type, arrow::default_memory_pool());
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type for shuffle: " +
                                  arrow_type->ToString());
  }
}

// Helper function to append value from Arrow array to builder
arrow::Status appendValueFromArray(
    arrow::ArrayBuilder* builder, const std::shared_ptr<arrow::Array>& array,
    size_t offset, const std::shared_ptr<arrow::DataType>& arrow_type) {
  if (array->IsNull(offset)) {
    return builder->AppendNull();
  }

  if (arrow_type->Equals(arrow::int64())) {
    auto* int64_builder = static_cast<arrow::Int64Builder*>(builder);
    auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
    return int64_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::int32())) {
    auto* int32_builder = static_cast<arrow::Int32Builder*>(builder);
    auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
    return int32_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::uint32())) {
    auto* uint32_builder = static_cast<arrow::UInt32Builder*>(builder);
    auto casted = std::static_pointer_cast<arrow::UInt32Array>(array);
    return uint32_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::uint64())) {
    auto* uint64_builder = static_cast<arrow::UInt64Builder*>(builder);
    auto casted = std::static_pointer_cast<arrow::UInt64Array>(array);
    return uint64_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::float32())) {
    auto* float_builder = static_cast<arrow::FloatBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::FloatArray>(array);
    return float_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::float64())) {
    auto* double_builder = static_cast<arrow::DoubleBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::DoubleArray>(array);
    return double_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::boolean())) {
    auto* bool_builder = static_cast<arrow::BooleanBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::BooleanArray>(array);
    return bool_builder->Append(casted->Value(offset));
  } else if (arrow_type->Equals(arrow::utf8())) {
    auto* string_builder = static_cast<arrow::StringBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);
    auto str_view = casted->GetView(offset);
    return string_builder->Append(str_view.data(), str_view.size());
  } else if (arrow_type->Equals(arrow::large_utf8())) {
    auto* large_string_builder =
        static_cast<arrow::LargeStringBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
    auto str_view = casted->GetView(offset);
    return large_string_builder->Append(str_view.data(), str_view.size());
  } else if (arrow_type->Equals(arrow::date32())) {
    auto* date_builder = static_cast<arrow::Date32Builder*>(builder);
    auto casted = std::static_pointer_cast<arrow::Date32Array>(array);
    return date_builder->Append(casted->Value(offset));
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    auto* timestamp_builder = static_cast<arrow::TimestampBuilder*>(builder);
    auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
    return timestamp_builder->Append(casted->Value(offset));
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type for append: " +
                                  arrow_type->ToString());
  }
}
}  // namespace

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

std::pair<size_t, size_t> ArrowArrayContextColumn::locate_array_and_offset(
    size_t idx) const {
  CHECK(idx < size_) << "Index out of range: " << idx << " >= " << size_;

  size_t accumulated_size = 0;
  for (size_t i = 0; i < columns_.size(); ++i) {
    size_t array_length = columns_[i]->length();
    if (idx < accumulated_size + array_length) {
      size_t offset = idx - accumulated_size;
      return {i, offset};
    }
    accumulated_size += array_length;
  }
  LOG(FATAL) << "Should not reach here";
  return {0, 0};
}

std::shared_ptr<IContextColumn> ArrowArrayContextColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  if (columns_.empty()) {
    return std::make_shared<ArrowArrayContextColumn>(
        std::vector<std::shared_ptr<arrow::Array>>());
  }

  auto arrow_type = columns_[0]->type();
  auto builder = createArrowBuilder(arrow_type);

  // Reserve space for better performance
  auto reserve_status = builder->Reserve(offsets.size());
  if (!reserve_status.ok()) {
    THROW_RUNTIME_ERROR("Failed to reserve space in Arrow builder: " +
                        reserve_status.ToString());
  }

  // Append values according to offsets, directly from Arrow arrays
  for (auto offset : offsets) {
    auto [array_idx, local_offset] = locate_array_and_offset(offset);
    const auto& array = columns_[array_idx];
    auto status =
        appendValueFromArray(builder.get(), array, local_offset, arrow_type);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to append value to Arrow builder: " +
                          status.ToString());
    }
  }

  // Finish building the array
  std::shared_ptr<arrow::Array> array;
  auto status = builder->Finish(&array);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish Arrow array: " + status.ToString());
  }

  // Create new ArrowArrayContextColumn with the shuffled array
  ArrowArrayContextColumnBuilder col_builder;
  col_builder.push_back(array);
  return col_builder.finish();
}

Value ArrowArrayContextColumn::get_elem(size_t idx) const {
  CHECK(idx < size_) << "Index out of range: " << idx << " >= " << size_;

  // Locate the array and offset for the given index.
  auto [array_idx, offset] = locate_array_and_offset(idx);
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
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
    return Value::TIMESTAMPMS(DateTime(casted->Value(offset)));
  } else {  // todo: support interval type
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported arrow type: " +
                                  arrow_type->ToString());
  }
}

}  // namespace execution
}  // namespace neug
