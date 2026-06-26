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

#include "parquet/arrow_context_column.h"

#include <arrow/array/array_binary.h>
#include <arrow/type.h>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {

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
  ValueColumnBuilder<date_t> builder;
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
  ValueColumnBuilder<date_t> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<arrow::Date64Array>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(Date(typed->Value(j)));
      }
    }
  }
  return builder.finish();
}

/// Convert timestamp arrow arrays to ValueColumn<timestamp_ms_t>.
static std::shared_ptr<IContextColumn> convert_timestamp_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  ValueColumnBuilder<timestamp_ms_t> builder;
  for (const auto& arr : arrays) {
    auto typed = std::static_pointer_cast<arrow::TimestampArray>(arr);
    for (int64_t j = 0; j < typed->length(); ++j) {
      if (typed->IsNull(j)) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(DateTime(typed->Value(j)));
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

}  // namespace execution
}  // namespace neug
