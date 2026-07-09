/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>

#include "neug/common/columns/list_columns.h"
#include "neug/common/columns/struct_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/types.h"
#include "neug/common/types/property_types.h"
#include "neug/common/types/value.h"
#include "neug/generated/proto/response/response.pb.h"

namespace neug {
namespace test {

inline bool protoValueValid(const std::string& validity, int row_idx) {
  return validity.empty() ||
         ((static_cast<uint8_t>(validity[static_cast<size_t>(row_idx) >> 3]) >>
           (row_idx & 7)) &
          1);
}

inline std::shared_ptr<IContextColumn> protoArrayToColumn(const Array& arr) {
  switch (arr.typed_array_case()) {
  case Array::kBoolArray: {
    const auto& typed = arr.bool_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<bool>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kInt32Array: {
    const auto& typed = arr.int32_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<int32_t>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kInt64Array: {
    const auto& typed = arr.int64_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<int64_t>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kFloatArray: {
    const auto& typed = arr.float_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<float>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kDoubleArray: {
    const auto& typed = arr.double_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<double>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kStringArray: {
    const auto& typed = arr.string_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<std::string>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kDateArray: {
    const auto& typed = arr.date_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<Date>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        Date date;
        date.from_timestamp(typed.values(i));
        builder->push_back_opt(date);
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kTimestampArray: {
    const auto& typed = arr.timestamp_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<DateTime>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        int64_t raw = typed.values(i);
        int64_t millis = raw >= 100000000000000LL ? raw / 1000 : raw;
        builder->push_back_opt(DateTime(millis));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kListArray: {
    const auto& typed = arr.list_array();
    auto elements_col = protoArrayToColumn(typed.elements());
    const auto& child_type = elements_col->elem_type();
    ListColumnBuilder builder(child_type);
    for (int row = 0; row + 1 < typed.offsets_size(); ++row) {
      int32_t start = typed.offsets(row);
      int32_t end = typed.offsets(row + 1);
      std::vector<Value> values;
      values.reserve(static_cast<size_t>(end - start));
      for (int i = start; i < end; ++i) {
        values.push_back(elements_col->get_elem(i));
      }
      builder.push_back_elem(Value::LIST(child_type, std::move(values)));
    }
    return builder.finish();
  }
  case Array::kStructArray: {
    const auto& typed = arr.struct_array();
    std::vector<DataType> child_types;
    std::vector<std::shared_ptr<IContextColumn>> field_cols;
    field_cols.reserve(typed.fields_size());
    child_types.reserve(typed.fields_size());
    for (int i = 0; i < typed.fields_size(); ++i) {
      field_cols.push_back(protoArrayToColumn(typed.fields(i)));
      child_types.push_back(field_cols.back()->elem_type());
    }
    auto struct_type = DataType::Struct(child_types);
    StructColumnBuilder builder(struct_type);
    const int num_rows =
        field_cols.empty() ? 0 : static_cast<int>(field_cols[0]->size());
    for (int row = 0; row < num_rows; ++row) {
      if (!typed.validity().empty() &&
          !protoValueValid(typed.validity(), row)) {
        builder.push_back_null();
        continue;
      }
      std::vector<Value> values;
      values.reserve(field_cols.size());
      for (const auto& field_col : field_cols) {
        values.push_back(field_col->get_elem(row));
      }
      builder.push_back_elem(Value::STRUCT(struct_type, std::move(values)));
    }
    return builder.finish();
  }
  case Array::kVertexArray: {
    const auto& typed = arr.vertex_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<std::string>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kEdgeArray: {
    const auto& typed = arr.edge_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<std::string>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  case Array::kPathArray: {
    const auto& typed = arr.path_array();
    bool has_nulls = !typed.validity().empty();
    auto builder = std::make_shared<ValueColumnBuilder<std::string>>(has_nulls);
    for (int i = 0; i < typed.values_size(); ++i) {
      if (protoValueValid(typed.validity(), i)) {
        builder->push_back_opt(typed.values(i));
      } else {
        builder->push_back_null();
      }
    }
    return builder->finish();
  }
  default:
    return nullptr;
  }
}

inline DataChunk queryResponseToDataChunk(const QueryResponse& response) {
  DataChunk chunk;
  for (int i = 0; i < response.arrays_size(); ++i) {
    auto column = protoArrayToColumn(response.arrays(i));
    if (column != nullptr) {
      chunk.set(i, std::move(column));
    }
  }
  return chunk;
}

}  // namespace test
}  // namespace neug
