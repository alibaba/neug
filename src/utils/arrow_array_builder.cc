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

#include "neug/utils/arrow_array_builder.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/arrow_utils.h"

#include <cstring>

namespace neug {

static std::vector<std::shared_ptr<arrow::DataType>>
    LEGAL_GRAPH_PROPERTY_ARROW_TYPES = {
        arrow::boolean(),    arrow::int8(),
        arrow::uint8(),      arrow::int16(),
        arrow::uint16(),     arrow::int32(),
        arrow::uint32(),     arrow::int64(),
        arrow::uint64(),     arrow::float32(),
        arrow::float64(),    arrow::utf8(),
        arrow::large_utf8(), arrow::date32(),
        arrow::date64(),     arrow::timestamp(arrow::TimeUnit::MILLI)};

std::shared_ptr<IArrowArrayBuilder> create_wrap_array_builder(DataType type) {
  if (type.id() == DataTypeId::kInt64) {
    return std::make_shared<ArrowArrayWrapBuilder<int64_t>>();
  } else if (type.id() == DataTypeId::kUInt64) {
    return std::make_shared<ArrowArrayWrapBuilder<uint64_t>>();
  } else if (type.id() == DataTypeId::kInt32) {
    return std::make_shared<ArrowArrayWrapBuilder<int32_t>>();
  } else if (type.id() == DataTypeId::kUInt32) {
    return std::make_shared<ArrowArrayWrapBuilder<uint32_t>>();
  } else if (type.id() == DataTypeId::kFloat) {
    return std::make_shared<ArrowArrayWrapBuilder<float>>();
  } else if (type.id() == DataTypeId::kDouble) {
    return std::make_shared<ArrowArrayWrapBuilder<double>>();
  } else if (type.id() == DataTypeId::kEmpty) {
    return std::make_shared<ArrowArrayWrapBuilder<EmptyType>>();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "create_wrap_array_builder not support for type " +
        std::to_string(static_cast<int>(type.id())));
  }
}

std::unique_ptr<arrow::ArrayBuilder> create_path_array_builder(
    const DataType& type) {
  std::vector<std::shared_ptr<arrow::Field>> path_fields;
  // Vertex list
  auto vertex_struct_type =
      arrow::struct_({arrow::field("label", arrow::utf8()),
                      arrow::field("id", arrow::int64())});
  auto vertex_list_type = arrow::list(vertex_struct_type);
  path_fields.push_back(arrow::field("vertices", vertex_list_type));
  // Edge list
  auto edge_struct_type =
      arrow::struct_({arrow::field("label", arrow::utf8()),
                      arrow::field("direction", arrow::utf8())});
  auto edge_list_type = arrow::list(edge_struct_type);
  path_fields.push_back(arrow::field("edges", edge_list_type));
  // weight and length
  path_fields.push_back(arrow::field("weight", arrow::float64()));
  path_fields.push_back(arrow::field("length", arrow::int32()));
  auto path_struct_type = arrow::struct_(path_fields);
  return arrow::MakeBuilder(path_struct_type, arrow::default_memory_pool())
      .ValueOrDie();
}

// Helper macro to reduce repetition in primitive builder switch.
#define PRIMITIVE_BUILDER_CASE(id, expr) \
  case DataTypeId::k##id:                \
    return expr;

std::unique_ptr<arrow::ArrayBuilder> create_primitive_array_builder(
    DataTypeId type_id) {
  switch (type_id) {
    PRIMITIVE_BUILDER_CASE(Boolean, std::make_unique<arrow::BooleanBuilder>())
    PRIMITIVE_BUILDER_CASE(Int8, std::make_unique<arrow::Int8Builder>())
    PRIMITIVE_BUILDER_CASE(Int16, std::make_unique<arrow::Int16Builder>())
    PRIMITIVE_BUILDER_CASE(Int32, std::make_unique<arrow::Int32Builder>())
    PRIMITIVE_BUILDER_CASE(Int64, std::make_unique<arrow::Int64Builder>())
    PRIMITIVE_BUILDER_CASE(UInt8, std::make_unique<arrow::UInt8Builder>())
    PRIMITIVE_BUILDER_CASE(UInt16, std::make_unique<arrow::UInt16Builder>())
    PRIMITIVE_BUILDER_CASE(UInt32, std::make_unique<arrow::UInt32Builder>())
    PRIMITIVE_BUILDER_CASE(UInt64, std::make_unique<arrow::UInt64Builder>())
    PRIMITIVE_BUILDER_CASE(Float, std::make_unique<arrow::FloatBuilder>())
    PRIMITIVE_BUILDER_CASE(Double, std::make_unique<arrow::DoubleBuilder>())
    PRIMITIVE_BUILDER_CASE(Varchar, std::make_unique<arrow::StringBuilder>())
    PRIMITIVE_BUILDER_CASE(Date, std::make_unique<arrow::Date64Builder>())
    PRIMITIVE_BUILDER_CASE(TimestampMs,
                           std::make_unique<arrow::TimestampBuilder>(
                               arrow::timestamp(arrow::TimeUnit::MILLI),
                               arrow::default_memory_pool()))
    PRIMITIVE_BUILDER_CASE(Interval, std::make_unique<arrow::StringBuilder>())
    PRIMITIVE_BUILDER_CASE(Empty, std::make_unique<arrow::NullBuilder>())
  default:
    return nullptr;
  }
}
#undef PRIMITIVE_BUILDER_CASE

std::unique_ptr<arrow::ArrayBuilder> create_arrow_list_array_builder(
    const StorageReadInterface& graph, DataType type,
    std::shared_ptr<execution::IContextColumn> col) {
  auto aux_info = dynamic_cast<const ListTypeInfo*>(type.RawExtraTypeInfo());
  assert(aux_info != nullptr);
  assert(aux_info->child_type.id() != DataTypeId::kInvalid);
  assert(aux_info->child_type.id() != DataTypeId::kUnknown);
  auto child_builder =
      create_arrow_array_builder(graph, aux_info->child_type, col);
  return std::make_unique<arrow::ListBuilder>(arrow::default_memory_pool(),
                                              std::move(child_builder));
}

std::unique_ptr<arrow::ArrayBuilder> create_arrow_struct_array_builder(
    const StorageReadInterface& graph, DataType type) {
  // TODO(zhanglei): support struct of arbitrary fields
  std::vector<std::shared_ptr<arrow::Field>> child_fields;
  auto struct_field_types = StructType::GetChildTypes(type);
  for (size_t i = 0; i < struct_field_types.size(); ++i) {
    child_fields.push_back(
        arrow::field("f" + std::to_string(i),
                     PropertyTypeToArrowType(struct_field_types[i])));
  }

  return arrow::MakeBuilder(arrow::struct_(child_fields),
                            arrow::default_memory_pool())
      .ValueOrDie();
}

std::unique_ptr<arrow::ArrayBuilder> create_arrow_map_array_builder(
    std::shared_ptr<arrow::DataType> key_type,
    std::vector<std::shared_ptr<arrow::DataType>> item_types) {
  std::vector<std::shared_ptr<arrow::Field>> entry_fields;
  std::vector<int8_t> type_codes;
  for (int i = 0; i < item_types.size(); ++i) {
    entry_fields.push_back(
        arrow::field("value_" + std::to_string(i), item_types[i]));
    type_codes.push_back(static_cast<int8_t>(i));
  }
  auto entry_type = arrow::sparse_union(entry_fields, type_codes);
  auto map_type = arrow::map(key_type, entry_type);
  return arrow::MakeBuilder(map_type, arrow::default_memory_pool())
      .ValueOrDie();
}

std::unique_ptr<arrow::ArrayBuilder> create_arrow_array_builder(
    const StorageReadInterface& graph, DataType type,
    std::shared_ptr<execution::IContextColumn> col) {
  if (auto primitive_builder = create_primitive_array_builder(type.id())) {
    return primitive_builder;
  }
  if (type.id() == DataTypeId::kList) {
    return create_arrow_list_array_builder(graph, type, col);
  } else if (type.id() == DataTypeId::kStruct) {
    return create_arrow_struct_array_builder(graph, type);
  } else if (type.id() == DataTypeId::kVertex) {
    assert(col);
    return create_arrow_map_array_builder(arrow::utf8(),
                                          LEGAL_GRAPH_PROPERTY_ARROW_TYPES);
  } else if (type.id() == DataTypeId::kEdge) {
    assert(col);
    return create_arrow_map_array_builder(arrow::utf8(),
                                          LEGAL_GRAPH_PROPERTY_ARROW_TYPES);
  } else if (type.id() == DataTypeId::kPath) {
    return create_path_array_builder(type);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "create_arrow_array_builder not support for type " +
        std::to_string(static_cast<int>(type.id())));
  }
}

#define APPEND_PROP_CASE(id, builder_t, expr)              \
  case DataTypeId::k##id: {                                \
    auto typed_builder = static_cast<builder_t*>(builder); \
    THROW_IF_ARROW_NOT_OK(typed_builder->Append(expr));    \
    break;                                                 \
  }

void append_property_to_builder(const Property& prop, DataTypeId type_id,
                                arrow::ArrayBuilder* builder) {
  switch (type_id) {
    APPEND_PROP_CASE(Boolean, arrow::BooleanBuilder, prop.as_bool())
    APPEND_PROP_CASE(Int32, arrow::Int32Builder, prop.as_int32())
    APPEND_PROP_CASE(UInt32, arrow::UInt32Builder, prop.as_uint32())
    APPEND_PROP_CASE(Int64, arrow::Int64Builder, prop.as_int64())
    APPEND_PROP_CASE(UInt64, arrow::UInt64Builder, prop.as_uint64())
    APPEND_PROP_CASE(Float, arrow::FloatBuilder, prop.as_float())
    APPEND_PROP_CASE(Double, arrow::DoubleBuilder, prop.as_double())
    APPEND_PROP_CASE(Date, arrow::Date64Builder, prop.as_date().to_timestamp())
    APPEND_PROP_CASE(
        TimestampMs, arrow::TimestampBuilder,
        prop.as_datetime().milli_second)  // timestamp builder requires millis
  case DataTypeId::kVarchar: {
    auto string_builder = static_cast<arrow::StringBuilder*>(builder);
    assert(string_builder != nullptr);
    const auto& str = prop.as_string_view();
    THROW_IF_ARROW_NOT_OK(string_builder->Append(str.data(), str.size()));
    break;
  }
  case DataTypeId::kInterval: {
    auto large_string_builder =
        static_cast<arrow::LargeStringBuilder*>(builder);
    assert(large_string_builder != nullptr);
    std::string interval_str = prop.as_interval().to_string();
    THROW_IF_ARROW_NOT_OK(
        large_string_builder->Append(interval_str.data(), interval_str.size()));
    break;
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "append_property_to_builder not support for type " +
        std::to_string(static_cast<int>(type_id)));
  }
}
#undef APPEND_PROP_CASE

uint8_t get_data_type_size(const DataTypeId& type) {
  switch (type) {
  case DataTypeId::kBoolean:
    return sizeof(bool);
  case DataTypeId::kInt32:
    return sizeof(int32_t);
  case DataTypeId::kUInt32:
    return sizeof(uint32_t);
  case DataTypeId::kInt64:
    return sizeof(int64_t);
  case DataTypeId::kUInt64:
    return sizeof(uint64_t);
  case DataTypeId::kFloat:
    return sizeof(float);
  case DataTypeId::kDouble:
    return sizeof(double);
  case DataTypeId::kDate:
    return sizeof(int32_t);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("get_data_type_size not support for type: " +
                                  std::to_string(type));
  }
}

///////////////// End of fixed size appender functions
/////////////////////

void ArrowTableBuilder::AddColumn(const std::string& name,
                                  std::shared_ptr<arrow::Array> array) {
  fields_.emplace_back(arrow::field(name, array->type(), false));
  columns_.emplace_back(array);
}

std::shared_ptr<arrow::Table> ArrowTableBuilder::Build() {
  auto schema = arrow::schema(fields_);
  return arrow::Table::Make(schema, columns_);
}

std::shared_ptr<arrow::Buffer> convert_vector_bool_to_bitmap(
    const std::vector<bool>& bitmap, size_t& null_count) {
  size_t byte_size = (bitmap.size() + 7) / 8;
  if (byte_size == 0) {
    null_count = 0;
    return nullptr;
  }
  auto maybe_buffer = arrow::AllocateBuffer(byte_size);
  THROW_IF_ARROW_NOT_OK(maybe_buffer.status());
  auto&& buffer = maybe_buffer.ValueOrDie();
  std::memset(buffer->mutable_data(), 0, byte_size);

  for (size_t i = 0; i < bitmap.size(); ++i) {
    if (bitmap[i]) {
      size_t byte_index = i >> 3;   // divide by 8
      size_t bit_offset = i & 0x7;  // modulo 8
      buffer->mutable_data()[byte_index] |=
          static_cast<uint8_t>(1u << bit_offset);
    } else {
      ++null_count;
    }
  }
  return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

}  // namespace neug