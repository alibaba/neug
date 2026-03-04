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

#include "neug/execution/common/operators/retrieve/sink.h"

#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/arrow_array_builder.h"
#include "neug/utils/encoder.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace execution {

void append_vertex_to_builder(const StorageReadInterface& graph,
                              const VertexRecord& record,
                              arrow::MapBuilder* map_builder);
void append_edge_to_builder(const StorageReadInterface& graph,
                            const EdgeRecord& record,
                            arrow::MapBuilder* map_builder);

inline void append_rt_any_to_builder(const StorageReadInterface& graph,
                                     const execution::Value& val,
                                     arrow::ArrayBuilder* builder) {
  auto type = val.type();
  if (type == DataType::BOOLEAN) {
    THROW_IF_ARROW_NOT_OK(
        // std::dynamic_pointer_cast<arrow::BooleanBuilder>(builder)->Append(
        dynamic_cast<arrow::BooleanBuilder*>(builder)->Append(
            val.GetValue<bool>()));
  } else if (type == DataType::INT32) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::Int32Builder*>(builder)->Append(
        val.GetValue<int32_t>()));
  } else if (type == DataType::UINT32) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::UInt32Builder*>(builder)->Append(
        val.GetValue<uint32_t>()));
  } else if (type == DataType::INT64) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::Int64Builder*>(builder)->Append(
        val.GetValue<int64_t>()));
  } else if (type == DataType::UINT64) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::UInt64Builder*>(builder)->Append(
        val.GetValue<uint64_t>()));
  } else if (type == DataType::FLOAT) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::FloatBuilder*>(builder)->Append(
        val.GetValue<float>()));
  } else if (type == DataType::DOUBLE) {
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::DoubleBuilder*>(builder)->Append(
        val.GetValue<double>()));
  } else if (type == DataType::VARCHAR) {
    if (builder->type()->id() == arrow::Type::STRING) {
      THROW_IF_ARROW_NOT_OK(
          dynamic_cast<arrow::StringBuilder*>(builder)->Append(
              val.GetValue<std::string>()));
    } else {
      assert(builder->type()->id() == arrow::Type::LARGE_STRING);
      THROW_IF_ARROW_NOT_OK(
          dynamic_cast<arrow::LargeStringBuilder*>(builder)->Append(
              val.GetValue<std::string>()));
    }
  } else if (type == DataType::DATE) {
    auto date_val = val.GetValue<Date>();
    THROW_IF_ARROW_NOT_OK(dynamic_cast<arrow::Date64Builder*>(builder)->Append(
        date_val.to_timestamp()));
  } else if (type == DataType::TIMESTAMP_MS) {
    auto dt_val = val.GetValue<DateTime>();
    THROW_IF_ARROW_NOT_OK(
        dynamic_cast<arrow::TimestampBuilder*>(builder)->Append(
            dt_val.milli_second));
  } else if (type.id() == DataTypeId::kStruct) {
    auto struct_builder =
        // std::dynamic_pointer_cast<arrow::StructBuilder>(builder);
        dynamic_cast<arrow::StructBuilder*>(builder);
    THROW_IF_ARROW_NOT_OK(struct_builder->Append());
    auto tuple_val = execution::StructValue::GetChildren(val);
    for (size_t i = 0; i < tuple_val.size(); ++i) {
      append_rt_any_to_builder(graph, tuple_val[i],
                               struct_builder->child_builder(i).get());
    }
  } else if (type.id() == DataTypeId::kList) {
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
    assert(list_builder != nullptr);
    THROW_IF_ARROW_NOT_OK(list_builder->Append());
    auto value_builder = list_builder->value_builder();
    auto list_values = execution::ListValue::GetChildren(val);
    for (int i = 0; i < list_values.size(); ++i) {
      append_rt_any_to_builder(graph, list_values[i], value_builder);
    }
  } else if (type.id() == DataTypeId::kNull) {
    THROW_IF_ARROW_NOT_OK(builder->AppendNull());
  } else if (type.id() == DataTypeId::kVertex) {
    auto vertex_record = val.GetValue<execution::vertex_t>();
    auto map_builder = dynamic_cast<arrow::MapBuilder*>(builder);
    assert(map_builder != nullptr);
    append_vertex_to_builder(graph, vertex_record, map_builder);
  } else if (type.id() == DataTypeId::kEdge) {
    auto edge_record = val.GetValue<execution::edge_t>();
    auto map_builder = dynamic_cast<arrow::MapBuilder*>(builder);
    assert(map_builder != nullptr);
    append_edge_to_builder(graph, edge_record, map_builder);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "append_rt_any_to_builder not support for type " +
        std::to_string(static_cast<int>(type.id())));
  }
}

inline void append_rt_any_to_builder(
    const StorageReadInterface& graph, const execution::Value& val,
    const std::shared_ptr<arrow::ArrayBuilder>& builder) {
  append_rt_any_to_builder(graph, val, builder.get());
}

template <typename T>
inline void wrap_primitive_type_array(
    const T* data, size_t size, const std::vector<bool>& validity_bitmap,
    std::shared_ptr<IArrowArrayBuilder> builder) {
  auto casted_builder =
      std::static_pointer_cast<ArrowArrayWrapBuilder<T>>(builder);
  if (!casted_builder) {
    THROW_NOT_SUPPORTED_EXCEPTION("Builder mismatch for type: expected " +
                                  std::string(typeid(T).name()) +
                                  " but got builder type");
  }
  casted_builder->Wrap(data, size, validity_bitmap);
}

#define VISIT_AND_WRAP_VALUE_COLUMN(TYPE, TYPE_ENUM)                           \
  case DataTypeId::TYPE_ENUM: {                                                \
    auto casted = std::static_pointer_cast<ValueColumn<TYPE>>(col);            \
    wrap_primitive_type_array<TYPE>(casted->data().data(), casted->size(),     \
                                    casted->validity_bitmap(), array_builder); \
                                                                               \
    break;                                                                     \
  }

template <typename T>
inline void append_to_builder(const std::vector<T>& data,
                              const std::vector<bool>& validity_bitmap,
                              arrow::ArrayBuilder* builder) {
  auto casted_builder =
      static_cast<typename TypeConverter<T>::ArrowBuilderType*>(builder);
  if (!casted_builder) {
    THROW_NOT_SUPPORTED_EXCEPTION("Builder mismatch for type: expected " +
                                  std::string(typeid(T).name()) +
                                  " but got builder type");
  }
  THROW_IF_ARROW_NOT_OK(casted_builder->Reserve(data.size()));
  if (!validity_bitmap.empty()) {
    for (size_t i = 0; i < data.size(); ++i) {
      if (validity_bitmap[i]) {
        THROW_IF_ARROW_NOT_OK(
            casted_builder->Append(TypeConverter<T>::ToArrowCType(data[i])));
      } else {
        THROW_IF_ARROW_NOT_OK(casted_builder->AppendNull());
      }
    }
  } else {
    for (const auto& item : data) {
      THROW_IF_ARROW_NOT_OK(
          casted_builder->Append(TypeConverter<T>::ToArrowCType(item)));
    }
  }
}

#define VISIT_AND_APPEND_VALUE_COLUMN(TYPE, TYPE_ENUM)                 \
  case DataTypeId::TYPE_ENUM: {                                        \
    auto casted = std::static_pointer_cast<ValueColumn<TYPE>>(col);    \
    append_to_builder<TYPE>(casted->data(), casted->validity_bitmap(), \
                            array_builder);                            \
                                                                       \
    break;                                                             \
  }

// For String column, we could not Use UnsafeAppend directly
void append_string_value_column(const std::shared_ptr<IContextColumn>& col,
                                arrow::ArrayBuilder* array_builder) {
  if (col->is_optional()) {
    auto str_col = std::static_pointer_cast<ValueColumn<std::string>>(col);
    auto str_builder =
        static_cast<TypeConverter<std::string>::ArrowBuilderType*>(
            array_builder);
    for (size_t i = 0; i < str_col->size(); ++i) {
      if (str_col->has_value(i)) {
        THROW_IF_ARROW_NOT_OK(str_builder->Append(str_col->get_value(i)));
      } else {
        THROW_IF_ARROW_NOT_OK(str_builder->AppendNull());
      }
    }
  } else {
    auto str_col = std::static_pointer_cast<ValueColumn<std::string>>(col);
    auto str_builder =
        static_cast<TypeConverter<std::string>::ArrowBuilderType*>(
            array_builder);
    THROW_IF_ARROW_NOT_OK(str_builder->AppendValues(str_col->data()));
  }
}

void append_list_column(const StorageReadInterface& graph,
                        const std::shared_ptr<IContextColumn>& col,
                        arrow::ArrayBuilder* array_builder) {
  auto list_col = std::static_pointer_cast<ListColumn>(col);
  auto list_builder = static_cast<arrow::ListBuilder*>(array_builder);
  auto child_builder = list_builder->value_builder();
  for (size_t i = 0; i < list_col->size(); ++i) {
    auto val = list_col->get_elem(i);
    THROW_IF_ARROW_NOT_OK(list_builder->Append());
    const auto& vals = ListValue::GetChildren(val);
    for (size_t j = 0; j < vals.size(); ++j) {
      append_rt_any_to_builder(graph, vals[j], child_builder);
    }
  }
}

void append_struct_column(const StorageReadInterface& graph,
                          const std::shared_ptr<IContextColumn>& col,
                          arrow::ArrayBuilder* array_builder) {
  auto struct_builder = dynamic_cast<arrow::StructBuilder*>(array_builder);
  assert(struct_builder);
  auto struct_col = std::dynamic_pointer_cast<StructColumn>(col);
  for (size_t i = 0; i < struct_col->size(); ++i) {
    if (struct_col->has_value(i)) {
      THROW_IF_ARROW_NOT_OK(struct_builder->Append());
      auto val = struct_col->get_elem(i);
      const auto& childs = StructValue::GetChildren(val);
      for (size_t j = 0; j < childs.size(); ++j) {
        auto child_builder = struct_builder->child_builder(j);
        if (childs[j].IsNull()) {
          THROW_IF_ARROW_NOT_OK(child_builder->AppendNull());
          continue;
        }
        append_rt_any_to_builder(graph, childs[j], child_builder.get());
      }
    } else {
      THROW_IF_ARROW_NOT_OK(struct_builder->AppendNull());
      for (size_t j = 0; j < struct_builder->num_fields(); ++j) {
        auto child_builder = struct_builder->child_builder(j);
        THROW_IF_ARROW_NOT_OK(child_builder->AppendNull());
      }
    }
  }
}
std::shared_ptr<arrow::Array> visit_and_build_value_column(
    const StorageReadInterface& graph,
    const std::shared_ptr<IContextColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto ele_type = col->elem_type();
  switch (ele_type.id()) {
    VISIT_AND_APPEND_VALUE_COLUMN(bool, kBoolean)
    VISIT_AND_APPEND_VALUE_COLUMN(Date, kDate)
    VISIT_AND_APPEND_VALUE_COLUMN(DateTime, kTimestampMs)
    VISIT_AND_APPEND_VALUE_COLUMN(Interval, kInterval)
  case DataTypeId::kVarchar:
    append_string_value_column(col, array_builder);
    break;
  case DataTypeId::kList:
    append_list_column(graph, col, array_builder);
    break;
  case DataTypeId::kStruct:
    append_struct_column(graph, col, array_builder);
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "visit_and_build_value_column not support for " + col->column_info());
  }

  return array_builder->Finish().ValueOrDie();
}

void append_property_to_sparse_union_builder(
    const Property& prop, arrow::SparseUnionBuilder* builder) {
  auto type_id = prop.type();
  auto arrow_type = builder->type();
  const auto& union_type =
      static_cast<const arrow::SparseUnionType&>(*arrow_type);
  int8_t type_code = -1;
  for (int i = 0; i < union_type.num_fields(); ++i) {
    if (union_type.field(i)->type()->id() ==
        PropertyTypeToArrowType(type_id)->id()) {
      type_code = union_type.type_codes()[i];
      break;
    }
  }
  if (type_code == -1) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "append_property_to_sparse_union_builder not support for type " +
        std::to_string(static_cast<int>(type_id)));
  }
  THROW_IF_ARROW_NOT_OK(builder->Append(type_code));
  for (int i = 0; i < union_type.num_fields(); ++i) {
    if (union_type.type_codes()[i] != type_code) {
      THROW_IF_ARROW_NOT_OK(builder->child_builder(i)->AppendNull());
    }
  }
  auto child_builder = builder->child_builder(type_code);
  append_property_to_builder(prop, type_id, child_builder.get());
}

void append_vertex_to_builder(const StorageReadInterface& graph,
                              const VertexRecord& record,
                              arrow::MapBuilder* map_builder) {
  if (record.label_ >= graph.schema().vertex_label_num() ||
      record.vid_ == std::numeric_limits<vid_t>::max()) {
    THROW_IF_ARROW_NOT_OK(map_builder->AppendNull());
    return;
  }
  auto property_types = graph.schema().get_vertex_properties(record.label_);
  THROW_IF_ARROW_NOT_OK(map_builder->Append());
  auto pk_types = graph.schema().get_vertex_primary_key(record.label_);
  assert(pk_types.size() == 1);
  auto type_name = graph.schema().get_vertex_label_name(record.label_);
  auto key_builder =
      dynamic_cast<arrow::StringBuilder*>(map_builder->key_builder());
  auto item_builder =
      dynamic_cast<arrow::SparseUnionBuilder*>(map_builder->item_builder());
  assert(item_builder && key_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_LABEL"));
  append_property_to_sparse_union_builder(Property::from_string_view(type_name),
                                          item_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_ID"));
  auto gid = encode_unique_vertex_id(record.label_, record.vid_);
  append_property_to_sparse_union_builder(Property::from_int64(gid),
                                          item_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append(std::get<1>(pk_types[0])));
  auto pk_prop = graph.GetVertexId(record.label_, record.vid_);
  append_property_to_sparse_union_builder(pk_prop, item_builder);
  for (size_t i = 0; i < property_types.size(); ++i) {
    auto field_name =
        graph.schema().get_vertex_property_names(record.label_)[i];
    THROW_IF_ARROW_NOT_OK(key_builder->Append(field_name));
    auto prop = graph.GetVertexProperty(record.label_, record.vid_, i);
    append_property_to_sparse_union_builder(prop, item_builder);
  }
}

void append_edge_to_builder(const StorageReadInterface& graph,
                            const EdgeRecord& record,
                            arrow::MapBuilder* map_builder) {
  if (record.label.src_label >= graph.schema().vertex_label_num() ||
      record.label.dst_label >= graph.schema().vertex_label_num() ||
      record.label.edge_label >= graph.schema().edge_label_num() ||
      record.src == std::numeric_limits<vid_t>::max() ||
      record.dst == std::numeric_limits<vid_t>::max()) {
    THROW_IF_ARROW_NOT_OK(map_builder->AppendNull());
    return;
  }
  auto property_types = graph.schema().get_edge_properties(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  auto property_names = graph.schema().get_edge_property_names(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  THROW_IF_ARROW_NOT_OK(map_builder->Append());
  auto edge_label = generate_edge_label_id(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  auto key_builder =
      dynamic_cast<arrow::StringBuilder*>(map_builder->key_builder());
  assert(key_builder);
  auto item_builder =
      dynamic_cast<arrow::SparseUnionBuilder*>(map_builder->item_builder());
  assert(item_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_LABEL"));
  auto edge_label_name =
      graph.schema().get_edge_label_name(record.label.edge_label);
  append_property_to_sparse_union_builder(
      Property::from_string_view(edge_label_name), item_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_ID"));
  append_property_to_sparse_union_builder(
      Property::from_int64(
          encode_unique_edge_id(edge_label, record.src, record.dst)),
      item_builder);
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_SRC_ID"));
  THROW_IF_ARROW_NOT_OK(key_builder->Append("_DST_ID"));
  auto src_gid = encode_unique_vertex_id(record.label.src_label, record.src);
  auto dst_gid = encode_unique_vertex_id(record.label.dst_label, record.dst);
  append_property_to_sparse_union_builder(Property::from_int64(src_gid),
                                          item_builder);
  append_property_to_sparse_union_builder(Property::from_int64(dst_gid),
                                          item_builder);
  for (size_t i = 0; i < property_types.size(); ++i) {
    auto prop =
        graph
            .GetEdgeDataAccessor(record.label.src_label, record.label.dst_label,
                                 record.label.edge_label, i)
            .get_data_from_ptr(record.prop);
    THROW_IF_ARROW_NOT_OK(key_builder->Append(property_names[i]));
    append_property_to_sparse_union_builder(prop, item_builder);
  }
}

std::shared_ptr<arrow::Array> visit_and_build_vertex_column(
    const StorageReadInterface& graph,
    const std::shared_ptr<IVertexColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto casted = dynamic_cast<arrow::MapBuilder*>(array_builder);
  if (!casted) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Builder mismatch for vertex column: expected MapBuilder but "
        "got builder type " +
        std::string(typeid(*array_builder).name()));
  }

  for (int i = 0; i < col->size(); ++i) {
    auto record = col->get_vertex(i);
    append_vertex_to_builder(graph, record, casted);
  }
  return casted->Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> visit_and_build_edge_column(
    const StorageReadInterface& graph, const std::shared_ptr<IEdgeColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto casted = dynamic_cast<arrow::MapBuilder*>(array_builder);
  if (!casted) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Builder mismatch for edge column: expected MapBuilder but got builder "
        "type " +
        std::string(typeid(*array_builder).name()));
  }
  for (int i = 0; i < col->size(); ++i) {
    auto record = col->get_edge(i);
    append_edge_to_builder(graph, record, casted);
  }
  return casted->Finish().ValueOrDie();
}

void append_vertex_to_path_builder(const StorageReadInterface& graph,
                                   const VertexRecord& record,
                                   arrow::StructBuilder* struct_builder) {
  // Only append GlobalId and label name
  assert(struct_builder->num_fields() == 2);
  auto label_builder = dynamic_cast<arrow::StringBuilder*>(
      struct_builder->child_builder(0).get());
  auto gid_builder = dynamic_cast<arrow::Int64Builder*>(
      struct_builder->child_builder(1).get());
  assert(gid_builder && label_builder);
  THROW_IF_ARROW_NOT_OK(
      gid_builder->Append(encode_unique_vertex_id(record.label_, record.vid_)));
  auto label_name = graph.schema().get_vertex_label_name(record.label_);
  THROW_IF_ARROW_NOT_OK(label_builder->Append(label_name));
}

void append_nodes_to_path_builder(const StorageReadInterface& graph,
                                  const std::vector<VertexRecord>& nodes,
                                  arrow::ListBuilder* list_builder) {
  auto element_builder =
      dynamic_cast<arrow::StructBuilder*>(list_builder->value_builder());
  THROW_IF_ARROW_NOT_OK(list_builder->Append());
  for (const auto& node : nodes) {
    THROW_IF_ARROW_NOT_OK(element_builder->Append());
    append_vertex_to_path_builder(graph, node, element_builder);
  }
}

void append_edges_to_path_builder(const StorageReadInterface& graph,
                                  const std::vector<EdgeRecord>& edges,
                                  arrow::ListBuilder* list_builder) {
  auto element_builder =
      dynamic_cast<arrow::StructBuilder*>(list_builder->value_builder());
  THROW_IF_ARROW_NOT_OK(list_builder->Append());
  for (const auto& edge : edges) {
    THROW_IF_ARROW_NOT_OK(element_builder->Append());
    // Only append edge_label name and direction
    assert(element_builder->num_fields() == 2);
    auto label_builder = dynamic_cast<arrow::StringBuilder*>(
        element_builder->child_builder(0).get());
    auto direction_builder = dynamic_cast<arrow::StringBuilder*>(
        element_builder->child_builder(1).get());
    assert(label_builder && direction_builder);
    auto edge_label_name =
        graph.schema().get_edge_label_name(edge.label.edge_label);
    THROW_IF_ARROW_NOT_OK(label_builder->Append(edge_label_name));
    THROW_IF_ARROW_NOT_OK(
        direction_builder->Append(edge.dir == Direction::kIn ? "IN" : "OUT"));
  }
}

std::shared_ptr<arrow::Array> visit_and_build_path_column(
    const StorageReadInterface& graph,
    const std::shared_ptr<IContextColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto casted = dynamic_cast<arrow::StructBuilder*>(array_builder);
  if (!casted) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Builder mismatch for path column: expected StructBuilder but got "
        "builder type " +
        std::string(typeid(*array_builder).name()));
  }
  auto path_col = std::dynamic_pointer_cast<PathColumn>(col);
  assert(path_col);
  for (int i = 0; i < path_col->size(); ++i) {
    const auto& path = path_col->get_path(i);
    THROW_IF_ARROW_NOT_OK(casted->Append());
    auto nodes_builder =
        dynamic_cast<arrow::ListBuilder*>(casted->child_builder(0).get());
    auto edges_builder =
        dynamic_cast<arrow::ListBuilder*>(casted->child_builder(1).get());
    auto weight_builder =
        dynamic_cast<arrow::DoubleBuilder*>(casted->child_builder(2).get());
    auto length_builder =
        dynamic_cast<arrow::Int32Builder*>(casted->child_builder(3).get());
    assert(nodes_builder && edges_builder && weight_builder && length_builder);
    append_nodes_to_path_builder(graph, path.nodes(), nodes_builder);
    append_edges_to_path_builder(graph, path.relationships(), edges_builder);

    THROW_IF_ARROW_NOT_OK(
        length_builder->Append(static_cast<int32_t>(path.length())));
    THROW_IF_ARROW_NOT_OK(weight_builder->Append(path.get_weight()));
  }
  return casted->Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> visit_and_build_arrow_array_column(
    const std::shared_ptr<IContextColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto arrow_array_col =
      std::dynamic_pointer_cast<ArrowArrayContextColumn>(col);
  assert(arrow_array_col);
  auto columns = arrow_array_col->GetColumns();
  // Create struct array from the columns
  assert(columns.size() >= 1);
  if (columns.size() == 1) {
    return columns[0];
  } else {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t i = 0; i < columns.size(); ++i) {
      fields.push_back(
          arrow::field("field_" + std::to_string(i), columns[i]->type()));
      arrays.push_back(columns[i]);
    }
    auto struct_type = arrow::struct_(fields);
    return std::make_shared<arrow::StructArray>(struct_type,
                                                arrays[0]->length(), arrays);
  }
}

std::shared_ptr<arrow::Array> visit_and_build_array(
    const StorageReadInterface& graph,
    const std::shared_ptr<IContextColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto col_type = col->column_type();
  if (col_type == ContextColumnType::kValue) {
    return visit_and_build_value_column(graph, col, array_builder);
  } else if (col_type == ContextColumnType::kVertex) {
    return visit_and_build_vertex_column(
        graph, std::dynamic_pointer_cast<IVertexColumn>(col), array_builder);
  } else if (col_type == ContextColumnType::kEdge) {
    return visit_and_build_edge_column(
        graph, std::dynamic_pointer_cast<IEdgeColumn>(col), array_builder);
  } else if (col_type == ContextColumnType::kPath) {
    return visit_and_build_path_column(graph, col, array_builder);
  } else if (col_type == ContextColumnType::kArrowArray) {
    return visit_and_build_arrow_array_column(col, array_builder);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("visit_and_build_array not support for " +
                                  col->column_info());
  }
}

std::shared_ptr<arrow::Array> wrap_pod_array_from_context_column(
    const std::shared_ptr<IContextColumn>& col) {
  auto ele_type = col->elem_type();
  auto array_builder = create_wrap_array_builder(ele_type);
  switch (ele_type.id()) {
    VISIT_AND_WRAP_VALUE_COLUMN(int32_t, kInt32);
    VISIT_AND_WRAP_VALUE_COLUMN(uint32_t, kUInt32);
    VISIT_AND_WRAP_VALUE_COLUMN(int64_t, kInt64);
    VISIT_AND_WRAP_VALUE_COLUMN(uint64_t, kUInt64);
    VISIT_AND_WRAP_VALUE_COLUMN(float, kFloat);
    VISIT_AND_WRAP_VALUE_COLUMN(double, kDouble);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "wrap_pod_array_from_context_column not support for " +
        col->column_info());
  }
  return array_builder->Build();
}

QueryResult Sink::sink_neug(const Context& ctx,
                            const StorageReadInterface& graph) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<IContextColumn>> columns;
  for (size_t j : ctx.tag_ids) {
    auto col = ctx.get(j);
    if (col) {
      std::shared_ptr<arrow::Array> array;
      auto ele_type = col->elem_type();
      if (col->column_type() == ContextColumnType::kValue &&
          (ele_type == DataType::INT32 || ele_type == DataType::UINT32 ||
           ele_type == DataType::INT64 || ele_type == DataType::UINT64 ||
           ele_type == DataType::FLOAT || ele_type == DataType::DOUBLE)) {
        array = wrap_pod_array_from_context_column(col);
        columns.push_back(col);
      } else {
        auto array_builder =
            create_arrow_array_builder(graph, col->elem_type(), col);
        array = visit_and_build_array(graph, col, array_builder.get());
      }

      fields.push_back(arrow::field("tag_" + std::to_string(j), array->type()));
      arrays.push_back(array);
    }
  }
  return QueryResult(std::move(fields), std::move(arrays), std::move(columns),
                     SerializationProtocol::kArrowIPC);
}

}  // namespace execution
}  // namespace neug