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
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/arrow_array_builder.h"
#include "neug/utils/encoder.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace execution {

static void sink_any(const Property& any, common::Value* value) {
  if (any.type() == DataTypeId::kInt64) {
    value->set_i64(any.as_int64());
  } else if (any.type() == DataTypeId::kVarchar) {
    auto str = any.as_string_view();
    value->set_str(str.data(), str.size());
  } else if (any.type() == DataTypeId::kDate) {
    value->set_u32(any.as_date().to_u32());
  } else if (any.type() == DataTypeId::kInt32) {
    value->set_i32(any.as_int32());
  } else if (any.type() == DataTypeId::kUInt32) {
    value->set_u32(any.as_uint32());
  } else if (any.type() == DataTypeId::kDouble) {
    value->set_f64(any.as_double());
  } else if (any.type() == DataTypeId::kBoolean) {
    value->set_boolean(any.as_bool());
  } else if (any.type() == DataTypeId::kEmpty) {
    value->mutable_none();
  } else if (any.type() == DataTypeId::kDate) {
    value->mutable_date()->set_item(any.as_date().to_num_days());
  } else if (any.type() == DataTypeId::kTimestampMs) {
    value->mutable_timestamp()->set_item(any.as_datetime().milli_second);
  } else if (any.type() == DataTypeId::kUInt64) {
    value->set_u64(any.as_uint64());
  } else if (any.type() == DataTypeId::kInterval) {
    auto interval_str = any.as_interval().to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else if (any.type() == DataTypeId::kFloat) {
    value->set_f32(any.as_float());
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink_any not support for " +
                                  std::to_string(any.type()) +
                                  " value: " + any.to_string());
  }
}

static void sink_property(const Property& prop, common::Value* value) {
  if (prop.type() == DataTypeId::kInt32) {
    value->set_i32(prop.as_int32());
  } else if (prop.type() == DataTypeId::kUInt32) {
    value->set_u32(prop.as_uint32());
  } else if (prop.type() == DataTypeId::kVarchar) {
    auto str = prop.as_string_view();
    value->set_str(str.data(), str.size());
  } else if (prop.type() == DataTypeId::kInt64) {
    value->set_i64(prop.as_int64());
  } else if (prop.type() == DataTypeId::kUInt64) {
    value->set_u64(prop.as_uint64());
  } else if (prop.type() == DataTypeId::kDouble) {
    value->set_f64(prop.as_double());
  } else if (prop.type() == DataTypeId::kFloat) {
    value->set_f32(prop.as_float());
  } else if (prop.type() == DataTypeId::kDate) {
    value->set_u32(prop.as_date().to_u32());
  } else if (prop.type() == DataTypeId::kEmpty) {
    value->mutable_none();
  } else if (prop.type() == DataTypeId::kTimestampMs) {
    value->mutable_timestamp()->set_item(prop.as_datetime().milli_second);
  } else if (prop.type() == DataTypeId::kInterval) {
    auto interval_str = prop.as_interval().to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else {
    LOG(FATAL) << "property value: " << prop.to_string()
               << ", type = " << std::to_string(prop.type());
  }
}

static bool sink_vertex(const StorageReadInterface& graph,
                        const VertexRecord& vertex, results::Vertex* v) {
  if (vertex.label_ >= graph.schema().vertex_label_frontier() ||
      vertex.vid_ == std::numeric_limits<vid_t>::max()) {
    return false;
  }

  v->mutable_label()->set_name(
      graph.schema().get_vertex_label_name(vertex.label_));
  v->set_id(encode_unique_vertex_id(vertex.label_, vertex.vid_));
  const auto& names = graph.schema().get_vertex_property_names(vertex.label_);
  // Add primary keys first, since primary keys are also the properties of a
  // vertex
  auto& primary_key = graph.schema().get_vertex_primary_key(vertex.label_);
  if (primary_key.size() > 1) {
    LOG(ERROR) << "Currently only support single primary key";
  }
  auto pk_name = std::get<1>(primary_key[0]);
  auto pk_prop = v->add_properties();
  pk_prop->mutable_key()->set_name(pk_name);
  sink_any(graph.GetVertexId(vertex.label_, vertex.vid_),
           pk_prop->mutable_value());

  for (size_t i = 0; i < names.size(); ++i) {
    auto prop = v->add_properties();
    prop->mutable_key()->set_name(names[i]);
    sink_any(graph.GetVertexProperty(vertex.label_, vertex.vid_, i),
             prop->mutable_value());
  }
  return true;
}

static bool sink_edge(const StorageReadInterface& graph, const EdgeRecord& edge,
                      results::Edge* e) {
  e->mutable_src_label()->set_name(
      graph.schema().get_vertex_label_name(edge.label.src_label));
  e->mutable_dst_label()->set_name(
      graph.schema().get_vertex_label_name(edge.label.dst_label));
  auto edge_label = generate_edge_label_id(
      edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
  e->mutable_label()->set_name(
      graph.schema().get_edge_label_name(edge.label.edge_label));
  e->set_src_id(encode_unique_vertex_id(edge.label.src_label, edge.src));
  e->set_dst_id(encode_unique_vertex_id(edge.label.dst_label, edge.dst));
  e->set_id(encode_unique_edge_id(edge_label, edge.src, edge.dst));
  auto prop_names = graph.schema().get_edge_property_names(
      edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
  for (size_t i = 0; i < prop_names.size(); ++i) {
    auto props = e->add_properties();
    props->mutable_key()->set_name(prop_names[i]);
    auto ed_accessor = graph.GetEdgeDataAccessor(
        edge.label.src_label, edge.label.dst_label, edge.label.edge_label, i);
    auto ep = ed_accessor.get_data_from_ptr(edge.prop);
    sink_property(ep, props->mutable_value());
  }
  return true;
}

static void sink_impl(const Value& val, common::Value* value) {
  if (val.IsNull()) {
    value->mutable_none();
    return;
  }
  switch (val.type().id()) {
  case DataTypeId::kBoolean:
    value->set_boolean(val.GetValue<bool>());
    break;
  case DataTypeId::kInt32:
    value->set_i32(val.GetValue<int32_t>());
    break;
  case DataTypeId::kUInt32:
    value->set_u32(val.GetValue<uint32_t>());
    break;
  case DataTypeId::kInt64:
    value->set_i64(val.GetValue<int64_t>());
    break;
  case DataTypeId::kUInt64:
    value->set_u64(val.GetValue<uint64_t>());
    break;
  case DataTypeId::kFloat:
    value->set_f32(val.GetValue<float>());
    break;
  case DataTypeId::kDouble:
    value->set_f64(val.GetValue<double>());
    break;
  case DataTypeId::kVarchar: {
    auto str = val.GetValue<std::string>();
    value->set_str(str.data(), str.size());
    break;
  }
  case DataTypeId::kDate: {
    auto date = val.GetValue<date_t>().to_string();
    value->set_str(date.data(), date.size());
    break;
  }
  case DataTypeId::kTimestampMs: {
    auto datetime = val.GetValue<timestamp_ms_t>().to_string();
    value->set_str(datetime.data(), datetime.size());
    break;
  }
  case DataTypeId::kInterval: {
    auto interval = val.GetValue<interval_t>().to_string();
    value->set_str(interval.data(), interval.size());
    break;
  }
  case DataTypeId::kNull:
    value->mutable_none();
    break;
  case DataTypeId::kStruct: {
    const auto& struct_fields = StructValue::GetChildren(val);
    for (const auto& field : struct_fields) {
      const auto& str = field.to_string();
      value->mutable_str_array()->add_item(str.data(), str.size());
      break;
    }
  }
  default:
    LOG(FATAL) << "Not supported sink for type: "
               << static_cast<int>(val.type().id());
  }
}

static void sink_entry(const Value& rt_val, const StorageReadInterface& graph,
                       results::Entry* entry) {
  if (rt_val.IsNull()) {
    entry->mutable_element()->mutable_object()->mutable_none();
    return;
  }
  auto type_ = rt_val.type();
  if (type_.id() == DataTypeId::kList) {
    auto collection = entry->mutable_collection();
    const auto& vals = ListValue::GetChildren(rt_val);
    for (const auto& val : vals) {
      // convert each element in the list to the target entry by recursive
      // invocation
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_.id() == DataTypeId::kStruct) {
    auto collection = entry->mutable_collection();
    const auto& vals = StructValue::GetChildren(rt_val);
    for (const auto& val : vals) {
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_.id() == DataTypeId::kVertex) {
    auto ele = entry->mutable_element();
    if (!sink_vertex(graph, rt_val.GetValue<vertex_t>(),
                     ele->mutable_vertex())) {
      ele->mutable_object()->mutable_none();
    }
  } else if (type_.id() == DataTypeId::kEdge) {
    sink_edge(graph, rt_val.GetValue<edge_t>(),
              entry->mutable_element()->mutable_edge());
  } else if (type_.id() == DataTypeId::kPath) {
    auto mutable_path = entry->mutable_element()->mutable_graph_path();
    auto path_nodes = PathValue::Get(rt_val).nodes();
    auto edges = PathValue::Get(rt_val).relationships();

    assert(edges.size() + 1 == path_nodes.size());
    size_t len = edges.size();
    for (size_t i = 0; i < len; ++i) {
      auto vertex_in_path = mutable_path->add_path();

      auto node = vertex_in_path->mutable_vertex();
      // node->mutable_label()->set_id(path_nodes[i].label());

      node->mutable_label()->set_name(
          graph.schema().get_vertex_label_name(edges[i].start_node().label()));
      node->set_id(encode_unique_vertex_id(edges[i].start_node().label(),
                                           edges[i].start_node().vid()));
      auto edge_in_path = mutable_path->add_path();

      auto edge = edge_in_path->mutable_edge();
      edge->mutable_src_label()->set_name(
          graph.schema().get_vertex_label_name(edges[i].start_node().label()));
      edge->mutable_dst_label()->set_name(
          graph.schema().get_vertex_label_name(edges[i].end_node().label()));
      edge->mutable_label()->set_name(
          graph.schema().get_edge_label_name(edges[i].label.edge_label));
      edge->set_id(encode_unique_edge_id(edges[i].label.edge_label,
                                         edges[i].start_node().vid(),
                                         edges[i].end_node().vid()));
      edge->set_src_id(encode_unique_vertex_id(edges[i].start_node().label(),
                                               edges[i].start_node().vid()));
      edge->set_dst_id(encode_unique_vertex_id(edges[i].end_node().label(),
                                               edges[i].end_node().vid()));
    }
    auto vertex_in_path = mutable_path->add_path();
    auto node = vertex_in_path->mutable_vertex();
    // node->mutable_label()->set_id(path_nodes[len - 1].label());
    node->mutable_label()->set_name(graph.schema().get_vertex_label_name(
        edges[len - 1].end_node().label()));
    node->set_id(encode_unique_vertex_id(edges[len - 1].end_node().label(),
                                         edges[len - 1].end_node().vid()));
  } else {
    sink_impl(rt_val, entry->mutable_element()->mutable_object());
  }
}

static void sink_value(const Value& val, const StorageReadInterface& graph,
                       int id, results::Column* col) {
  col->mutable_name_or_id()->set_id(id);
  sink_entry(val, graph, col->mutable_entry());
}

static void sink_value(const Value& val, const StorageReadInterface& graph,
                       Encoder& encoder) {
  auto type_ = val.type();
  if (val.IsNull()) {
    encoder.put_string_view("");
    return;
  }
  if (type_.id() == DataTypeId::kList) {
    const auto& vals = ListValue::GetChildren(val);
    encoder.put_int(vals.size());
    for (const auto& val : vals) {
      sink_value(val, graph, encoder);
    }
  } else if (type_.id() == DataTypeId::kStruct) {
    const auto& vals = StructValue::GetChildren(val);
    for (const auto& val : vals) {
      sink_value(val, graph, encoder);
    }
  } else if (type_.id() == DataTypeId::kVarchar) {
    encoder.put_string_view(StringValue::Get(val));
  } else if (type_.id() == DataTypeId::kDate) {
    encoder.put_long(val.GetValue<date_t>().to_timestamp());
  } else if (type_.id() == DataTypeId::kTimestampMs) {
    encoder.put_long(val.GetValue<timestamp_ms_t>().milli_second);
  } else if (type_.id() == DataTypeId::kInterval) {
    encoder.put_long(val.GetValue<interval_t>().millisecond());
  } else if (type_.id() == DataTypeId::kInt64) {
    encoder.put_long(val.GetValue<int64_t>());
  } else if (type_.id() == DataTypeId::kInt32) {
    encoder.put_int(val.GetValue<int32_t>());
  } else if (type_.id() == DataTypeId::kUInt32) {
    encoder.put_uint(val.GetValue<uint32_t>());
  } else if (type_.id() == DataTypeId::kUInt64) {
    encoder.put_long(static_cast<int64_t>(val.GetValue<uint64_t>()));
  } else if (type_.id() == DataTypeId::kFloat) {
    encoder.put_float(static_cast<float>(val.GetValue<float>()));
  } else if (type_.id() == DataTypeId::kDouble) {
    int64_t long_value;
    auto double_value = val.GetValue<double>();
    std::memcpy(&long_value, &double_value, sizeof(long_value));
    encoder.put_long(long_value);
  } else if (type_.id() == DataTypeId::kBoolean) {
    encoder.put_byte((val.GetValue<bool>()) ? static_cast<uint8_t>(1)
                                            : static_cast<uint8_t>(0));
  } else if (type_.id() == DataTypeId::kVertex) {
    encoder.put_byte(val.GetValue<vertex_t>().label_);
    encoder.put_int(val.GetValue<vertex_t>().vid_);
  } else if (type_.id() == DataTypeId::kNull) {
    encoder.put_string_view("");
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink not support for " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
}

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
        date_val.to_num_days()));
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

results::CollectiveResults Sink::sink(const Context& ctx,
                                      const StorageReadInterface& graph) {
  size_t row_num = ctx.row_num();
  VLOG(10) << "Sink row num: " << row_num;
  results::CollectiveResults results;
  for (size_t i = 0; i < row_num; ++i) {
    auto result = results.add_results();
    for (size_t j : ctx.tag_ids) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto column = result->mutable_record()->add_columns();
      auto val = col->get_elem(i);
      sink_value(val, graph, j, column);
    }
  }
  return results;
}

template <typename T>
inline void wrap_primitive_type_array(
    const T* data, size_t size, const std::vector<bool>* validity_bitmap,
    std::shared_ptr<IArrowArrayBuilder> builder) {
  auto casted_builder =
      std::static_pointer_cast<ArrowArrayWrapBuilder<T>>(builder);
  if (!casted_builder) {
    THROW_NOT_SUPPORTED_EXCEPTION("Builder mismatch for type: expected " +
                                  std::string(typeid(T).name()) +
                                  " but got builder type");
  }
  if (validity_bitmap) {
    casted_builder->Wrap(data, size, validity_bitmap);
  } else {
    casted_builder->Wrap(data, size);
  }
}

#define VISIT_AND_WRAP_VALUE_COLUMN(TYPE, TYPE_ENUM)                          \
  case DataTypeId::TYPE_ENUM: {                                               \
    if (col->is_optional()) {                                                 \
      auto casted = std::static_pointer_cast<OptionalValueColumn<TYPE>>(col); \
      wrap_primitive_type_array<TYPE>(casted->data().data(), casted->size(),  \
                                      &casted->validity_bitmap(),             \
                                      array_builder);                         \
    } else {                                                                  \
      auto casted = std::static_pointer_cast<ValueColumn<TYPE>>(col);         \
      wrap_primitive_type_array<TYPE>(casted->data().data(), casted->size(),  \
                                      nullptr, array_builder);                \
    }                                                                         \
    break;                                                                    \
  }

template <typename T>
inline void append_to_builder(const std::vector<T>& data,
                              const std::vector<bool>* validity_bitmap,
                              arrow::ArrayBuilder* builder) {
  auto casted_builder =
      static_cast<typename TypeConverter<T>::ArrowBuilderType*>(builder);
  if (!casted_builder) {
    THROW_NOT_SUPPORTED_EXCEPTION("Builder mismatch for type: expected " +
                                  std::string(typeid(T).name()) +
                                  " but got builder type");
  }
  THROW_IF_ARROW_NOT_OK(casted_builder->Reserve(data.size()));
  if (validity_bitmap) {
    for (size_t i = 0; i < data.size(); ++i) {
      if ((*validity_bitmap)[i]) {
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

#define VISIT_AND_APPEND_VALUE_COLUMN(TYPE, TYPE_ENUM)                        \
  case DataTypeId::TYPE_ENUM: {                                               \
    if (col->is_optional()) {                                                 \
      auto casted = std::static_pointer_cast<OptionalValueColumn<TYPE>>(col); \
      append_to_builder<TYPE>(casted->data(), &casted->validity_bitmap(),     \
                              array_builder);                                 \
    } else {                                                                  \
      auto casted = std::static_pointer_cast<ValueColumn<TYPE>>(col);         \
      append_to_builder<TYPE>(casted->data(), nullptr, array_builder);        \
    }                                                                         \
    break;                                                                    \
  }

// For String column, we could not Use UnsafeAppend directly
#define VISIT_AND_APPEND_STRING_VALUE_COLUMN(TYPE, TYPE_ENUM)                 \
  case DataTypeId::TYPE_ENUM: {                                               \
    if (col->is_optional()) {                                                 \
      auto str_col = static_pointer_cast<OptionalValueColumn<TYPE>>(col);     \
      auto str_builder =                                                      \
          static_cast<TypeConverter<TYPE>::ArrowBuilderType*>(array_builder); \
      for (size_t i = 0; i < str_col->size(); ++i) {                          \
        if (str_col->has_value(i)) {                                          \
          THROW_IF_ARROW_NOT_OK(str_builder->Append(str_col->get_value(i)));  \
        } else {                                                              \
          THROW_IF_ARROW_NOT_OK(str_builder->AppendNull());                   \
        }                                                                     \
      }                                                                       \
    } else {                                                                  \
      auto str_col = static_pointer_cast<ValueColumn<TYPE>>(col);             \
      auto str_builder =                                                      \
          static_cast<TypeConverter<TYPE>::ArrowBuilderType*>(array_builder); \
      THROW_IF_ARROW_NOT_OK(str_builder->AppendValues(str_col->data()));      \
    }                                                                         \
    break;                                                                    \
  }

#define VISIT_AND_APPEND_LIST_VALUE_COLUMN(TYPE_ENUM)                    \
  case DataTypeId::TYPE_ENUM: {                                          \
    auto list_col = std::static_pointer_cast<ListColumn>(col);           \
    auto list_builder = static_cast<arrow::ListBuilder*>(array_builder); \
    auto child_builder = list_builder->value_builder();                  \
    for (size_t i = 0; i < list_col->size(); ++i) {                      \
      auto val = list_col->get_elem(i);                                  \
      THROW_IF_ARROW_NOT_OK(list_builder->Append());                     \
      const auto& vals = ListValue::GetChildren(val);                    \
      for (size_t j = 0; j < vals.size(); ++j) {                         \
        append_rt_any_to_builder(graph, vals[j], child_builder);         \
      }                                                                  \
    }                                                                    \
    break;                                                               \
  }

#define VISIT_AND_APPEND_STRUCT_VALUE_COLUMN(TYPE_ENUM)                       \
  case DataTypeId::TYPE_ENUM: {                                               \
    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(array_builder); \
    assert(struct_builder);                                                   \
    auto struct_col = std::dynamic_pointer_cast<StructColumn>(col);           \
    for (size_t i = 0; i < struct_col->size(); ++i) {                         \
      if (struct_col->has_value(i)) {                                         \
        THROW_IF_ARROW_NOT_OK(struct_builder->Append());                      \
        auto val = struct_col->get_elem(i);                                   \
        const auto& childs = StructValue::GetChildren(val);                   \
        for (size_t j = 0; j < childs.size(); ++j) {                          \
          auto child_builder = struct_builder->child_builder(j);              \
          if (childs[j].IsNull()) {                                           \
            THROW_IF_ARROW_NOT_OK(child_builder->AppendNull());               \
            continue;                                                         \
          }                                                                   \
          append_rt_any_to_builder(graph, childs[j], child_builder.get());    \
        }                                                                     \
      } else {                                                                \
        THROW_IF_ARROW_NOT_OK(struct_builder->AppendNull());                  \
        for (size_t j = 0; j < struct_builder->num_fields(); ++j) {           \
          auto child_builder = struct_builder->child_builder(j);              \
          THROW_IF_ARROW_NOT_OK(child_builder->AppendNull());                 \
        }                                                                     \
      }                                                                       \
    }                                                                         \
    break;                                                                    \
  }

std::shared_ptr<arrow::Array> visit_and_build_value_column(
    const StorageReadInterface& graph,
    const std::shared_ptr<IContextColumn>& col,
    arrow::ArrayBuilder* array_builder) {
  auto ele_type = col->elem_type();
  switch (ele_type.id()) {
    VISIT_AND_APPEND_VALUE_COLUMN(bool, kBoolean);
    VISIT_AND_APPEND_STRING_VALUE_COLUMN(std::string, kVarchar);
    VISIT_AND_APPEND_VALUE_COLUMN(Date, kDate);
    VISIT_AND_APPEND_VALUE_COLUMN(DateTime, kTimestampMs);
    VISIT_AND_APPEND_VALUE_COLUMN(Interval, kInterval);

    VISIT_AND_APPEND_LIST_VALUE_COLUMN(kList);
    VISIT_AND_APPEND_STRUCT_VALUE_COLUMN(kStruct);
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
  auto path_col = std::dynamic_pointer_cast<IPathColumn>(col);
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

QueryResult Sink::sink_neug_serial(const Context& ctx,
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

void Sink::sink(const Context& ctx, const StorageReadInterface& graph,
                Encoder& output) {
  results::CollectiveResults results = sink(ctx, graph);
  auto res = results.SerializeAsString();
  output.put_bytes(res.data(), res.size());
}

void Sink::sink_encoder(const Context& ctx, const StorageReadInterface& graph,
                        Encoder& encoder) {
  size_t row_num = ctx.row_num();
  for (size_t i = 0; i < row_num; ++i) {
    for (size_t j : ctx.tag_ids) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }

      auto val = col->get_elem(i);
      sink_value(val, graph, encoder);
    }
  }
}

void Sink::sink_beta(const Context& ctx, const StorageReadInterface& graph,
                     Encoder& output) {
  size_t row_num = ctx.row_num();
  std::stringstream ss;

  for (size_t i = 0; i < row_num; ++i) {
    for (size_t j : ctx.tag_ids) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto val = col->get_elem(i);
      ss << val.to_string() << "|";
    }
    ss << std::endl;
  }
  ss << "========================================================="
     << std::endl;
  // std::cout << ss.str();
  auto res = ss.str();
  output.put_bytes(res.data(), res.size());
}

}  // namespace execution
}  // namespace neug