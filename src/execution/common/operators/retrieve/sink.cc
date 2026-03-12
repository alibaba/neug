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
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"

#include "neug/storages/graph/graph_interface.h"

#include "neug/utils/property/types.h"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace neug {
namespace execution {

void append_property_to_json(const std::string& key, const Property& prop,
                             rapidjson::Value& doc,
                             rapidjson::Document::AllocatorType& allocator) {
  auto type_id = prop.type();
  switch (type_id) {
  case DataTypeId::kVarchar: {
    auto str = prop.as_string_view();
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(str.data(), str.size(), allocator),
                  allocator);
    break;
  }
  case DataTypeId::kBoolean:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_bool()), allocator);
    break;
  case DataTypeId::kInt32:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_int32()), allocator);
    break;
  case DataTypeId::kUInt32:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_uint32()), allocator);
    break;
  case DataTypeId::kInt64:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_int64()), allocator);
    break;
  case DataTypeId::kUInt64:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_uint64()), allocator);
    break;
  case DataTypeId::kFloat:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_float()), allocator);
    break;
  case DataTypeId::kDouble:
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(prop.as_double()), allocator);
    break;
  case DataTypeId::kDate: {
    auto day = prop.as_date();
    std::stringstream ss;
    ss << day.year() << "-" << std::setfill('0') << std::setw(2) << day.month()
       << "-" << std::setfill('0') << std::setw(2) << day.day();
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(ss.str().c_str(), allocator), allocator);
    break;
  }
  case DataTypeId::kTimestampMs: {
    auto milliseconds = prop.as_datetime().milli_second;
    std::time_t seconds = milliseconds / 1000;
    int ms = milliseconds % 1000;
    std::tm* tm_info = std::gmtime(&seconds);
    std::stringstream ss;
    ss << std::put_time(tm_info, "%Y-%m-%dT%H:%M:%S");
    if (ms > 0) {
      ss << "." << std::setfill('0') << std::setw(3) << ms;
    }
    ss << "Z";
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(ss.str().c_str(), allocator), allocator);
    break;
  }
  case DataTypeId::kInterval: {
    auto interval_str = prop.as_interval().to_string();
    doc.AddMember(rapidjson::Value(key.c_str(), allocator),
                  rapidjson::Value(interval_str.c_str(), allocator), allocator);
    break;
  }
  default:
    LOG(WARNING) << "append_property_to_json not support for type " +
                        std::to_string(static_cast<int>(type_id));
  }
}

std::string convert_vertex_to_json(const StorageReadInterface& graph,
                                   const VertexRecord& record) {
  if (record.label_ == std::numeric_limits<vid_t>::max() ||
      record.vid_ == std::numeric_limits<vid_t>::max()) {
    return "";
  }
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();
  auto label_name = graph.schema().get_vertex_label_name(record.label_);
  auto gid = encode_unique_vertex_id(record.label_, record.vid_);
  doc.AddMember("_ID", rapidjson::Value(gid), allocator);
  doc.AddMember("_LABEL", rapidjson::Value(label_name.c_str(), allocator),
                allocator);
  auto pk_prop = graph.GetVertexId(record.label_, record.vid_);
  auto pk_types = graph.schema().get_vertex_primary_key(record.label_);
  append_property_to_json(std::get<1>(pk_types[0]), pk_prop, doc, allocator);
  const auto& property_names =
      graph.schema().get_vertex_property_names(record.label_);
  for (size_t i = 0; i < property_names.size(); ++i) {
    auto prop = graph.GetVertexProperty(record.label_, record.vid_, i);
    append_property_to_json(property_names[i], prop, doc, allocator);
  }
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string convert_edge_to_json(const StorageReadInterface& graph,
                                 const EdgeRecord& record) {
  if (record.label.src_label == std::numeric_limits<vid_t>::max() ||
      record.label.dst_label == std::numeric_limits<vid_t>::max() ||
      record.label.edge_label == std::numeric_limits<vid_t>::max() ||
      record.src == std::numeric_limits<vid_t>::max() ||
      record.dst == std::numeric_limits<vid_t>::max()) {
    return "";
  }
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();
  auto edge_label_name =
      graph.schema().get_edge_label_name(record.label.edge_label);

  auto edge_id = encode_unique_edge_id(
      generate_edge_label_id(record.label.src_label, record.label.dst_label,
                             record.label.edge_label),
      record.src, record.dst);
  doc.AddMember("_ID", rapidjson::Value(edge_id), allocator);
  doc.AddMember("_LABEL", rapidjson::Value(edge_label_name.c_str(), allocator),
                allocator);

  auto src_gid = encode_unique_vertex_id(record.label.src_label, record.src);
  auto dst_gid = encode_unique_vertex_id(record.label.dst_label, record.dst);
  doc.AddMember("_SRC_ID", rapidjson::Value(src_gid), allocator);
  doc.AddMember("_DST_ID", rapidjson::Value(dst_gid), allocator);
  auto property_types = graph.schema().get_edge_properties(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  auto property_names = graph.schema().get_edge_property_names(
      record.label.src_label, record.label.dst_label, record.label.edge_label);
  for (size_t i = 0; i < property_types.size(); ++i) {
    auto prop =
        graph
            .GetEdgeDataAccessor(record.label.src_label, record.label.dst_label,
                                 record.label.edge_label, i)
            .get_data_from_ptr(record.prop);
    append_property_to_json(property_names[i], prop, doc, allocator);
  }
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string convert_path_to_json(const StorageReadInterface& graph,
                                 const Path& path) {
  if (path.is_null()) {
    return "";
  }
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  // nodes
  rapidjson::Value nodes(rapidjson::kArrayType);
  for (const auto& node : path.nodes()) {
    std::string node_json_str = convert_vertex_to_json(graph, node);

    rapidjson::Document node_doc;
    node_doc.Parse(node_json_str.c_str());

    rapidjson::Value node_val;
    node_val.CopyFrom(node_doc, allocator);
    nodes.PushBack(node_val, allocator);
  }
  doc.AddMember("nodes", nodes, allocator);

  // edges
  rapidjson::Value edges(rapidjson::kArrayType);
  for (const auto& edge : path.relationships()) {
    std::string edge_json_str = convert_edge_to_json(graph, edge);

    rapidjson::Document edge_doc;
    edge_doc.Parse(edge_json_str.c_str());

    rapidjson::Value edge_val;
    edge_val.CopyFrom(edge_doc, allocator);
    edges.PushBack(edge_val, allocator);
  }
  doc.AddMember("rels", edges, allocator);

  doc.AddMember("length", path.length(), allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

static std::string BoolVectorToBitmap(const std::vector<bool>& flags) {
  size_t num_bytes = (flags.size() + 7) / 8;
  std::string bitmap(num_bytes, 0x00);
  for (size_t i = 0; i < flags.size(); ++i) {
    if (flags[i]) {
      bitmap[i / 8] |= (1 << (i % 8));
    }
  }
  return bitmap;
}

template <typename T, typename COL_T>
static void add_primitive_column(const ValueColumn<T>& col,
                                 const StorageReadInterface& graph,
                                 COL_T* column) {
  column->mutable_values()->Reserve(col.size());
  for (size_t i = 0; i < col.size(); ++i) {
    column->add_values(col.get_value(i));
  }
  if (col.is_optional()) {
    auto bitmap = BoolVectorToBitmap(col.validity_bitmap());
    column->set_validity(bitmap);
  }
}

static void add_column(const std::shared_ptr<IContextColumn>& col,
                       const StorageReadInterface& graph, neug::Array* column) {
  switch (col->elem_type().id()) {
  case DataTypeId::kBoolean: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<bool>>(col);
    add_primitive_column<bool, neug::BoolArray>(*casted, graph,
                                                column->mutable_bool_array());
    break;
  }
  case DataTypeId::kInt32: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<int32_t>>(col);
    add_primitive_column<int32_t, neug::Int32Array>(
        *casted, graph, column->mutable_int32_array());
    break;
  }

  case DataTypeId::kUInt32: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(col);
    add_primitive_column<uint32_t, neug::UInt32Array>(
        *casted, graph, column->mutable_uint32_array());
    break;
  }

  case DataTypeId::kInt64: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<int64_t>>(col);
    add_primitive_column<int64_t, neug::Int64Array>(
        *casted, graph, column->mutable_int64_array());
    break;
  }

  case DataTypeId::kUInt64: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(col);
    add_primitive_column<uint64_t, neug::UInt64Array>(
        *casted, graph, column->mutable_uint64_array());
    break;
  }

  case DataTypeId::kFloat: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<float>>(col);
    add_primitive_column<float, neug::FloatArray>(
        *casted, graph, column->mutable_float_array());
    break;
  }
  case DataTypeId::kDouble: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<double>>(col);
    add_primitive_column<double, neug::DoubleArray>(
        *casted, graph, column->mutable_double_array());
    break;
  }
  case DataTypeId::kVarchar: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<std::string>>(col);
    auto varchar_col = column->mutable_string_array();
    add_primitive_column<std::string, neug::StringArray>(*casted, graph,
                                                         varchar_col);
    break;
  }
  case DataTypeId::kDate: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<Date>>(col);
    auto date_col = column->mutable_date_array();
    date_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      date_col->add_values(casted->get_value(i).to_timestamp());
    }
    if (casted->is_optional()) {
      auto bitmap = BoolVectorToBitmap(casted->validity_bitmap());
      date_col->set_validity(bitmap);
    }
    break;
  }
  case DataTypeId::kTimestampMs: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<DateTime>>(col);
    auto ts_col = column->mutable_timestamp_array();
    ts_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      ts_col->add_values(casted->get_value(i).milli_second);
    }
    if (casted->is_optional()) {
      auto bitmap = BoolVectorToBitmap(casted->validity_bitmap());
      ts_col->set_validity(bitmap);
    }
    break;
  }

  case DataTypeId::kInterval: {
    auto casted = std::dynamic_pointer_cast<ValueColumn<Interval>>(col);
    auto interval_col = column->mutable_interval_array();
    interval_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      interval_col->add_values(casted->get_value(i).to_string());
    }
    if (casted->is_optional()) {
      auto bitmap = BoolVectorToBitmap(casted->validity_bitmap());
      interval_col->set_validity(bitmap);
    }
    break;
  }
  case DataTypeId::kList: {
    auto casted_raw = std::dynamic_pointer_cast<ListColumn>(col);
    auto casted = std::dynamic_pointer_cast<ListColumn>(casted_raw->reorder());

    auto list_col = column->mutable_list_array();
    const auto& children = casted->data_column();
    add_column(children, graph, list_col->mutable_elements());
    list_col->mutable_offsets()->Reserve(casted->size() + 1);
    const auto& items = casted->items();
    size_t current_offset = 0;
    for (size_t i = 0; i < casted->size(); ++i) {
      list_col->add_offsets(current_offset);
      current_offset += items[i].length;
    }
    if (items.empty()) {
      list_col->add_offsets(0);
    } else {
      list_col->add_offsets(current_offset);
    }

    break;
  }
  case DataTypeId::kStruct: {
    auto casted = std::dynamic_pointer_cast<StructColumn>(col);
    auto struct_col = column->mutable_struct_array();
    const auto& children = casted->children();
    struct_col->mutable_fields()->Reserve(children.size());
    for (size_t i = 0; i < children.size(); ++i) {
      auto child_field = struct_col->add_fields();
      add_column(children[i], graph, child_field);
    }
    if (casted->is_optional()) {
      auto bitmap = BoolVectorToBitmap(casted->validity_bitmap());
      struct_col->set_validity(bitmap);
    }
    break;
  }
  case DataTypeId::kVertex: {
    auto casted = std::dynamic_pointer_cast<IVertexColumn>(col);
    auto vertex_col = column->mutable_vertex_array();
    vertex_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      auto record = casted->get_vertex(i);
      vertex_col->add_values(convert_vertex_to_json(graph, record));
    }
    if (casted->is_optional()) {
      std::vector<bool> validity(casted->size());
      for (size_t i = 0; i < casted->size(); ++i) {
        validity[i] = casted->has_value(i);
      }
      auto bitmap = BoolVectorToBitmap(validity);
      vertex_col->set_validity(bitmap);
    }
    break;
  }
  case DataTypeId::kEdge: {
    auto casted = std::dynamic_pointer_cast<IEdgeColumn>(col);
    auto edge_col = column->mutable_edge_array();
    edge_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      auto record = casted->get_edge(i);
      edge_col->add_values(convert_edge_to_json(graph, record));
    }
    if (casted->is_optional()) {
      std::vector<bool> validity(casted->size());
      for (size_t i = 0; i < casted->size(); ++i) {
        validity[i] = casted->has_value(i);
      }
      auto bitmap = BoolVectorToBitmap(validity);
      edge_col->set_validity(bitmap);
    }
    break;
  }
  case DataTypeId::kPath: {
    auto casted = std::dynamic_pointer_cast<PathColumn>(col);
    auto path_col = column->mutable_path_array();
    path_col->mutable_values()->Reserve(casted->size());
    for (size_t i = 0; i < casted->size(); ++i) {
      auto path = casted->get_path(i);
      path_col->add_values(convert_path_to_json(graph, path));
    }
    if (casted->is_optional()) {
      std::vector<bool> validity(casted->size());
      for (size_t i = 0; i < casted->size(); ++i) {
        validity[i] = casted->has_value(i);
      }
      auto bitmap = BoolVectorToBitmap(validity);
      path_col->set_validity(bitmap);
    }
    break;
  }
  default: {
    LOG(ERROR) << "add_column not support for " << col->column_info();
  }
  }
}
void Sink::sink_results(const Context& ctx, const StorageReadInterface& graph,
                        neug::QueryResponse* response) {
  response->set_row_count(ctx.row_num());

  response->mutable_arrays()->Reserve(ctx.tag_ids.size());
  for (size_t i : ctx.tag_ids) {
    auto col = ctx.get(i);
    if (col == nullptr) {
      continue;
    }
    if (col->column_type() != ContextColumnType::kArrowArray) {
      add_column(col, graph, response->add_arrays());
    } else {
      auto casted = dynamic_cast<ArrowArrayContextColumn*>(col.get())
                        ->cast_to_value_column();
      add_column(casted, graph, response->add_arrays());
    }
  }
}

}  // namespace execution
}  // namespace neug