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

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/storages/graph/graph_interface.h"
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
  if (vertex.label_ >= graph.schema().vertex_label_num() ||
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