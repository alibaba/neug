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

namespace gs {
namespace runtime {

static void sink_any(const Property& any, common::Value* value) {
  if (any.type() == DataTypeId::BIGINT) {
    value->set_i64(any.as_int64());
  } else if (any.type() == DataTypeId::VARCHAR) {
    auto str = any.as_string_view();
    value->set_str(str.data(), str.size());
  } else if (any.type() == DataTypeId::DATE) {
    value->set_u32(any.as_date().to_u32());
  } else if (any.type() == DataTypeId::INTEGER) {
    value->set_i32(any.as_int32());
  } else if (any.type() == DataTypeId::UINTEGER) {
    value->set_u32(any.as_uint32());
  } else if (any.type() == DataTypeId::DOUBLE) {
    value->set_f64(any.as_double());
  } else if (any.type() == DataTypeId::BOOLEAN) {
    value->set_boolean(any.as_bool());
  } else if (any.type() == DataTypeId::EMPTY) {
    value->mutable_none();
  } else if (any.type() == DataTypeId::DATE) {
    value->mutable_date()->set_item(any.as_date().to_num_days());
  } else if (any.type() == DataTypeId::TIMESTAMP_MS) {
    value->mutable_timestamp()->set_item(any.as_datetime().milli_second);
  } else if (any.type() == DataTypeId::UBIGINT) {
    value->set_u64(any.as_uint64());
  } else if (any.type() == DataTypeId::INTERVAL) {
    auto interval_str = any.as_interval().to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else if (any.type() == DataTypeId::FLOAT) {
    value->set_f32(any.as_float());
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink_any not support for " +
                                  std::to_string(any.type()) +
                                  " value: " + any.to_string());
  }
}

static void sink_property(const Property& prop, common::Value* value) {
  if (prop.type() == DataTypeId::INTEGER) {
    value->set_i32(prop.as_int32());
  } else if (prop.type() == DataTypeId::UINTEGER) {
    value->set_u32(prop.as_uint32());
  } else if (prop.type() == DataTypeId::VARCHAR) {
    auto str = prop.as_string_view();
    value->set_str(str.data(), str.size());
  } else if (prop.type() == DataTypeId::BIGINT) {
    value->set_i64(prop.as_int64());
  } else if (prop.type() == DataTypeId::UBIGINT) {
    value->set_u64(prop.as_uint64());
  } else if (prop.type() == DataTypeId::DOUBLE) {
    value->set_f64(prop.as_double());
  } else if (prop.type() == DataTypeId::FLOAT) {
    value->set_f32(prop.as_float());
  } else if (prop.type() == DataTypeId::DATE) {
    value->set_u32(prop.as_date().to_u32());
  } else if (prop.type() == DataTypeId::EMPTY) {
    value->mutable_none();
  } else if (prop.type() == DataTypeId::TIMESTAMP_MS) {
    value->mutable_timestamp()->set_item(prop.as_datetime().milli_second);
  } else if (prop.type() == DataTypeId::INTERVAL) {
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

static void sink_entry(const RTAny& rt_val, const StorageReadInterface& graph,
                       results::Entry* entry) {
  auto type_ = rt_val.type();
  if (type_.id() == DataTypeId::LIST) {
    auto collection = entry->mutable_collection();
    for (size_t i = 0; i < rt_val.value().list.size(); ++i) {
      auto val = rt_val.value().list.get(i);
      // convert each element in the list to the target entry by recursive
      // invocation
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_.id() == DataTypeId::STRUCT) {
    auto collection = entry->mutable_collection();
    for (size_t i = 0; i < rt_val.value().t.size(); ++i) {
      auto val = rt_val.value().t.get(i);
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_.id() == DataTypeId::SET) {
    auto collection = entry->mutable_collection();
    for (auto& val : rt_val.value().set.values()) {
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_.id() == DataTypeId::VERTEX) {
    auto ele = entry->mutable_element();
    if (!sink_vertex(graph, rt_val.value().vertex, ele->mutable_vertex())) {
      ele->mutable_object()->mutable_none();
    }
  } else if (type_.id() == DataTypeId::EDGE) {
    sink_edge(graph, rt_val.value().edge,
              entry->mutable_element()->mutable_edge());
  } else if (type_.id() == DataTypeId::PATH) {
    auto mutable_path = entry->mutable_element()->mutable_graph_path();
    auto path_nodes = rt_val.as_path().nodes();
    auto edge_labels = rt_val.as_path().edge_labels();
    auto edges = rt_val.as_path().relationships();

    assert(edge_labels.size() + 1 == path_nodes.size());
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
          graph.schema().get_edge_label_name(edge_labels[i]));
      edge->set_id(encode_unique_edge_id(edge_labels[i],
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
    rt_val.sink_impl(entry->mutable_element()->mutable_object());
  }
}

static void sink_rt_any(const RTAny& val, const StorageReadInterface& graph,
                        int id, results::Column* col) {
  col->mutable_name_or_id()->set_id(id);
  sink_entry(val, graph, col->mutable_entry());
}

static void sink_rt_any(const RTAny& val, const StorageReadInterface& graph,
                        Encoder& encoder) {
  auto type_ = val.type();
  if (type_.id() == DataTypeId::LIST) {
    encoder.put_int(val.value().list.size());
    for (size_t i = 0; i < val.value().list.size(); ++i) {
      sink_rt_any(val.value().list.get(i), graph, encoder);
    }
  } else if (type_.id() == DataTypeId::STRUCT) {
    for (size_t i = 0; i < val.value().t.size(); ++i) {
      sink_rt_any(val.value().t.get(i), graph, encoder);
    }
  } else if (type_.id() == DataTypeId::VARCHAR) {
    encoder.put_string_view(val.value().str_val);
  } else if (type_.id() == DataTypeId::DATE) {
    encoder.put_long(val.value().date_val.to_timestamp());
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    encoder.put_long(val.value().dt_val.milli_second);
  } else if (type_.id() == DataTypeId::INTERVAL) {
    encoder.put_long(val.value().interval_val.millisecond());
  } else if (type_.id() == DataTypeId::BIGINT) {
    encoder.put_long(val.value().i64_val);
  } else if (type_.id() == DataTypeId::INTEGER) {
    encoder.put_int(val.value().i32_val);
  } else if (type_.id() == DataTypeId::UINTEGER) {
    encoder.put_uint(val.value().u32_val);
  } else if (type_.id() == DataTypeId::FLOAT) {
    encoder.put_float(static_cast<float>(val.value().f32_val));
  } else if (type_.id() == DataTypeId::DOUBLE) {
    int64_t long_value;
    std::memcpy(&long_value, &val.value().f64_val, sizeof(long_value));
    encoder.put_long(long_value);
  } else if (type_.id() == DataTypeId::BOOLEAN) {
    encoder.put_byte(val.value().b_val ? static_cast<uint8_t>(1)
                                       : static_cast<uint8_t>(0));
  } else if (type_.id() == DataTypeId::SET) {
    encoder.put_int(val.value().set.size());
    auto value = val.value().set.values();
    for (const auto& val : value) {
      sink_rt_any(val, graph, encoder);
    }
  } else if (type_.id() == DataTypeId::VERTEX) {
    encoder.put_byte(val.value().vertex.label_);
    encoder.put_int(val.value().vertex.vid_);
  } else if (type_.id() == DataTypeId::SQLNULL) {
    encoder.put_string_view("");
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink not support for " +
                                  static_cast<int>(type_.id()));
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
      sink_rt_any(val, graph, j, column);
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
      sink_rt_any(val, graph, encoder);
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

}  // namespace runtime
}  // namespace gs