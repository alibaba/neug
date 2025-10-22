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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SINK_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SINK_H_

#include <limits>
#include <string>

#include "neug/execution/common/context.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {
namespace runtime {

[[maybe_unused]] static void sink_any(const Any& any, common::Value* value) {
  if (any.type == PropertyType::Int64()) {
    value->set_i64(any.AsInt64());
  } else if (any.type == PropertyType::StringView()) {
    auto str = any.AsStringView();
    value->set_str(str.data(), str.size());
  } else if (any.type == PropertyType::Date()) {
    value->set_u32(any.AsDate().to_u32());
  } else if (any.type == PropertyType::Int32()) {
    value->set_i32(any.AsInt32());
  } else if (any.type == PropertyType::UInt32()) {
    value->set_u32(any.AsUInt32());
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.AsDouble());
  } else if (any.type == PropertyType::Bool()) {
    value->set_boolean(any.AsBool());
  } else if (any.type == PropertyType::Empty()) {
    value->mutable_none();
  } else if (any.type == PropertyType::Date()) {
    value->mutable_date()->set_item(any.AsDate().to_num_days());
  } else if (any.type == PropertyType::DateTime()) {
    value->mutable_timestamp()->set_item(any.AsDateTime().milli_second);
  } else if (any.type == PropertyType::UInt64()) {
    value->set_u64(any.AsUInt64());
  } else if (any.type == PropertyType::Interval()) {
    auto interval_str = any.AsInterval().to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else if (any.type == PropertyType::Float()) {
    value->set_f32(any.AsFloat());
  } else if (any.type == PropertyType::Timestamp()) {
    value->mutable_timestamp()->set_item(any.AsTimeStamp().milli_second);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink_any not support for " +
                                  any.type.ToString() +
                                  " value: " + any.to_string());
  }
}

[[maybe_unused]] static void sink_property(const Prop& prop,
                                           common::Value* value) {
  if (prop.type() == PropType::kInt32) {
    value->set_i32(prop.as_int32());
  } else if (prop.type() == PropType::kUInt32) {
    value->set_u32(prop.as_uint32());
  } else if (prop.type() == PropType::kString) {
    auto str = prop.as_string();
    value->set_str(str.data(), str.size());
  } else if (prop.type() == PropType::kInt64) {
    value->set_i64(prop.as_int64());
  } else if (prop.type() == PropType::kUInt64) {
    value->set_u64(prop.as_uint64());
  } else if (prop.type() == PropType::kDouble) {
    value->set_f64(prop.as_double());
  } else if (prop.type() == PropType::kFloat) {
    value->set_f32(prop.as_float());
  } else if (prop.type() == PropType::kTimestamp) {
    value->mutable_timestamp()->set_item(prop.as_timestamp().milli_second);
  } else if (prop.type() == PropType::kDate) {
    value->set_u32(prop.as_date().to_u32());
  } else if (prop.type() == PropType::kEmpty) {
    value->mutable_none();
  } else if (prop.type() == PropType::kDateTime) {
    value->mutable_timestamp()->set_item(prop.as_date_time().milli_second);
  } else if (prop.type() == PropType::kInterval) {
    auto interval_str = prop.as_interval().to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else {
    LOG(FATAL) << "property value: " << prop.to_string()
               << ", type = " << static_cast<int>(prop.type());
  }
}

template <typename GraphInterface>
bool sink_vertex(const GraphInterface& graph, const VertexRecord& vertex,
                 results::Vertex* v) {
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

template <typename GraphInterface>
bool sink_edge(const GraphInterface& graph, const EdgeRecord& edge,
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
  auto& prop_names = graph.schema().get_edge_property_names(
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

template <typename GraphInterface>
void sink_entry(const RTAny& rt_val, const GraphInterface& graph,
                results::Entry* entry) {
  auto type_ = rt_val.type();
  if (type_ == RTAnyType::kList) {
    auto collection = entry->mutable_collection();
    for (size_t i = 0; i < rt_val.value().list.size(); ++i) {
      auto val = rt_val.value().list.get(i);
      // convert each element in the list to the target entry by recursive
      // invocation
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_ == RTAnyType::kTuple) {
    auto collection = entry->mutable_collection();
    for (size_t i = 0; i < rt_val.value().t.size(); ++i) {
      auto val = rt_val.value().t.get(i);
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_ == RTAnyType::kSet) {
    auto collection = entry->mutable_collection();
    for (auto& val : rt_val.value().set.values()) {
      sink_entry(val, graph, collection->add_collection());
    }
  } else if (type_ == RTAnyType::kVertex) {
    auto ele = entry->mutable_element();
    if (!sink_vertex(graph, rt_val.value().vertex, ele->mutable_vertex())) {
      ele->mutable_object()->mutable_none();
    }
  } else if (type_ == RTAnyType::kMap) {
    auto mp = entry->mutable_map();
    auto [keys_ptr, vals_ptr] = rt_val.value().map.key_vals();
    auto& keys = keys_ptr;
    auto& vals = vals_ptr;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (vals[i].is_null()) {
        continue;
      }
      auto ret = mp->add_key_values();
      keys[i].sink_impl(ret->mutable_key());
      if (vals[i].type() == RTAnyType::kVertex) {
        auto ele = ret->mutable_value()->mutable_element();
        if (!sink_vertex(graph, vals[i].as_vertex(), ele->mutable_vertex())) {
          ele->mutable_object()->mutable_none();
        }
      } else {
        vals[i].sink_impl(
            ret->mutable_value()->mutable_element()->mutable_object());
      }
    }
  } else if (type_ == RTAnyType::kRelation) {
    auto ele = entry->mutable_element()->mutable_edge();

    auto val = rt_val.value().relation;
    auto label = val.label;
    auto src = val.src;
    auto dst = val.dst;
    ele->mutable_src_label()->set_name(
        graph.schema().get_vertex_label_name(label.src_label));
    ele->mutable_dst_label()->set_name(
        graph.schema().get_vertex_label_name(label.dst_label));
    auto edge_label = generate_edge_label_id(label.src_label, label.dst_label,
                                             label.edge_label);
    ele->mutable_label()->set_name(
        graph.schema().get_edge_label_name(label.edge_label));
    ele->set_src_id(encode_unique_vertex_id(label.src_label, src));
    ele->set_dst_id(encode_unique_vertex_id(label.dst_label, dst));
    ele->set_id(encode_unique_edge_id(edge_label, src, dst));

  } else if (type_ == RTAnyType::kEdge) {
    sink_edge(graph, rt_val.value().edge,
              entry->mutable_element()->mutable_edge());
  } else if (type_ == RTAnyType::kPath) {
    auto mutable_path = entry->mutable_element()->mutable_graph_path();
    auto path_nodes = rt_val.as_path().nodes();
    auto edge_labels = rt_val.as_path().edge_labels();
    // same label for all edges
    if (edge_labels.size() == 1) {
      for (size_t i = 0; i + 2 < path_nodes.size(); ++i) {
        edge_labels.emplace_back(edge_labels[0]);
      }
    }
    assert(edge_labels.size() + 1 == path_nodes.size());
    size_t len = path_nodes.size();
    for (size_t i = 0; i + 1 < len; ++i) {
      auto vertex_in_path = mutable_path->add_path();

      auto node = vertex_in_path->mutable_vertex();
      // node->mutable_label()->set_id(path_nodes[i].label());
      node->mutable_label()->set_name(
          graph.schema().get_vertex_label_name(path_nodes[i].label()));
      node->set_id(
          encode_unique_vertex_id(path_nodes[i].label(), path_nodes[i].vid()));
      auto edge_in_path = mutable_path->add_path();

      auto edge = edge_in_path->mutable_edge();
      edge->mutable_src_label()->set_name(
          graph.schema().get_vertex_label_name(path_nodes[i].label()));
      edge->mutable_dst_label()->set_name(
          graph.schema().get_vertex_label_name(path_nodes[i + 1].label()));
      edge->mutable_label()->set_name(
          graph.schema().get_edge_label_name(edge_labels[i]));
      edge->set_id(encode_unique_edge_id(edge_labels[i], path_nodes[i].vid(),
                                         path_nodes[i + 1].vid()));
      edge->set_src_id(
          encode_unique_vertex_id(path_nodes[i].label(), path_nodes[i].vid()));
      edge->set_dst_id(encode_unique_vertex_id(path_nodes[i + 1].label(),
                                               path_nodes[i + 1].vid()));
    }
    auto vertex_in_path = mutable_path->add_path();
    auto node = vertex_in_path->mutable_vertex();
    // node->mutable_label()->set_id(path_nodes[len - 1].label());
    node->mutable_label()->set_name(
        graph.schema().get_vertex_label_name(path_nodes[len - 1].label()));
    node->set_id(encode_unique_vertex_id(path_nodes[len - 1].label(),
                                         path_nodes[len - 1].vid()));
  } else {
    rt_val.sink_impl(entry->mutable_element()->mutable_object());
  }
}

template <typename GraphInterface>
void sink_rt_any(const RTAny& val, const GraphInterface& graph, int id,
                 results::Column* col) {
  col->mutable_name_or_id()->set_id(id);
  sink_entry(val, graph, col->mutable_entry());
}

template <typename GraphInterface>
void sink_rt_any(const RTAny& val, const GraphInterface& graph,
                 Encoder& encoder) {
  auto type_ = val.type();
  if (type_ == RTAnyType::kList) {
    encoder.put_int(val.value().list.size());
    for (size_t i = 0; i < val.value().list.size(); ++i) {
      sink_rt_any(val.value().list.get(i), graph, encoder);
    }
  } else if (type_ == RTAnyType::kTuple) {
    for (size_t i = 0; i < val.value().t.size(); ++i) {
      sink_rt_any(val.value().t.get(i), graph, encoder);
    }
  } else if (type_ == RTAnyType::kStringValue) {
    encoder.put_string_view(val.value().str_val);
  } else if (type_ == RTAnyType::kDate) {
    encoder.put_long(val.value().date_val.to_timestamp());
  } else if (type_ == RTAnyType::kDateTime) {
    encoder.put_long(val.value().dt_val.milli_second);
  } else if (type_ == RTAnyType::kTimestamp) {
    encoder.put_long(val.value().ts_val.milli_second);
  } else if (type_ == RTAnyType::kInterval) {
    encoder.put_long(val.value().interval_val.millisecond());
  } else if (type_ == RTAnyType::kI64Value) {
    encoder.put_long(val.value().i64_val);
  } else if (type_ == RTAnyType::kI32Value) {
    encoder.put_int(val.value().i32_val);
  } else if (type_ == RTAnyType::kU32Value) {
    encoder.put_uint(val.value().u32_val);
  } else if (type_ == RTAnyType::kF32Value) {
    encoder.put_float(static_cast<float>(val.value().f32_val));
  } else if (type_ == RTAnyType::kF64Value) {
    int64_t long_value;
    std::memcpy(&long_value, &val.value().f64_val, sizeof(long_value));
    encoder.put_long(long_value);
  } else if (type_ == RTAnyType::kBoolValue) {
    encoder.put_byte(val.value().b_val ? static_cast<uint8_t>(1)
                                       : static_cast<uint8_t>(0));
  } else if (type_ == RTAnyType::kSet) {
    encoder.put_int(val.value().set.size());
    auto value = val.value().set.values();
    for (const auto& val : value) {
      sink_rt_any(val, graph, encoder);
    }
  } else if (type_ == RTAnyType::kVertex) {
    encoder.put_byte(val.value().vertex.label_);
    encoder.put_int(val.value().vertex.vid_);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("sink not support for " +
                                  std::string(typeid(val).name()));
  }
}

class Sink {
 public:
  template <typename GraphInterface>
  static results::CollectiveResults sink(const Context& ctx,
                                         const GraphInterface& graph) {
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

  template <typename GraphInterface>
  static void sink(const Context& ctx, const GraphInterface& graph,
                   Encoder& output) {
    results::CollectiveResults results = sink(ctx, graph);
    auto res = results.SerializeAsString();
    output.put_bytes(res.data(), res.size());
  }

  template <typename GraphInterface>
  static void sink_encoder(const Context& ctx, const GraphInterface& graph,
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

  // for debug
  template <typename GraphInterface>
  static void sink_beta(const Context& ctx, const GraphInterface& graph,
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
};

}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SINK_H_
