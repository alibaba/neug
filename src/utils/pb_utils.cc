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

#include "neug/utils/pb_utils.h"
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <stddef.h>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/iterator.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

std::string convert_object_to_string(const ::common::Value& value) {
  if (value.item_case() == common::Value::ItemCase::kI32) {
    return std::to_string(value.i32());
  } else if (value.item_case() == common::Value::ItemCase::kI64) {
    return std::to_string(value.i64());
  } else if (value.item_case() == common::Value::ItemCase::kF64) {
    return std::to_string(value.f64());
  } else if (value.item_case() == common::Value::ItemCase::kStr) {
    return value.str();
  } else if (value.item_case() == common::Value::ItemCase::kBoolean) {
    return value.boolean() ? "True" : "False";
  } else if (value.item_case() == common::Value::ItemCase::kStrArray) {
    std::vector<std::string> str_array;
    for (const auto& s : value.str_array().item()) {
      str_array.emplace_back(s);
    }
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < str_array.size(); ++i) {
      oss << "\"" << str_array[i] << "\"";
      if (i != str_array.size() - 1) {
        oss << ", ";
      }
    }
    oss << "]";
    return oss.str();
  } else if (value.item_case() == common::Value::ItemCase::kI32Array) {
    std::vector<int32_t> i32_array;
    for (const auto& i : value.i32_array().item()) {
      i32_array.emplace_back(i);
    }
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < i32_array.size(); ++i) {
      oss << i32_array[i];
      if (i != i32_array.size() - 1) {
        oss << ", ";
      }
    }
    oss << "]";
    return oss.str();
  } else if (value.item_case() == common::Value::ItemCase::kI64Array) {
    std::vector<int64_t> i64_array;
    for (const auto& i : value.i64_array().item()) {
      i64_array.emplace_back(i);
    }
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < i64_array.size(); ++i) {
      oss << i64_array[i];
      if (i != i64_array.size() - 1) {
        oss << ", ";
      }
    }
    oss << "]";
    return oss.str();
  } else if (value.item_case() == common::Value::ItemCase::kF64Array) {
    std::vector<double> f64_array;
    for (const auto& i : value.f64_array().item()) {
      f64_array.emplace_back(i);
    }
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < f64_array.size(); ++i) {
      oss << f64_array[i];
      if (i != f64_array.size() - 1) {
        oss << ", ";
      }
    }
    oss << "]";
    return oss.str();
  } else if (value.item_case() == common::Value::ItemCase::kNone) {
    return "None";
  } else {
    return "";
  }
}

void add_entry_value_to_table(rapidjson::Value& table, size_t index,
                              std::string& field_name,
                              const results::Entry& entry,
                              rapidjson::Document::AllocatorType& allocator) {
  std::string value;
  if (entry.has_element()) {
    const auto& element = entry.element();
    if (element.has_vertex()) {
      google::protobuf::util::MessageToJsonString(element.vertex(), &value);
    } else if (element.has_edge()) {
      google::protobuf::util::MessageToJsonString(element.edge(), &value);
    } else if (element.has_graph_path()) {
      google::protobuf::util::MessageToJsonString(element.graph_path(), &value);
    } else if (element.has_object()) {
      value = convert_object_to_string(element.object());
    }
  } else if (entry.has_collection()) {
    std::vector<std::string> v_collection;
    for (const auto& e : entry.collection().collection()) {
      auto item = e.element();
      if (item.has_vertex()) {
        std::string v;
        google::protobuf::util::MessageToJsonString(item.vertex(), &v);
        v_collection.emplace_back(v);
      } else if (item.has_edge()) {
        std::string v;
        google::protobuf::util::MessageToJsonString(item.edge(), &v);
        v_collection.emplace_back(v);
      } else if (item.has_graph_path()) {
        std::string v;
        google::protobuf::util::MessageToJsonString(item.graph_path(), &v);
        v_collection.emplace_back(v);
      } else if (item.has_object()) {
        v_collection.emplace_back(convert_object_to_string(item.object()));
      }
      std::ostringstream oss;
      oss << "[";
      for (size_t i = 0; i < v_collection.size(); ++i) {
        oss << v_collection[i];
        if (i != v_collection.size() - 1) {
          oss << ", ";
        }
      }
      oss << "]";
      value = oss.str();
    }
  } else if (entry.has_map()) {
  }

  rapidjson::Value key(field_name.c_str(), allocator);
  if (index < table.Size()) {
    rapidjson::Value& target_object = table[index];
    target_object.AddMember(key, value, allocator);
  } else {
    rapidjson::Value row(rapidjson::kObjectType);
    row.AddMember(key, value, allocator);
    table.PushBack(row, allocator);
  }
}

rapidjson::Value convert_properties_to_json(
    const google::protobuf::RepeatedPtrField<results::Property>& props,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value prop_obj(rapidjson::kObjectType);

  for (const auto& prop : props) {
    std::string key;
    switch (prop.key().item_case()) {
    case common::NameOrId::ItemCase::kName: {
      key = prop.key().name();
      break;
    }
    case common::NameOrId::ItemCase::kId: {
      key = std::to_string(prop.key().id());
      break;
    }
    default: {
      THROW_INTERNAL_EXCEPTION("Unknown property key type");
    }
    }
    rapidjson::Value key_val(key.c_str(), allocator);
    rapidjson::Value value_val;

    // Convert common::Value to rapidjson::Value
    if (prop.value().item_case() == common::Value::ItemCase::kI32) {
      value_val.SetInt(prop.value().i32());
    } else if (prop.value().item_case() == common::Value::ItemCase::kI64) {
      value_val.SetInt64(prop.value().i64());
    } else if (prop.value().item_case() == common::Value::ItemCase::kF64) {
      value_val.SetDouble(prop.value().f64());
    } else if (prop.value().item_case() == common::Value::ItemCase::kStr) {
      value_val.SetString(prop.value().str().c_str(), allocator);
    } else if (prop.value().item_case() == common::Value::ItemCase::kBoolean) {
      value_val.SetBool(prop.value().boolean());
    } else if (prop.value().item_case() == common::Value::ItemCase::kStrArray) {
      value_val.SetArray();
      for (const auto& s : prop.value().str_array().item()) {
        rapidjson::Value str_val(s.c_str(), allocator);
        value_val.PushBack(str_val, allocator);
      }
    } else if (prop.value().item_case() == common::Value::ItemCase::kI32Array) {
      value_val.SetArray();
      for (const auto& i : prop.value().i32_array().item()) {
        value_val.PushBack(rapidjson::Value(i), allocator);
      }
    } else if (prop.value().item_case() == common::Value::ItemCase::kI64Array) {
      value_val.SetArray();
      for (const auto& i : prop.value().i64_array().item()) {
        value_val.PushBack(rapidjson::Value(i), allocator);
      }
    } else if (prop.value().item_case() == common::Value::ItemCase::kF64Array) {
      value_val.SetArray();
      for (const auto& f : prop.value().f64_array().item()) {
        value_val.PushBack(rapidjson::Value(f), allocator);
      }
    } else {
      // Default to null for unknown types
      value_val.SetNull();
    }

    prop_obj.AddMember(key_val, value_val, allocator);
  }
  return prop_obj;
}

// Helper function to convert vertex to node and add to collections
void convert_vertex_to_node(const results::Vertex& vertex,
                            rapidjson::Value& nodes,
                            std::unordered_set<int64_t>& node_ids,
                            rapidjson::Document::AllocatorType& allocator) {
  if (node_ids.find(vertex.id()) == node_ids.end()) {
    rapidjson::Value node(rapidjson::kObjectType);

    std::string id_str = std::to_string(vertex.id());
    node.AddMember("id", rapidjson::Value(id_str.c_str(), allocator),
                   allocator);

    // Convert label
    std::string label_str;
    switch (vertex.label().item_case()) {
    case common::NameOrId::ItemCase::kName: {
      label_str = vertex.label().name();
      break;
    }
    case common::NameOrId::ItemCase::kId: {
      label_str = std::to_string(vertex.label().id());
      break;
    }
    default: {
      THROW_INTERNAL_EXCEPTION("Unknown vertex label type");
    }
    }

    node.AddMember("label", rapidjson::Value(label_str.c_str(), allocator),
                   allocator);

    rapidjson::Value properties =
        convert_properties_to_json(vertex.properties(), allocator);
    node.AddMember("properties", properties, allocator);

    nodes.PushBack(node, allocator);
    node_ids.insert(vertex.id());
  }
}

// Helper function to convert edge and add to collections
void convert_edge_to_relationship(
    const results::Edge& edge, rapidjson::Value& edges,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  if (edge_ids.find(edge.id()) == edge_ids.end()) {
    rapidjson::Value edge_obj(rapidjson::kObjectType);

    std::string id_str = std::to_string(edge.id());
    edge_obj.AddMember("id", rapidjson::Value(id_str.c_str(), allocator),
                       allocator);

    // Convert label
    std::string label_str;
    switch (edge.label().item_case()) {
    case common::NameOrId::ItemCase::kName: {
      label_str = edge.label().name();
      break;
    }
    case common::NameOrId::ItemCase::kId: {
      label_str = std::to_string(edge.label().id());
      break;
    }
    default: {
      THROW_INTERNAL_EXCEPTION("Unknown edge label type");
    }
    }
    edge_obj.AddMember("label", rapidjson::Value(label_str.c_str(), allocator),
                       allocator);

    std::string src_str = std::to_string(edge.src_id());
    std::string dst_str = std::to_string(edge.dst_id());
    edge_obj.AddMember("source", rapidjson::Value(src_str.c_str(), allocator),
                       allocator);
    edge_obj.AddMember("target", rapidjson::Value(dst_str.c_str(), allocator),
                       allocator);

    rapidjson::Value properties =
        convert_properties_to_json(edge.properties(), allocator);
    edge_obj.AddMember("properties", properties, allocator);

    edges.PushBack(edge_obj, allocator);
    edge_ids.insert(edge.id());
  }
}

// Helper function to create Neo4j-style vertex record field
rapidjson::Value create_vertex_record_field(
    const results::Vertex& vertex,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value field(rapidjson::kObjectType);

  // Identity object
  rapidjson::Value identity(rapidjson::kObjectType);
  identity.AddMember("low", rapidjson::Value(static_cast<int64_t>(vertex.id())),
                     allocator);
  identity.AddMember("high", rapidjson::Value(0), allocator);
  field.AddMember("identity", identity, allocator);

  // Labels array
  rapidjson::Value labels(rapidjson::kArrayType);
  std::string label_str;
  switch (vertex.label().item_case()) {
  case common::NameOrId::ItemCase::kName: {
    label_str = vertex.label().name();
    break;
  }
  case common::NameOrId::ItemCase::kId: {
    label_str = std::to_string(vertex.label().id());
    break;
  }
  default: {
    THROW_INTERNAL_EXCEPTION("Unknown vertex label type");
  }
  }
  labels.PushBack(rapidjson::Value(label_str.c_str(), allocator), allocator);
  field.AddMember("labels", labels, allocator);

  rapidjson::Value properties =
      convert_properties_to_json(vertex.properties(), allocator);
  field.AddMember("properties", properties, allocator);

  std::string element_id = std::to_string(vertex.id());
  field.AddMember("elementId", rapidjson::Value(element_id.c_str(), allocator),
                  allocator);

  return field;
}

// Helper function to create Neo4j-style edge record field
rapidjson::Value create_edge_record_field(
    const results::Edge& edge, rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value field(rapidjson::kObjectType);

  // Identity object
  rapidjson::Value identity(rapidjson::kObjectType);
  identity.AddMember("low", rapidjson::Value(static_cast<int64_t>(edge.id())),
                     allocator);
  identity.AddMember("high", rapidjson::Value(0), allocator);
  field.AddMember("identity", identity, allocator);

  std::string type_str;
  switch (edge.label().item_case()) {
  case common::NameOrId::ItemCase::kName: {
    type_str = edge.label().name();
    break;
  }
  case common::NameOrId::ItemCase::kId: {
    type_str = std::to_string(edge.label().id());
    break;
  }
  default: {
    THROW_INTERNAL_EXCEPTION("Unknown edge label type");
  }
  }
  field.AddMember("type", rapidjson::Value(type_str.c_str(), allocator),
                  allocator);

  // Start and end objects
  rapidjson::Value start(rapidjson::kObjectType);
  start.AddMember("low", rapidjson::Value(static_cast<int64_t>(edge.src_id())),
                  allocator);
  start.AddMember("high", rapidjson::Value(0), allocator);
  field.AddMember("start", start, allocator);

  rapidjson::Value end(rapidjson::kObjectType);
  end.AddMember("low", rapidjson::Value(static_cast<int64_t>(edge.dst_id())),
                allocator);
  end.AddMember("high", rapidjson::Value(0), allocator);
  field.AddMember("end", end, allocator);

  rapidjson::Value properties =
      convert_properties_to_json(edge.properties(), allocator);
  field.AddMember("properties", properties, allocator);

  std::string element_id = std::to_string(edge.id());
  field.AddMember("elementId", rapidjson::Value(element_id.c_str(), allocator),
                  allocator);

  return field;
}

// Helper function to create Neo4j-style path record field
rapidjson::Value create_path_record_field(
    const results::GraphPath& graph_path, rapidjson::Value& nodes,
    rapidjson::Value& edges, std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value path_segments(rapidjson::kArrayType);
  rapidjson::Value start_node(rapidjson::kNullType);
  rapidjson::Value end_node(rapidjson::kNullType);

  for (const auto& path_element : graph_path.path()) {
    if (path_element.has_vertex()) {
      convert_vertex_to_node(path_element.vertex(), nodes, node_ids, allocator);
      rapidjson::Value vertex_field =
          create_vertex_record_field(path_element.vertex(), allocator);
      path_segments.PushBack(vertex_field, allocator);

      if (start_node.IsNull()) {
        start_node.CopyFrom(vertex_field, allocator);
      }
      end_node.CopyFrom(vertex_field, allocator);
    } else if (path_element.has_edge()) {
      convert_edge_to_relationship(path_element.edge(), edges, edge_ids,
                                   allocator);
      rapidjson::Value edge_field =
          create_edge_record_field(path_element.edge(), allocator);
      path_segments.PushBack(edge_field, allocator);
    }
  }

  rapidjson::Value path_field(rapidjson::kObjectType);
  path_field.AddMember("start", start_node, allocator);
  path_field.AddMember("end", end_node, allocator);
  path_field.AddMember("segments", path_segments, allocator);
  path_field.AddMember("length", rapidjson::Value(graph_path.path_size()),
                       allocator);

  return path_field;
}

// Enhanced function to handle property access and primitive values
rapidjson::Value convert_primitive_value(
    const ::common::Value& value,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value result;

  if (value.item_case() == common::Value::ItemCase::kI32) {
    result.SetInt(value.i32());
  } else if (value.item_case() == common::Value::ItemCase::kI64) {
    result.SetInt64(value.i64());
  } else if (value.item_case() == common::Value::ItemCase::kF64) {
    result.SetDouble(value.f64());
  } else if (value.item_case() == common::Value::ItemCase::kStr) {
    result.SetString(value.str().c_str(), allocator);
  } else if (value.item_case() == common::Value::ItemCase::kBoolean) {
    result.SetBool(value.boolean());
  } else if (value.item_case() == common::Value::ItemCase::kStrArray) {
    result.SetArray();
    for (const auto& s : value.str_array().item()) {
      result.PushBack(rapidjson::Value(s.c_str(), allocator), allocator);
    }
  } else if (value.item_case() == common::Value::ItemCase::kI32Array) {
    result.SetArray();
    for (const auto& i : value.i32_array().item()) {
      result.PushBack(rapidjson::Value(i), allocator);
    }
  } else if (value.item_case() == common::Value::ItemCase::kI64Array) {
    result.SetArray();
    for (const auto& i : value.i64_array().item()) {
      result.PushBack(rapidjson::Value(i), allocator);
    }
  } else if (value.item_case() == common::Value::ItemCase::kF64Array) {
    result.SetArray();
    for (const auto& f : value.f64_array().item()) {
      result.PushBack(rapidjson::Value(f), allocator);
    }
  } else if (value.item_case() == common::Value::ItemCase::kNone) {
    result.SetNull();
  } else {
    result.SetNull();
  }

  return result;
}

// Forward declaration for recursive processing
rapidjson::Value process_entry_recursive(
    const results::Entry& entry, rapidjson::Value& nodes,
    rapidjson::Value& edges, std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator);

// Helper function to process collections
rapidjson::Value process_collection(
    const results::Collection& collection, rapidjson::Value& nodes,
    rapidjson::Value& edges, std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value collection_json(rapidjson::kArrayType);
  for (const auto& item : collection.collection()) {
    results::Entry item_entry;
    item_entry.CopyFrom(item);
    rapidjson::Value processed_item = process_entry_recursive(
        item_entry, nodes, edges, node_ids, edge_ids, allocator);
    collection_json.PushBack(processed_item, allocator);
  }
  return collection_json;
}

// Helper function to process key-value maps
rapidjson::Value process_key_values(
    const results::KeyValues& key_values, rapidjson::Value& nodes,
    rapidjson::Value& edges, std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value map_obj(rapidjson::kObjectType);
  for (const auto& kv : key_values.key_values()) {
    std::string key;
    if (kv.key().item_case() == common::Value::ItemCase::kStr) {
      key = kv.key().str();
    } else if (kv.key().item_case() == common::Value::ItemCase::kI64) {
      key = std::to_string(kv.key().i64());
    } else if (kv.key().item_case() == common::Value::ItemCase::kI32) {
      key = std::to_string(kv.key().i32());
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Only string and integer keys are supported in maps");
    }

    rapidjson::Value key_val(key.c_str(), allocator);
    rapidjson::Value value_val = process_entry_recursive(
        kv.value(), nodes, edges, node_ids, edge_ids, allocator);
    map_obj.AddMember(key_val, value_val, allocator);
  }
  return map_obj;
}

// Enhanced entry processing to better handle property access
rapidjson::Value process_entry_recursive(
    const results::Entry& entry, rapidjson::Value& nodes,
    rapidjson::Value& edges, std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  if (entry.has_element()) {
    const auto& element = entry.element();
    if (element.has_vertex()) {
      // For full vertex returns (like RETURN n), add to nodes collection
      convert_vertex_to_node(element.vertex(), nodes, node_ids, allocator);
      return create_vertex_record_field(element.vertex(), allocator);
    } else if (element.has_edge()) {
      // For full edge returns (like RETURN r), add to edges collection
      convert_edge_to_relationship(element.edge(), edges, edge_ids, allocator);
      return create_edge_record_field(element.edge(), allocator);
    } else if (element.has_graph_path()) {
      return create_path_record_field(element.graph_path(), nodes, edges,
                                      node_ids, edge_ids, allocator);
    } else if (element.has_object()) {
      // This handles property access like n.id, n.name - these are primitive
      // values
      return convert_primitive_value(element.object(), allocator);
    }
  } else if (entry.has_collection()) {
    return process_collection(entry.collection(), nodes, edges, node_ids,
                              edge_ids, allocator);
  } else if (entry.has_map()) {
    return process_key_values(entry.map(), nodes, edges, node_ids, edge_ids,
                              allocator);
  }

  rapidjson::Value null_val;
  null_val.SetNull();
  return null_val;
}

// Helper function to parse result schema and extract column names
std::vector<std::string> parse_result_schema_column_names(
    const std::string& result_schema) {
  std::vector<std::string> column_names;

  if (result_schema.empty()) {
    return column_names;
  }

  try {
    YAML::Node schema = YAML::Load(result_schema);

    if (schema["returns"] && schema["returns"].IsSequence()) {
      for (const auto& return_item : schema["returns"]) {
        if (return_item["name"] && return_item["name"].IsScalar()) {
          column_names.push_back(return_item["name"].as<std::string>());
        }
      }
    }
  } catch (const YAML::Exception& e) {
    LOG(WARNING) << "Failed to parse result schema YAML: " << e.what()
                 << ", falling back to default column names";
    // Return empty vector to indicate parsing failure
    column_names.clear();
  }

  return column_names;
}

std::string proto_to_bolt_response(const results::CollectiveResults& result) {
  rapidjson::Document response;
  response.SetObject();
  auto& allocator = response.GetAllocator();

  rapidjson::Value nodes(rapidjson::kArrayType);
  rapidjson::Value edges(rapidjson::kArrayType);
  rapidjson::Value records(rapidjson::kArrayType);
  rapidjson::Value table(rapidjson::kArrayType);

  // Track unique nodes and edges to avoid duplicates
  std::unordered_set<int64_t> node_ids;
  std::unordered_set<int64_t> edge_ids;

  // Parse column names from result schema
  std::vector<std::string> schema_column_names =
      parse_result_schema_column_names(result.result_schema());

  // Process all results and build records
  size_t index = 0;
  for (const auto& res : result.results()) {
    if (res.has_record()) {
      rapidjson::Value record(rapidjson::kObjectType);
      rapidjson::Value keys(rapidjson::kArrayType);
      rapidjson::Value fields(rapidjson::kArrayType);
      rapidjson::Value field_lookup(rapidjson::kObjectType);

      uint64_t field_index = 0;
      for (const auto& column : res.record().columns()) {
        std::string column_name;

        // Use schema column name if available, otherwise fall back to column
        // name/id
        if (field_index < schema_column_names.size() &&
            !schema_column_names[field_index].empty()) {
          column_name = schema_column_names[field_index];
        } else {
          // Fallback to original logic
          if (column.name_or_id().item_case() ==
              common::NameOrId::ItemCase::kName) {
            column_name = column.name_or_id().name();
          } else if (column.name_or_id().item_case() ==
                     common::NameOrId::ItemCase::kId) {
            column_name = std::to_string(column.name_or_id().id());
          } else {
            THROW_INTERNAL_EXCEPTION("Unknown column name/id type");
          }
        }

        keys.PushBack(rapidjson::Value(column_name.c_str(), allocator),
                      allocator);
        rapidjson::Value field_value = process_entry_recursive(
            column.entry(), nodes, edges, node_ids, edge_ids, allocator);
        add_entry_value_to_table(table, index, column_name, column.entry(),
                                 allocator);
        fields.PushBack(field_value, allocator);
        field_lookup.AddMember(rapidjson::Value(column_name.c_str(), allocator),
                               rapidjson::Value(field_index++), allocator);
      }

      record.AddMember("keys", keys, allocator);
      record.AddMember("length",
                       rapidjson::Value(static_cast<int>(fields.Size())),
                       allocator);
      record.AddMember("_fields", fields, allocator);
      record.AddMember("_fieldLookup", field_lookup, allocator);

      records.PushBack(record, allocator);
    }
    index++;
  }

  // Build the complete response
  response.AddMember("nodes", nodes, allocator);
  response.AddMember("edges", edges, allocator);

  // Build raw section (Neo4j Bolt format)
  rapidjson::Value raw(rapidjson::kObjectType);
  raw.AddMember("records", records, allocator);
  raw.AddMember("summary", create_bolt_summary("", allocator), allocator);

  response.AddMember("raw", raw, allocator);
  response.AddMember("table", table, allocator);

  // Convert to string with pretty formatting
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
  response.Accept(writer);

  return buffer.GetString();
}

bool multiplicity_to_storage_strategy(
    const physical::CreateEdgeSchema::Multiplicity& multiplicity,
    EdgeStrategy& oe_strategy, EdgeStrategy& ie_strategy) {
  switch (multiplicity) {
  case physical::CreateEdgeSchema::ONE_TO_ONE:
    oe_strategy = EdgeStrategy::kSingle;
    ie_strategy = EdgeStrategy::kSingle;
    break;
  case physical::CreateEdgeSchema::ONE_TO_MANY:
    oe_strategy = EdgeStrategy::kMultiple;
    ie_strategy = EdgeStrategy::kSingle;
    break;
  case physical::CreateEdgeSchema::MANY_TO_ONE:
    oe_strategy = EdgeStrategy::kSingle;
    ie_strategy = EdgeStrategy::kMultiple;
    break;
  case physical::CreateEdgeSchema::MANY_TO_MANY:
    oe_strategy = EdgeStrategy::kMultiple;
    ie_strategy = EdgeStrategy::kMultiple;
    break;
  default:
    LOG(ERROR) << "Unknown multiplicity: " << multiplicity;
    return false;
  }
  return true;
}

bool primitive_type_to_property_type(
    const common::PrimitiveType& primitive_type, DataTypeId& out_type) {
  switch (primitive_type) {
  case common::PrimitiveType::DT_ANY:
    LOG(ERROR) << "Any type is not supported";
    return false;
  case common::PrimitiveType::DT_SIGNED_INT32:
    out_type = DataTypeId::kInt32;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    out_type = DataTypeId::kUInt32;
    break;
  case common::PrimitiveType::DT_SIGNED_INT64:
    out_type = DataTypeId::kInt64;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    out_type = DataTypeId::kUInt64;
    break;
  case common::PrimitiveType::DT_BOOL:
    out_type = DataTypeId::kBoolean;
    break;
  case common::PrimitiveType::DT_FLOAT:
    out_type = DataTypeId::kFloat;
    break;
  case common::PrimitiveType::DT_DOUBLE:
    out_type = DataTypeId::kDouble;
    break;
  case common::PrimitiveType::DT_NULL:
    out_type = DataTypeId::kEmpty;
    break;
  default:
    LOG(ERROR) << "Unknown primitive type: " << primitive_type;
    return false;
  }
  return true;
}

bool string_type_to_property_type(const common::String& string_type,
                                  DataTypeId& out_type) {
  switch (string_type.item_case()) {
  case common::String::kVarChar: {
    out_type = DataTypeId::kVarchar;
    break;
  }
  case common::String::kLongText: {
    out_type = DataTypeId::kVarchar;
    break;
  }
  case common::String::kChar: {
    // Currently, we implement fixed-char as varchar with fixed length.
    THROW_NOT_SUPPORTED_EXCEPTION("Char type is not supported yet");
  }
  case common::String::ITEM_NOT_SET: {
    LOG(ERROR) << "String type is not set: " << string_type.DebugString();
    return false;
  }
  default:
    LOG(ERROR) << "Unknown string type: " << string_type.DebugString();
    return false;
  }
  return true;
}

bool temporal_type_to_property_type(const common::Temporal& temporal_type,
                                    DataTypeId& out_type) {
  switch (temporal_type.item_case()) {
  case common::Temporal::kDate32:
    out_type = DataTypeId::kDate;
    ;
    break;
  case common::Temporal::kDateTime:
    out_type = DataTypeId::kTimestampMs;
    break;
  case common::Temporal::kTimestamp:
    out_type = DataTypeId::kTimestampMs;
    break;
  case common::Temporal::kDate:
    // TODO(zhanglei): Parse format
    out_type = DataTypeId::kDate;
    break;
  case common::Temporal::kInterval:
    out_type = DataTypeId::kInterval;
    break;
  default:
    LOG(ERROR) << "Unknown temporal type: " << temporal_type.DebugString();
    return false;
  }
  return true;
}

bool data_type_to_property_type(const common::DataType& data_type,
                                DataTypeId& out_type) {
  switch (data_type.item_case()) {
  case common::DataType::kPrimitiveType: {
    return primitive_type_to_property_type(data_type.primitive_type(),
                                           out_type);
  }
  case common::DataType::kDecimal: {
    LOG(ERROR) << "Decimal type is not supported";
    return false;
  }
  case common::DataType::kString: {
    return string_type_to_property_type(data_type.string(), out_type);
  }
  case common::DataType::kTemporal: {
    return temporal_type_to_property_type(data_type.temporal(), out_type);
  }
  case common::DataType::kArray: {
    LOG(ERROR) << "Array type is not supported";
    return false;
  }
  case common::DataType::kMap: {
    LOG(ERROR) << "Map type is not supported";
    return false;
  }
  case common::DataType::ITEM_NOT_SET: {
    LOG(ERROR) << "Data type is not set: " << data_type.DebugString();
    return false;
  }
  default:
    LOG(ERROR) << "Unknown data type: " << data_type.DebugString();
    return false;
  }
}

bool common_value_to_value(const DataTypeId type, const common::Value& value,
                           execution::Value& out_value) {
  switch (value.item_case()) {
  case common::Value::kBoolean:
    out_value = execution::Value::BOOLEAN(value.boolean());
    break;
  case common::Value::kI32:
    out_value = execution::Value::INT32(value.i32());
    break;
  case common::Value::kI64:
    out_value = execution::Value::INT64(value.i64());
    break;
  case common::Value::kU32:
    out_value = execution::Value::UINT32(value.u32());
    break;
  case common::Value::kU64:
    out_value = execution::Value::UINT64(value.u64());
    break;
  case common::Value::kF32:
    out_value = execution::Value::FLOAT(value.f32());
    break;
  case common::Value::kF64:
    out_value = execution::Value::DOUBLE(value.f64());
    break;
  case common::Value::kStr:
    if (type == DataTypeId::kDate) {
      // Special handling for date stored as string
      Date date(value.str());
      out_value = execution::Value::DATE(date);
      break;
    } else if (type == DataTypeId::kTimestampMs) {
      // Special handling for datetime stored as string
      DateTime datetime(value.str());
      out_value = execution::Value::TIMESTAMPMS(datetime);
      break;
    } else if (type == DataTypeId::kInterval) {
      // Special handling for interval stored as string
      Interval interval(value.str());
      out_value = execution::Value::INTERVAL(interval);
      break;
    } else {
      LOG(INFO) << "Setting string value: " << value.str()
                << " for type: " << std::to_string(type);
      out_value = execution::Value::STRING(value.str());
    }
    break;
  case common::Value::kDate:
    out_value = execution::Value::DATE(Date(value.date().item()));
    break;
  default:
    LOG(ERROR) << "Unknown value type: " << value.DebugString();
    return false;
  }
  return true;
}

neug::result<std::vector<std::pair<std::string, execution::Value>>>
property_defs_to_value(
    const google::protobuf::RepeatedPtrField<physical::PropertyDef>&
        properties) {
  std::vector<std::pair<std::string, execution::Value>> result;
  for (const auto& property : properties) {
    const auto& name = property.name();
    execution::Value default_value(DataType::SQLNULL);
    DataTypeId type;
    if (!data_type_to_property_type(property.type(), type)) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid property type: " + property.DebugString()));
    }

    if (property.has_default_value()) {
      if (!common_value_to_value(type, property.default_value(),
                                 default_value)) {
        RETURN_ERROR(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Invalid default value: " + property.DebugString()));
      } else {
        VLOG(10) << "Default value convert to any success:"
                 << property.default_value().DebugString();
      }
    } else {
      default_value = execution::property_to_value(get_default_value(type));
      VLOG(1) << "No default value, use type default:"
              << default_value.to_string()
              << " type: " << default_value.type().ToString();
    }
    result.emplace_back(name, default_value);
  }
  return result;
}

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const physical::ConflictAction& action) {
  if (action == physical::ConflictAction::ON_CONFLICT_THROW) {
    return true;
  } else if (action == physical::ConflictAction::ON_CONFLICT_DO_NOTHING) {
    return false;
  } else {
    LOG(FATAL) << "invalid action: " << action;
    return false;  // to suppress warning
  }
}

}  // namespace neug
