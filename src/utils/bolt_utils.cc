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

#include "neug/utils/bolt_utils.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include "glog/logging.h"

#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"

namespace neug {

rapidjson::Value create_bolt_summary(
    const std::string& query_text,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value summary(rapidjson::kObjectType);

  // Query object
  rapidjson::Value query(rapidjson::kObjectType);
  query.AddMember("text", rapidjson::Value(query_text.c_str(), allocator),
                  allocator);
  query.AddMember("parameters", rapidjson::Value(rapidjson::kObjectType),
                  allocator);
  summary.AddMember("query", query, allocator);

  summary.AddMember("queryType", rapidjson::Value("r", allocator), allocator);

  // Add counters (all zeros for read queries)
  rapidjson::Value counters(rapidjson::kObjectType);
  rapidjson::Value stats(rapidjson::kObjectType);
  stats.AddMember("nodesCreated", rapidjson::Value(0), allocator);
  stats.AddMember("nodesDeleted", rapidjson::Value(0), allocator);
  stats.AddMember("relationshipsCreated", rapidjson::Value(0), allocator);
  stats.AddMember("relationshipsDeleted", rapidjson::Value(0), allocator);
  stats.AddMember("propertiesSet", rapidjson::Value(0), allocator);
  stats.AddMember("labelsAdded", rapidjson::Value(0), allocator);
  stats.AddMember("labelsRemoved", rapidjson::Value(0), allocator);
  stats.AddMember("indexesAdded", rapidjson::Value(0), allocator);
  stats.AddMember("indexesRemoved", rapidjson::Value(0), allocator);
  stats.AddMember("constraintsAdded", rapidjson::Value(0), allocator);
  stats.AddMember("constraintsRemoved", rapidjson::Value(0), allocator);
  counters.AddMember("_stats", stats, allocator);
  counters.AddMember("_systemUpdates", rapidjson::Value(0), allocator);

  summary.AddMember("counters", counters, allocator);
  summary.AddMember("updateStatistics", rapidjson::Value(counters, allocator),
                    allocator);
  summary.AddMember("plan", rapidjson::Value(false), allocator);
  summary.AddMember("profile", rapidjson::Value(false), allocator);
  summary.AddMember("notifications", rapidjson::Value(rapidjson::kArrayType),
                    allocator);

  // Add status information
  rapidjson::Value status(rapidjson::kObjectType);
  status.AddMember("gqlStatus", rapidjson::Value("00000", allocator),
                   allocator);
  status.AddMember("statusDescription",
                   rapidjson::Value("note: successful completion", allocator),
                   allocator);

  rapidjson::Value diagnostic(rapidjson::kObjectType);
  diagnostic.AddMember("OPERATION", rapidjson::Value("", allocator), allocator);
  diagnostic.AddMember("OPERATION_CODE", rapidjson::Value("0", allocator),
                       allocator);
  diagnostic.AddMember("CURRENT_SCHEMA", rapidjson::Value("/", allocator),
                       allocator);
  status.AddMember("diagnosticRecord", diagnostic, allocator);

  status.AddMember("severity", rapidjson::Value("UNKNOWN", allocator),
                   allocator);
  status.AddMember("classification", rapidjson::Value("UNKNOWN", allocator),
                   allocator);
  status.AddMember("isNotification", rapidjson::Value(false), allocator);

  rapidjson::Value gql_status_objects(rapidjson::kArrayType);
  gql_status_objects.PushBack(status, allocator);
  summary.AddMember("gqlStatusObjects", gql_status_objects, allocator);

  // Add server info
  rapidjson::Value server(rapidjson::kObjectType);
  server.AddMember("address", rapidjson::Value("127.0.0.1:7687", allocator),
                   allocator);
  server.AddMember("agent", rapidjson::Value("GraphScope/1.0.0", allocator),
                   allocator);
  server.AddMember("protocolVersion", rapidjson::Value(4.4), allocator);
  summary.AddMember("server", server, allocator);

  // Add timing info
  rapidjson::Value result_consumed(rapidjson::kObjectType);
  result_consumed.AddMember("low", rapidjson::Value(27), allocator);
  result_consumed.AddMember("high", rapidjson::Value(0), allocator);
  summary.AddMember("resultConsumedAfter", result_consumed, allocator);

  rapidjson::Value result_available(rapidjson::kObjectType);
  result_available.AddMember("low", rapidjson::Value(70), allocator);
  result_available.AddMember("high", rapidjson::Value(0), allocator);
  summary.AddMember("resultAvailableAfter", result_available, allocator);

  // Add database info
  rapidjson::Value database(rapidjson::kObjectType);
  database.AddMember("name", rapidjson::Value("graphscope", allocator),
                     allocator);
  summary.AddMember("database", database, allocator);

  return summary;
}

// Helper function to extract value from Arrow array at given index
rapidjson::Value value_to_bolt(const neug::Array& column, int64_t index,
                               rapidjson::Document::AllocatorType& allocator);

inline bool is_valid(const std::string& map, size_t i) {
  return map.empty() || (static_cast<uint8_t>(map[i >> 3]) >> (i & 7)) & 1;
}

static rapidjson::Value make_bolt_int64(
    int64_t value, rapidjson::Document::AllocatorType& allocator) {
  uint64_t u = static_cast<uint64_t>(value);
  int32_t low = static_cast<int32_t>(u & 0xFFFFFFFFULL);
  int32_t high = static_cast<int32_t>(u >> 32);

  rapidjson::Value obj(rapidjson::kObjectType);
  obj.AddMember("low", rapidjson::Value(low), allocator);
  obj.AddMember("high", rapidjson::Value(high), allocator);
  return obj;
}

void convert_vertex_to_node(const rapidjson::Value& doc,
                            rapidjson::Value& nodes,
                            std::unordered_set<std::string>& node_ids,
                            rapidjson::Document::AllocatorType& allocator) {
  int64_t gid = doc["_ID"].GetInt64();
  std::string element_id = std::to_string(gid);
  if (node_ids.find(element_id) == node_ids.end()) {
    rapidjson::Value node(rapidjson::kObjectType);
    std::string id_str = element_id;
    node.AddMember(
        "id",
        rapidjson::Value(id_str.c_str(),
                         static_cast<rapidjson::SizeType>(id_str.size()),
                         allocator),
        allocator);
    node.AddMember("label",
                   rapidjson::Value(doc["_LABEL"].GetString(),
                                    doc["_LABEL"].GetStringLength(), allocator),
                   allocator);
    rapidjson::Value properties(rapidjson::kObjectType);
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
      const auto& name = it->name;
      const char* k = name.GetString();
      if (std::strcmp(k, "_ID") != 0 && std::strcmp(k, "_LABEL") != 0) {
        rapidjson::Value jsonKey(name.GetString(), name.GetStringLength(),
                                 allocator);
        rapidjson::Value jsonValue(it->value, allocator);
        properties.AddMember(jsonKey, jsonValue, allocator);
      }
    }
    node.AddMember("properties", properties, allocator);
    node_ids.insert(element_id);
    nodes.PushBack(node.Move(), allocator);
  }
}

void convert_edge_to_relationship(
    const rapidjson::Value& doc, rapidjson::Value& relationships,
    std::unordered_set<std::string>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  int64_t gid = doc["_ID"].GetInt64();
  std::string element_id = std::to_string(gid);
  if (edge_ids.find(element_id) == edge_ids.end()) {
    rapidjson::Value relationship(rapidjson::kObjectType);
    relationship.AddMember(
        "id",
        rapidjson::Value(element_id.c_str(),
                         static_cast<rapidjson::SizeType>(element_id.size()),
                         allocator),
        allocator);
    relationship.AddMember(
        "label",
        rapidjson::Value(doc["_LABEL"].GetString(),
                         doc["_LABEL"].GetStringLength(), allocator),
        allocator);
    std::string source_id_str = std::to_string(doc["_SRC_ID"].GetInt64());
    std::string target_id_str = std::to_string(doc["_DST_ID"].GetInt64());
    relationship.AddMember(
        "source",
        rapidjson::Value(source_id_str.c_str(),
                         static_cast<rapidjson::SizeType>(source_id_str.size()),
                         allocator),
        allocator);
    relationship.AddMember(
        "target",
        rapidjson::Value(target_id_str.c_str(),
                         static_cast<rapidjson::SizeType>(target_id_str.size()),
                         allocator),
        allocator);
    rapidjson::Value properties(rapidjson::kObjectType);
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
      const auto& name = it->name;
      const char* k = name.GetString();
      if (std::strcmp(k, "_ID") != 0 && std::strcmp(k, "_LABEL") != 0 &&
          std::strcmp(k, "_SRC_ID") != 0 && std::strcmp(k, "_DST_ID") != 0) {
        rapidjson::Value jsonKey(name.GetString(), name.GetStringLength(),
                                 allocator);
        rapidjson::Value jsonValue(it->value, allocator);
        properties.AddMember(jsonKey, jsonValue, allocator);
      }
    }
    relationship.AddMember("properties", properties, allocator);
    relationships.PushBack(relationship.Move(), allocator);
    edge_ids.insert(element_id);
  }
}

rapidjson::Value vertex_to_bolt(const rapidjson::Value& doc,
                                rapidjson::Document::AllocatorType& allocator,
                                std::unordered_set<std::string>& element_ids,
                                rapidjson::Value& nodes) {
  rapidjson::Value vertex(rapidjson::kObjectType);
  int64_t gid = doc["_ID"].GetInt64();
  auto identity = make_bolt_int64(gid, allocator);
  vertex.AddMember("identity", identity, allocator);
  rapidjson::Value labelsArr(rapidjson::kArrayType);
  labelsArr.PushBack(
      rapidjson::Value(doc["_LABEL"].GetString(),
                       doc["_LABEL"].GetStringLength(), allocator),
      allocator);
  vertex.AddMember("labels", labelsArr, allocator);
  rapidjson::Value properties(rapidjson::kObjectType);
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    const auto& name = it->name;
    const char* k = name.GetString();
    if (std::strcmp(k, "_ID") != 0 && std::strcmp(k, "_LABEL") != 0) {
      rapidjson::Value jsonKey(name.GetString(), name.GetStringLength(),
                               allocator);

      rapidjson::Value jsonValue(it->value, allocator);
      properties.AddMember(jsonKey, jsonValue, allocator);
    }
  }
  vertex.AddMember("properties", properties, allocator);
  std::string element_id = std::to_string(gid);
  vertex.AddMember(
      "elementId",
      rapidjson::Value(element_id.c_str(),
                       static_cast<rapidjson::SizeType>(element_id.size()),
                       allocator),
      allocator);
  convert_vertex_to_node(doc, nodes, element_ids, allocator);

  return vertex;
}

rapidjson::Value edge_to_bolt(const rapidjson::Value& doc,
                              rapidjson::Document::AllocatorType& allocator,
                              std::unordered_set<std::string>& element_ids,
                              rapidjson::Value& edges) {
  rapidjson::Value edge(rapidjson::kObjectType);
  int64_t gid = doc["_ID"].GetInt64();
  auto identity = make_bolt_int64(gid, allocator);
  edge.AddMember("identity", identity, allocator);
  edge.AddMember("type",
                 rapidjson::Value(doc["_LABEL"].GetString(),
                                  doc["_LABEL"].GetStringLength(), allocator),
                 allocator);

  // Start node
  int64_t src_id = doc["_SRC_ID"].GetInt64();
  rapidjson::Value start(rapidjson::kObjectType);
  int32_t src_low = static_cast<int32_t>(src_id & 0xFFFFFFFFULL);
  int32_t src_high = static_cast<int32_t>(src_id >> 32);
  start.AddMember("low", rapidjson::Value(src_low), allocator);
  start.AddMember("high", rapidjson::Value(src_high), allocator);
  edge.AddMember("start", start, allocator);

  // End node
  int64_t dst_id = doc["_DST_ID"].GetInt64();
  rapidjson::Value end(rapidjson::kObjectType);
  int32_t dst_low = static_cast<int32_t>(dst_id & 0xFFFFFFFFULL);
  int32_t dst_high = static_cast<int32_t>(dst_id >> 32);
  end.AddMember("low", rapidjson::Value(dst_low), allocator);
  end.AddMember("high", rapidjson::Value(dst_high), allocator);
  edge.AddMember("end", end, allocator);

  // Properties
  rapidjson::Value properties(rapidjson::kObjectType);
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    const auto& name = it->name;
    const char* k = name.GetString();
    if (std::strcmp(k, "_ID") != 0 && std::strcmp(k, "_LABEL") != 0 &&
        std::strcmp(k, "_SRC_ID") != 0 && std::strcmp(k, "_DST_ID") != 0) {
      rapidjson::Value jsonKey(name.GetString(), name.GetStringLength(),
                               allocator);

      rapidjson::Value jsonValue(it->value, allocator);
      properties.AddMember(jsonKey, jsonValue, allocator);
    }
  }
  edge.AddMember("properties", properties, allocator);
  std::string element_id = std::to_string(gid);
  edge.AddMember(
      "elementId",
      rapidjson::Value(element_id.c_str(),
                       static_cast<rapidjson::SizeType>(element_id.size()),
                       allocator),
      allocator);
  convert_edge_to_relationship(doc, edges, element_ids, allocator);
  return edge;
}

rapidjson::Value value_to_bolt(const neug::Array& column, int64_t index,
                               rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value null_val;
  null_val.SetNull();
  if (column.has_bool_array()) {
    const auto& bool_col = column.bool_array();
    const auto& bool_map = bool_col.validity();
    if (is_valid(bool_map, index)) {
      return rapidjson::Value(bool_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_int32_array()) {
    const auto& int32_col = column.int32_array();
    const auto& int32_map = int32_col.validity();
    if (is_valid(int32_map, index)) {
      return rapidjson::Value(int32_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_uint32_array()) {
    const auto& uint32_col = column.uint32_array();
    const auto& uint32_map = uint32_col.validity();
    if (is_valid(uint32_map, index)) {
      return rapidjson::Value(uint32_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_int64_array()) {
    const auto& int64_col = column.int64_array();
    const auto& int64_map = int64_col.validity();
    if (is_valid(int64_map, index)) {
      return rapidjson::Value(int64_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_uint64_array()) {
    const auto& uint64_col = column.uint64_array();
    const auto& uint64_map = uint64_col.validity();
    if (is_valid(uint64_map, index)) {
      return rapidjson::Value(uint64_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_float_array()) {
    const auto& float_col = column.float_array();
    const auto& float_map = float_col.validity();
    if (is_valid(float_map, index)) {
      return rapidjson::Value(float_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_double_array()) {
    const auto& double_col = column.double_array();
    const auto& double_map = double_col.validity();
    if (is_valid(double_map, index)) {
      return rapidjson::Value(double_col.values(index));
    } else {
      return null_val;
    }
  } else if (column.has_string_array()) {
    const auto& string_col = column.string_array();
    const auto& string_map = string_col.validity();
    if (is_valid(string_map, index)) {
      auto view = string_col.values(index);
      return rapidjson::Value(view.data(),
                              static_cast<rapidjson::SizeType>(view.size()),
                              allocator);
    } else {
      return null_val;
    }
  } else if (column.has_date_array()) {
    const auto& date_col = column.date_array();
    const auto& date_map = date_col.validity();
    if (is_valid(date_map, index)) {
      int64_t days = date_col.values(index);
      Date day;
      day.from_timestamp(days);
      std::stringstream ss;
      ss << day.year() << "-" << std::setfill('0') << std::setw(2)
         << day.month() << "-" << std::setfill('0') << std::setw(2)
         << day.day();
      return rapidjson::Value(ss.str().c_str(), allocator);
    } else {
      return null_val;
    }
  } else if (column.has_timestamp_array()) {
    const auto& timestamp_col = column.timestamp_array();
    const auto& timestamp_map = timestamp_col.validity();
    if (is_valid(timestamp_map, index)) {
      int64_t milliseconds = timestamp_col.values(index);
      std::time_t seconds = milliseconds / 1000;
      int ms = milliseconds % 1000;
      std::tm* tm_info = std::gmtime(&seconds);
      std::stringstream ss;
      ss << std::put_time(tm_info, "%Y-%m-%dT%H:%M:%S");
      if (ms > 0) {
        ss << "." << std::setfill('0') << std::setw(3) << ms;
      }
      ss << "Z";
      return rapidjson::Value(ss.str().c_str(), allocator);
    } else {
      return null_val;
    }
  } else if (column.has_interval_array()) {
    const auto& interval_col = column.interval_array();
    const auto& interval_map = interval_col.validity();
    if (is_valid(interval_map, index)) {
      const auto& interval = interval_col.values(index);
      return rapidjson::Value(interval.c_str(),
                              static_cast<rapidjson::SizeType>(interval.size()),
                              allocator);
    } else {
      return null_val;
    }
  } else {
    // Unsupported type, return null
    LOG(WARNING) << "Unsupported column type: " << column.DebugString();
    return null_val;
  }
}

// Helper function to convert Arrow Path STRUCT to Bolt path format
rapidjson::Value path_to_bolt(const rapidjson::Value& doc, int64_t index,
                              rapidjson::Value& nodes, rapidjson::Value& edges,
                              std::unordered_set<std::string>& node_ids,
                              std::unordered_set<std::string>& edge_ids,
                              rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value segments(rapidjson::kArrayType);
  rapidjson::Value start_node(rapidjson::kNullType);
  rapidjson::Value end_node(rapidjson::kNullType);
  int path_length = 0;

  // Process vertices
  if (doc.HasMember("nodes") && doc["nodes"].IsArray()) {
    auto nodes_array = doc["nodes"].GetArray();

    for (int32_t i = 0; i < nodes_array.Size(); ++i) {
      auto vertex_ref =
          vertex_to_bolt(nodes_array[i], allocator, node_ids, nodes);
      if (i == 0) {
        start_node.CopyFrom(vertex_ref, allocator);
      }
      end_node.CopyFrom(vertex_ref, allocator);
      segments.PushBack(vertex_ref, allocator);
    }
  }

  // Process edges and create segments
  if (doc.HasMember("rels") && doc["rels"].IsArray()) {
    const auto& edges_array = doc["rels"].GetArray();

    for (int32_t i = 0; i < edges_array.Size(); ++i) {
      rapidjson::Value segment =
          edge_to_bolt(edges_array[i], allocator, edge_ids, edges);

      segments.PushBack(segment, allocator);
      path_length++;
    }
  }

  // Build the path object
  rapidjson::Value path_obj(rapidjson::kObjectType);
  path_obj.AddMember("start", start_node, allocator);
  path_obj.AddMember("end", end_node, allocator);
  path_obj.AddMember("segments", segments, allocator);
  path_obj.AddMember("length", rapidjson::Value(path_length), allocator);

  return path_obj;
}

void convert_entry_to_table(const neug::Array& column, size_t index,
                            rapidjson::Value& val,
                            rapidjson::Document::AllocatorType& allocator) {
  if (column.has_vertex_array()) {
    const auto& vertex_col = column.vertex_array();
    const auto& vertex_map = vertex_col.validity();
    if (is_valid(vertex_map, index)) {
      const std::string vertex_str = vertex_col.values(index);
      val = rapidjson::Value(
          vertex_str.c_str(),
          static_cast<rapidjson::SizeType>(vertex_str.size()), allocator);
    } else {
      val.SetNull();
    }
  } else if (column.has_edge_array()) {
    const auto& edge_col = column.edge_array();
    const auto& edge_map = edge_col.validity();
    if (is_valid(edge_map, index)) {
      const std::string edge_str = edge_col.values(index);
      val = rapidjson::Value(edge_str.c_str(),
                             static_cast<rapidjson::SizeType>(edge_str.size()),
                             allocator);
    } else {
      val.SetNull();
    }
  } else if (column.has_path_array()) {
    const auto& path_col = column.path_array();
    const auto& path_map = path_col.validity();
    if (is_valid(path_map, index)) {
      const std::string path_str = path_col.values(index);
      val = rapidjson::Value(path_str.c_str(),
                             static_cast<rapidjson::SizeType>(path_str.size()),
                             allocator);
    } else {
      val.SetNull();
    }
  } else if (column.has_list_array()) {
    const auto& list_col = column.list_array();
    const auto& list_map = list_col.validity();
    if (is_valid(list_map, index)) {
      const auto& offsets = list_col.offsets();
      auto start = offsets[index];
      auto end = offsets[index + 1];
      const auto& child = list_col.elements();
      rapidjson::Value list_val(rapidjson::kArrayType);
      for (uint32_t i = start; i < end; ++i) {
        rapidjson::Value item;
        convert_entry_to_table(child, i, item, allocator);
        list_val.PushBack(item, allocator);
      }
      val = list_val;
    } else {
      val.SetNull();
    }
  } else if (column.has_struct_array()) {
    const auto& struct_column = column.struct_array();
    const auto& struct_map = struct_column.validity();
    if (is_valid(struct_map, index)) {
      rapidjson::Value list_val(rapidjson::kArrayType);
      size_t num_fields = struct_column.fields_size();
      for (size_t i = 0; i < num_fields; ++i) {
        rapidjson::Value item;
        convert_entry_to_table(struct_column.fields(i), index, item, allocator);
        list_val.PushBack(item, allocator);
      }
      val = list_val;
    } else {
      val.SetNull();
    }
  } else {
    val = value_to_bolt(column, index, allocator);
  }
}

void convert_entry_to_field(const neug::Array& column, size_t index,
                            rapidjson::Value& field,
                            rapidjson::Document::AllocatorType& allocator,
                            std::unordered_set<std::string>& node_ids,
                            std::unordered_set<std::string>& edge_ids,
                            rapidjson::Value& nodes, rapidjson::Value& edges) {
  if (column.has_vertex_array()) {
    const auto& vertex_col = column.vertex_array();
    const auto& vertex_map = vertex_col.validity();
    if (is_valid(vertex_map, index)) {
      const std::string vertex_str = column.vertex_array().values(index);
      rapidjson::Document doc;
      if (doc.Parse(vertex_str.c_str()).HasParseError()) {
        LOG(ERROR) << "Failed to parse vertex struct string: " << vertex_str;
        field.SetNull();
        return;
      }
      field = vertex_to_bolt(doc, allocator, node_ids, nodes);
    } else {
      field.SetNull();
    }
  } else if (column.has_edge_array()) {
    const auto& edge_col = column.edge_array();
    const auto& edge_map = edge_col.validity();
    if (is_valid(edge_map, index)) {
      const std::string edge_str = column.edge_array().values(index);
      rapidjson::Document doc;
      if (doc.Parse(edge_str.c_str()).HasParseError()) {
        LOG(ERROR) << "Failed to parse edge struct string: " << edge_str;
        field.SetNull();
        return;
      }
      field = edge_to_bolt(doc, allocator, edge_ids, edges);
    } else {
      field.SetNull();
    }
  } else if (column.has_path_array()) {
    const auto& path_col = column.path_array();
    const auto& path_map = path_col.validity();
    if (is_valid(path_map, index)) {
      const std::string path_str = column.path_array().values(index);
      rapidjson::Document doc;
      if (doc.Parse(path_str.c_str()).HasParseError()) {
        LOG(ERROR) << "Failed to parse path struct string: " << path_str;
        field.SetNull();
        return;
      }
      field =
          path_to_bolt(doc, index, nodes, edges, node_ids, edge_ids, allocator);
    } else {
      field.SetNull();
    }
  } else if (column.has_list_array()) {
    const auto& list_col = column.list_array();
    const auto& list_map = list_col.validity();
    if (is_valid(list_map, index)) {
      const auto& offsets = list_col.offsets();
      auto start = offsets[index];
      auto end = offsets[index + 1];
      const auto& child = list_col.elements();
      rapidjson::Value list_val(rapidjson::kArrayType);
      for (uint32_t i = start; i < end; ++i) {
        rapidjson::Value item;
        convert_entry_to_field(child, i, item, allocator, node_ids, edge_ids,
                               nodes, edges);
        list_val.PushBack(item, allocator);
      }
      field = list_val;
    } else {
      field.SetNull();
    }
  } else if (column.has_struct_array()) {
    const auto& struct_column = column.struct_array();
    const auto& struct_map = struct_column.validity();
    if (is_valid(struct_map, index)) {
      rapidjson::Value struct_val(rapidjson::kArrayType);
      size_t num_fields = struct_column.fields_size();
      for (size_t i = 0; i < num_fields; ++i) {
        rapidjson::Value item;
        convert_entry_to_field(struct_column.fields(i), index, item, allocator,
                               node_ids, edge_ids, nodes, edges);
        struct_val.PushBack(item, allocator);
      }
      field = struct_val;
    } else {
      field.SetNull();
    }
  } else {
    field = value_to_bolt(column, index, allocator);
  }
}
std::string results_to_bolt_response(
    const neug::QueryResponse& table,
    const std::vector<std::string>& column_names) {
  rapidjson::Document response;
  response.SetObject();
  auto& allocator = response.GetAllocator();

  rapidjson::Value nodes(rapidjson::kArrayType);
  rapidjson::Value edges(rapidjson::kArrayType);
  rapidjson::Value records(rapidjson::kArrayType);
  rapidjson::Value table_json(rapidjson::kArrayType);

  std::unordered_set<std::string> node_ids;
  std::unordered_set<std::string> edge_ids;

  // Get the number of rows and columns
  int64_t num_rows = table.row_count();
  int num_columns = table.arrays_size();

  // Process each row
  for (int64_t row = 0; row < num_rows; ++row) {
    rapidjson::Value record(rapidjson::kObjectType);
    rapidjson::Value keys(rapidjson::kArrayType);
    rapidjson::Value fields(rapidjson::kArrayType);
    rapidjson::Value field_lookup(rapidjson::kObjectType);
    rapidjson::Value table_row(rapidjson::kObjectType);

    for (int col = 0; col < num_columns; ++col) {
      std::string column_name;

      if (col < (int) column_names.size() && !column_names[col].empty()) {
        column_name = column_names[col];
      } else {
        column_name = "column" + std::to_string(col);
      }
      keys.PushBack(rapidjson::Value(column_name.c_str(), allocator),
                    allocator);

      auto column = table.arrays(col);

      rapidjson::Value field_value;
      convert_entry_to_field(column, row, field_value, allocator, node_ids,
                             edge_ids, nodes, edges);
      fields.PushBack(rapidjson::Value(field_value, allocator), allocator);
      field_lookup.AddMember(
          rapidjson::Value(column_name.c_str(),
                           static_cast<rapidjson::SizeType>(column_name.size()),
                           allocator),
          rapidjson::Value(col), allocator);

      // Add to table
      {
        rapidjson::Value table_value;
        convert_entry_to_table(column, row, table_value, allocator);
        table_row.AddMember(rapidjson::Value(column_name.c_str(), allocator),
                            table_value, allocator);
      }
    }

    record.AddMember("keys", keys, allocator);
    record.AddMember("length", rapidjson::Value(num_columns), allocator);
    record.AddMember("_fields", fields, allocator);
    record.AddMember("_fieldLookup", field_lookup, allocator);

    records.PushBack(record, allocator);
    table_json.PushBack(table_row, allocator);
  }

  // Build the complete response
  response.AddMember("nodes", nodes, allocator);
  response.AddMember("edges", edges, allocator);

  // Build raw section (Neo4j Bolt format)
  rapidjson::Value raw(rapidjson::kObjectType);
  raw.AddMember("records", records, allocator);
  raw.AddMember("summary", create_bolt_summary("", allocator), allocator);

  response.AddMember("raw", raw, allocator);
  response.AddMember("table", table_json, allocator);

  // Convert to string with pretty formatting
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
  response.Accept(writer);

  return buffer.GetString();
}

}  // namespace neug
