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

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/iterator.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
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

#include "neug/utils/bolt_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"

namespace neug {

namespace {

template <typename ArrowArray>
rapidjson::Value arrow_numeric_to_json(
    const std::shared_ptr<arrow::Array>& column, int64_t index) {
  return rapidjson::Value(
      std::static_pointer_cast<ArrowArray>(column)->Value(index));
}

template <typename ArrowArray>
rapidjson::Value arrow_string_to_json(
    const std::shared_ptr<arrow::Array>& column, int64_t index,
    rapidjson::Document::AllocatorType& allocator) {
  auto view = std::static_pointer_cast<ArrowArray>(column)->GetView(index);
  return rapidjson::Value(
      view.data(), static_cast<rapidjson::SizeType>(view.size()), allocator);
}

}  // namespace

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
rapidjson::Value arrow_value_to_json(
    std::shared_ptr<arrow::Array> column, int64_t index,
    rapidjson::Document::AllocatorType& allocator);

// Helper function to convert Arrow STRUCT (Vertex/Edge/Path) to Bolt format
rapidjson::Value arrow_struct_to_bolt_element(
    std::shared_ptr<arrow::StructArray> struct_array, int64_t index,
    const std::string& label, rapidjson::Value& nodes, rapidjson::Value& edges,
    std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  auto struct_type =
      std::static_pointer_cast<arrow::StructType>(struct_array->type());

  // Check if this is a path structure (has vertices and edges fields)
  bool has_vertices = false;
  bool has_edges_field = false;
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    auto field_name = struct_type->field(i)->name();
    if (field_name == "vertices") {
      has_vertices = true;
    } else if (field_name == "edges") {
      has_edges_field = true;
    }
  }

  // If it's a path, handle it specially
  if (has_vertices && has_edges_field) {
    return arrow_path_to_bolt_path(struct_array, index, nodes, edges, node_ids,
                                   edge_ids, allocator);
  }

  // Check if this is a vertex or edge by looking for required fields
  bool has_id = false;
  bool has_src_id = false;
  bool has_dst_id = false;
  int id_idx = -1, src_id_idx = -1, dst_id_idx = -1;

  for (int i = 0; i < struct_type->num_fields(); ++i) {
    auto field_name = struct_type->field(i)->name();
    if (field_name == "_ID") {
      has_id = true;
      id_idx = i;
    } else if (field_name == "_SRC_ID") {
      has_src_id = true;
      src_id_idx = i;
    } else if (field_name == "_DST_ID") {
      has_dst_id = true;
      dst_id_idx = i;
    }
  }

  if (has_id && has_src_id && has_dst_id) {
    // This is an Edge
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(
        struct_array->field(id_idx));
    int64_t edge_id = id_array->Value(index);

    if (edge_ids.find(edge_id) == edge_ids.end()) {
      edge_ids.insert(edge_id);

      rapidjson::Value edge_obj(rapidjson::kObjectType);
      rapidjson::Value identity(rapidjson::kObjectType);
      identity.AddMember("low", rapidjson::Value(edge_id), allocator);
      identity.AddMember("high", rapidjson::Value(0), allocator);
      edge_obj.AddMember("identity", identity, allocator);

      edge_obj.AddMember("type", rapidjson::Value(label.c_str(), allocator),
                         allocator);

      // Start node
      auto src_id_array = std::static_pointer_cast<arrow::Int64Array>(
          struct_array->field(src_id_idx));
      int64_t src_id = src_id_array->Value(index);
      rapidjson::Value start(rapidjson::kObjectType);
      start.AddMember("low", rapidjson::Value(src_id), allocator);
      start.AddMember("high", rapidjson::Value(0), allocator);
      edge_obj.AddMember("start", start, allocator);

      // End node
      auto dst_id_array = std::static_pointer_cast<arrow::Int64Array>(
          struct_array->field(dst_id_idx));
      int64_t dst_id = dst_id_array->Value(index);
      rapidjson::Value end(rapidjson::kObjectType);
      end.AddMember("low", rapidjson::Value(dst_id), allocator);
      end.AddMember("high", rapidjson::Value(0), allocator);
      edge_obj.AddMember("end", end, allocator);

      // Properties
      rapidjson::Value properties(rapidjson::kObjectType);
      for (int i = 0; i < struct_type->num_fields(); ++i) {
        auto field_name = struct_type->field(i)->name();
        if (field_name != "_ID" && field_name != "_LABEL" &&
            field_name != "_SRC_ID" && field_name != "_DST_ID" &&
            field_name != "_SRC_LABEL" && field_name != "_DST_LABEL") {
          auto child_array = struct_array->field(i);
          if (!child_array->IsNull(index)) {
            properties.AddMember(
                rapidjson::Value(field_name.c_str(), allocator),
                arrow_value_to_json(child_array, index, allocator), allocator);
          }
        }
      }
      edge_obj.AddMember("properties", properties, allocator);

      std::string element_id = std::to_string(edge_id);
      edge_obj.AddMember("elementId",
                         rapidjson::Value(element_id.c_str(), allocator),
                         allocator);

      edges.PushBack(edge_obj, allocator);
    }

    // Return reference to edge
    rapidjson::Value edge_ref(rapidjson::kObjectType);
    rapidjson::Value identity(rapidjson::kObjectType);
    identity.AddMember("low", rapidjson::Value(edge_id), allocator);
    identity.AddMember("high", rapidjson::Value(0), allocator);
    edge_ref.AddMember("identity", identity, allocator);
    edge_ref.AddMember("type", rapidjson::Value(label.c_str(), allocator),
                       allocator);

    auto src_id_array = std::static_pointer_cast<arrow::Int64Array>(
        struct_array->field(src_id_idx));
    int64_t src_id = src_id_array->Value(index);
    rapidjson::Value start(rapidjson::kObjectType);
    start.AddMember("low", rapidjson::Value(src_id), allocator);
    start.AddMember("high", rapidjson::Value(0), allocator);
    edge_ref.AddMember("start", start, allocator);

    auto dst_id_array = std::static_pointer_cast<arrow::Int64Array>(
        struct_array->field(dst_id_idx));
    int64_t dst_id = dst_id_array->Value(index);
    rapidjson::Value end(rapidjson::kObjectType);
    end.AddMember("low", rapidjson::Value(dst_id), allocator);
    end.AddMember("high", rapidjson::Value(0), allocator);
    edge_ref.AddMember("end", end, allocator);

    rapidjson::Value properties(rapidjson::kObjectType);
    for (int i = 0; i < struct_type->num_fields(); ++i) {
      auto field_name = struct_type->field(i)->name();
      if (field_name != "_ID" && field_name != "_LABEL" &&
          field_name != "_SRC_ID" && field_name != "_DST_ID" &&
          field_name != "_SRC_LABEL" && field_name != "_DST_LABEL") {
        auto child_array = struct_array->field(i);
        if (!child_array->IsNull(index)) {
          properties.AddMember(
              rapidjson::Value(field_name.c_str(), allocator),
              arrow_value_to_json(child_array, index, allocator), allocator);
        }
      }
    }
    edge_ref.AddMember("properties", properties, allocator);

    std::string element_id = std::to_string(edge_id);
    edge_ref.AddMember("elementId",
                       rapidjson::Value(element_id.c_str(), allocator),
                       allocator);

    return edge_ref;

  } else if (has_id) {
    // This is a Vertex
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(
        struct_array->field(id_idx));
    int64_t node_id = id_array->Value(index);

    if (node_ids.find(node_id) == node_ids.end()) {
      node_ids.insert(node_id);

      rapidjson::Value node_obj(rapidjson::kObjectType);
      rapidjson::Value identity(rapidjson::kObjectType);
      identity.AddMember("low", rapidjson::Value(node_id), allocator);
      identity.AddMember("high", rapidjson::Value(0), allocator);
      node_obj.AddMember("identity", identity, allocator);

      rapidjson::Value labels(rapidjson::kArrayType);
      labels.PushBack(rapidjson::Value(label.c_str(), allocator), allocator);
      node_obj.AddMember("labels", labels, allocator);

      // Properties
      rapidjson::Value properties(rapidjson::kObjectType);
      for (int i = 0; i < struct_type->num_fields(); ++i) {
        auto field_name = struct_type->field(i)->name();
        if (field_name != "_ID" && field_name != "_LABEL") {
          auto child_array = struct_array->field(i);
          if (!child_array->IsNull(index)) {
            properties.AddMember(
                rapidjson::Value(field_name.c_str(), allocator),
                arrow_value_to_json(child_array, index, allocator), allocator);
          }
        }
      }
      node_obj.AddMember("properties", properties, allocator);

      std::string element_id = std::to_string(node_id);
      node_obj.AddMember("elementId",
                         rapidjson::Value(element_id.c_str(), allocator),
                         allocator);

      nodes.PushBack(node_obj, allocator);
    }

    // Return reference to vertex
    rapidjson::Value node_ref(rapidjson::kObjectType);
    rapidjson::Value identity(rapidjson::kObjectType);
    identity.AddMember("low", rapidjson::Value(node_id), allocator);
    identity.AddMember("high", rapidjson::Value(0), allocator);
    node_ref.AddMember("identity", identity, allocator);

    rapidjson::Value labels(rapidjson::kArrayType);
    labels.PushBack(rapidjson::Value(label.c_str(), allocator), allocator);
    node_ref.AddMember("labels", labels, allocator);

    rapidjson::Value properties(rapidjson::kObjectType);
    for (int i = 0; i < struct_type->num_fields(); ++i) {
      auto field_name = struct_type->field(i)->name();
      if (field_name != "_ID" && field_name != "_LABEL") {
        auto child_array = struct_array->field(i);
        if (!child_array->IsNull(index)) {
          properties.AddMember(
              rapidjson::Value(field_name.c_str(), allocator),
              arrow_value_to_json(child_array, index, allocator), allocator);
        }
      }
    }
    node_ref.AddMember("properties", properties, allocator);

    std::string element_id = std::to_string(node_id);
    node_ref.AddMember("elementId",
                       rapidjson::Value(element_id.c_str(), allocator),
                       allocator);

    return node_ref;
  }

  // If neither vertex nor edge, return a generic object
  rapidjson::Value obj(rapidjson::kObjectType);
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    auto field_name = struct_type->field(i)->name();
    auto child_array = struct_array->field(i);
    if (!child_array->IsNull(index)) {
      obj.AddMember(rapidjson::Value(field_name.c_str(), allocator),
                    arrow_value_to_json(child_array, index, allocator),
                    allocator);
    }
  }
  return obj;
}

rapidjson::Value arrow_value_to_json(
    std::shared_ptr<arrow::Array> column, int64_t index,
    rapidjson::Document::AllocatorType& allocator) {
  if (column->IsNull(index)) {
    rapidjson::Value null_val;
    null_val.SetNull();
    return null_val;
  }

  switch (column->type()->id()) {
  case arrow::Type::BOOL: {
    return arrow_numeric_to_json<arrow::BooleanArray>(column, index);
  }
  case arrow::Type::INT32: {
    return arrow_numeric_to_json<arrow::Int32Array>(column, index);
  }
  case arrow::Type::UINT32: {
    return arrow_numeric_to_json<arrow::UInt32Array>(column, index);
  }
  case arrow::Type::INT64: {
    return arrow_numeric_to_json<arrow::Int64Array>(column, index);
  }
  case arrow::Type::UINT64: {
    return arrow_numeric_to_json<arrow::UInt64Array>(column, index);
  }
  case arrow::Type::FLOAT: {
    return arrow_numeric_to_json<arrow::FloatArray>(column, index);
  }
  case arrow::Type::DOUBLE: {
    return arrow_numeric_to_json<arrow::DoubleArray>(column, index);
  }
  case arrow::Type::STRING: {
    return arrow_string_to_json<arrow::StringArray>(column, index, allocator);
  }
  case arrow::Type::LARGE_STRING: {
    return arrow_string_to_json<arrow::LargeStringArray>(column, index,
                                                         allocator);
  }
  case arrow::Type::DATE32: {
    auto date32_array = std::static_pointer_cast<arrow::Date32Array>(column);
    int32_t days = date32_array->Value(index);
    Date day;
    day.from_num_days(days);
    std::stringstream ss;
    ss << day.year() << "-" << std::setfill('0') << std::setw(2) << day.month()
       << "-" << std::setfill('0') << std::setw(2) << day.day();
    return rapidjson::Value(ss.str().c_str(), allocator);
  }
  case arrow::Type::DATE64: {
    auto date64_array = std::static_pointer_cast<arrow::Date64Array>(column);
    int64_t milliseconds = date64_array->Value(index);
    std::time_t seconds = milliseconds / 1000;
    std::tm* tm_info = std::gmtime(&seconds);
    char buffer[20];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", tm_info);
    return rapidjson::Value(buffer, allocator);
  }
  case arrow::Type::TIMESTAMP: {
    auto timestamp_array =
        std::static_pointer_cast<arrow::TimestampArray>(column);
    auto type =
        std::static_pointer_cast<arrow::TimestampType>(timestamp_array->type());
    int64_t milliseconds;
    switch (type->unit()) {
    case arrow::TimeUnit::MILLI:
      milliseconds = timestamp_array->Value(index);
      break;
    case arrow::TimeUnit::MICRO:
      milliseconds = timestamp_array->Value(index) / 1000;
      break;
    case arrow::TimeUnit::NANO:
      milliseconds = timestamp_array->Value(index) / 1000000;
      break;
    case arrow::TimeUnit::SECOND:
      milliseconds = timestamp_array->Value(index) * 1000;
      break;
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported TimeUnit");
    }
    // Convert to ISO 8601 string format
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
  }
  case arrow::Type::LIST: {
    auto list_array = std::static_pointer_cast<arrow::ListArray>(column);
    auto value_array = list_array->values();
    int32_t start = list_array->value_offset(index);
    int32_t end = list_array->value_offset(index + 1);
    rapidjson::Value list_val(rapidjson::kArrayType);
    for (int32_t i = start; i < end; ++i) {
      list_val.PushBack(arrow_value_to_json(value_array, i, allocator),
                        allocator);
    }
    return list_val;
  }
  case arrow::Type::STRUCT: {
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(column);
    auto struct_type =
        std::static_pointer_cast<arrow::StructType>(struct_array->type());
    rapidjson::Value obj(rapidjson::kObjectType);
    for (int i = 0; i < struct_type->num_fields(); ++i) {
      auto field_name = struct_type->field(i)->name();
      auto child_array = struct_array->field(i);
      if (!child_array->IsNull(index)) {
        obj.AddMember(rapidjson::Value(field_name.c_str(), allocator),
                      arrow_value_to_json(child_array, index, allocator),
                      allocator);
      }
    }
    return obj;
  }
  case arrow::Type::NA: {
    rapidjson::Value null_val;
    null_val.SetNull();
    return null_val;
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported Arrow type: " +
                                  column->type()->ToString());
  }
  }
}

// Helper function to convert Arrow Path STRUCT to Bolt path format
rapidjson::Value arrow_path_to_bolt_path(
    std::shared_ptr<arrow::StructArray> path_struct, int64_t index,
    rapidjson::Value& nodes, rapidjson::Value& edges,
    std::unordered_set<int64_t>& node_ids,
    std::unordered_set<int64_t>& edge_ids,
    rapidjson::Document::AllocatorType& allocator) {
  auto struct_type =
      std::static_pointer_cast<arrow::StructType>(path_struct->type());

  // Find the vertices and edges fields
  int vertices_idx = -1, edges_idx = -1;
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    auto field_name = struct_type->field(i)->name();
    if (field_name == "vertices") {
      vertices_idx = i;
    } else if (field_name == "edges") {
      edges_idx = i;
    }
  }

  rapidjson::Value segments(rapidjson::kArrayType);
  rapidjson::Value start_node(rapidjson::kNullType);
  rapidjson::Value end_node(rapidjson::kNullType);
  int path_length = 0;

  // Process vertices
  std::vector<rapidjson::Value> vertex_refs;
  if (vertices_idx >= 0) {
    auto vertices_list = std::static_pointer_cast<arrow::ListArray>(
        path_struct->field(vertices_idx));
    auto vertex_values = vertices_list->values();
    auto vertex_struct_array =
        std::static_pointer_cast<arrow::StructArray>(vertex_values);
    auto vertex_struct_type = std::static_pointer_cast<arrow::StructType>(
        vertex_struct_array->type());

    int32_t start_offset = vertices_list->value_offset(index);
    int32_t end_offset = vertices_list->value_offset(index + 1);

    // Find label and id field indices
    int label_idx = -1, id_idx = -1;
    for (int i = 0; i < vertex_struct_type->num_fields(); ++i) {
      auto field_name = vertex_struct_type->field(i)->name();
      if (field_name == "label") {
        label_idx = i;
      } else if (field_name == "id") {
        id_idx = i;
      }
    }

    auto label_array = std::static_pointer_cast<arrow::StringArray>(
        vertex_struct_array->field(label_idx));
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(
        vertex_struct_array->field(id_idx));

    for (int32_t i = start_offset; i < end_offset; ++i) {
      int64_t node_id = id_array->Value(i);
      std::string_view label = label_array->GetView(i);

      // Add to nodes collection if not already present
      if (node_ids.find(node_id) == node_ids.end()) {
        node_ids.insert(node_id);

        rapidjson::Value node_obj(rapidjson::kObjectType);
        rapidjson::Value identity(rapidjson::kObjectType);
        identity.AddMember("low", rapidjson::Value(node_id), allocator);
        identity.AddMember("high", rapidjson::Value(0), allocator);
        node_obj.AddMember("identity", identity, allocator);

        rapidjson::Value labels(rapidjson::kArrayType);
        labels.PushBack(
            rapidjson::Value(label.data(),
                             static_cast<rapidjson::SizeType>(label.size()),
                             allocator),
            allocator);
        node_obj.AddMember("labels", labels, allocator);

        rapidjson::Value properties(rapidjson::kObjectType);
        node_obj.AddMember("properties", properties, allocator);

        std::string element_id = std::to_string(node_id);
        node_obj.AddMember("elementId",
                           rapidjson::Value(element_id.c_str(), allocator),
                           allocator);

        nodes.PushBack(node_obj, allocator);
      }

      // Create vertex reference
      rapidjson::Value node_ref(rapidjson::kObjectType);
      rapidjson::Value identity(rapidjson::kObjectType);
      identity.AddMember("low", rapidjson::Value(node_id), allocator);
      identity.AddMember("high", rapidjson::Value(0), allocator);
      node_ref.AddMember("identity", identity, allocator);

      rapidjson::Value labels(rapidjson::kArrayType);
      labels.PushBack(
          rapidjson::Value(label.data(),
                           static_cast<rapidjson::SizeType>(label.size()),
                           allocator),
          allocator);
      node_ref.AddMember("labels", labels, allocator);

      rapidjson::Value properties(rapidjson::kObjectType);
      node_ref.AddMember("properties", properties, allocator);

      std::string element_id = std::to_string(node_id);
      node_ref.AddMember("elementId",
                         rapidjson::Value(element_id.c_str(), allocator),
                         allocator);

      vertex_refs.push_back(std::move(node_ref));
    }

    if (!vertex_refs.empty()) {
      start_node = rapidjson::Value(vertex_refs.front(), allocator);
      end_node = rapidjson::Value(vertex_refs.back(), allocator);
    }
  }

  // Process edges and create segments
  if (edges_idx >= 0 && vertex_refs.size() > 1) {
    auto edges_list = std::static_pointer_cast<arrow::ListArray>(
        path_struct->field(edges_idx));
    auto edge_values = edges_list->values();
    auto edge_struct_array =
        std::static_pointer_cast<arrow::StructArray>(edge_values);
    auto edge_struct_type =
        std::static_pointer_cast<arrow::StructType>(edge_struct_array->type());

    int32_t start_offset = edges_list->value_offset(index);
    int32_t end_offset = edges_list->value_offset(index + 1);

    // Find label and direction field indices
    int label_idx = -1;
    for (int i = 0; i < edge_struct_type->num_fields(); ++i) {
      auto field_name = edge_struct_type->field(i)->name();
      if (field_name == "label") {
        label_idx = i;
      }
    }

    auto label_array = std::static_pointer_cast<arrow::StringArray>(
        edge_struct_array->field(label_idx));

    for (int32_t i = start_offset;
         i < end_offset && i - start_offset < vertex_refs.size() - 1; ++i) {
      std::string_view edge_label = label_array->GetView(i);
      int edge_idx = i - start_offset;

      // Create a segment with start node, relationship, and end node
      rapidjson::Value segment(rapidjson::kObjectType);
      segment.AddMember("start",
                        rapidjson::Value(vertex_refs[edge_idx], allocator),
                        allocator);

      // Create relationship object (simplified without actual edge ID)
      rapidjson::Value relationship(rapidjson::kObjectType);
      rapidjson::Value rel_identity(rapidjson::kObjectType);
      // Use a synthetic ID based on position
      int64_t synthetic_edge_id = -(i + 1);
      rel_identity.AddMember("low", rapidjson::Value(synthetic_edge_id),
                             allocator);
      rel_identity.AddMember("high", rapidjson::Value(0), allocator);
      relationship.AddMember("identity", rel_identity, allocator);

      relationship.AddMember(
          "type",
          rapidjson::Value(edge_label.data(),
                           static_cast<rapidjson::SizeType>(edge_label.size()),
                           allocator),
          allocator);

      // Start and end should reference the actual vertex IDs
      rapidjson::Value rel_start(rapidjson::kObjectType);
      rel_start.AddMember(
          "low",
          rapidjson::Value(vertex_refs[edge_idx]["identity"]["low"].GetInt64()),
          allocator);
      rel_start.AddMember("high", rapidjson::Value(0), allocator);
      relationship.AddMember("start", rel_start, allocator);

      rapidjson::Value rel_end(rapidjson::kObjectType);
      rel_end.AddMember(
          "low",
          rapidjson::Value(
              vertex_refs[edge_idx + 1]["identity"]["low"].GetInt64()),
          allocator);
      rel_end.AddMember("high", rapidjson::Value(0), allocator);
      relationship.AddMember("end", rel_end, allocator);

      rapidjson::Value rel_properties(rapidjson::kObjectType);
      relationship.AddMember("properties", rel_properties, allocator);

      std::string rel_element_id = std::to_string(synthetic_edge_id);
      relationship.AddMember(
          "elementId", rapidjson::Value(rel_element_id.c_str(), allocator),
          allocator);

      segment.AddMember("relationship", relationship, allocator);
      segment.AddMember("end",
                        rapidjson::Value(vertex_refs[edge_idx + 1], allocator),
                        allocator);

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

std::string arrow_table_to_bolt_response(
    const arrow::Table& table, const std::vector<std::string>& column_names) {
  rapidjson::Document response;
  response.SetObject();
  auto& allocator = response.GetAllocator();

  rapidjson::Value nodes(rapidjson::kArrayType);
  rapidjson::Value edges(rapidjson::kArrayType);
  rapidjson::Value records(rapidjson::kArrayType);
  rapidjson::Value table_json(rapidjson::kArrayType);

  std::unordered_set<int64_t> node_ids;
  std::unordered_set<int64_t> edge_ids;

  // Get the number of rows and columns
  int64_t num_rows = table.num_rows();
  int num_columns = table.num_columns();

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
        column_name = table.schema()->field(col)->name();
      }

      keys.PushBack(rapidjson::Value(column_name.c_str(), allocator),
                    allocator);

      auto chunked_array = table.column(col);
      if (chunked_array->num_chunks() != 1) {
        THROW_INTERNAL_EXCEPTION("Expected single chunk per column, got " +
                                 std::to_string(chunked_array->num_chunks()));
      }
      auto column_array = chunked_array->chunk(0);

      rapidjson::Value field_value;

      // Handle SPARSE_UNION type (for Vertex/Edge)
      if (column_array->type()->id() == arrow::Type::SPARSE_UNION) {
        auto union_array =
            std::static_pointer_cast<arrow::UnionArray>(column_array);
        int8_t type_code = union_array->type_code(row);
        auto union_type =
            std::static_pointer_cast<arrow::UnionType>(union_array->type());
        std::string label = union_type->field(type_code)->name();
        auto child_array = union_array->field(type_code);

        if (child_array->type()->id() == arrow::Type::STRUCT) {
          auto struct_array =
              std::static_pointer_cast<arrow::StructArray>(child_array);
          field_value = arrow_struct_to_bolt_element(struct_array, row, label,
                                                     nodes, edges, node_ids,
                                                     edge_ids, allocator);
        } else {
          field_value = arrow_value_to_json(child_array, row, allocator);
        }
      } else {
        // Check if this is a STRUCT that might be a Path
        if (column_array->type()->id() == arrow::Type::STRUCT) {
          auto struct_array =
              std::static_pointer_cast<arrow::StructArray>(column_array);
          auto struct_type =
              std::static_pointer_cast<arrow::StructType>(struct_array->type());

          // Check if it's a path structure
          bool has_vertices = false;
          bool has_edges_field = false;
          for (int i = 0; i < struct_type->num_fields(); ++i) {
            auto field_name = struct_type->field(i)->name();
            if (field_name == "vertices") {
              has_vertices = true;
            } else if (field_name == "edges") {
              has_edges_field = true;
            }
          }

          if (has_vertices && has_edges_field) {
            // This is a path, process it specially
            field_value = arrow_path_to_bolt_path(
                struct_array, row, nodes, edges, node_ids, edge_ids, allocator);
          } else {
            field_value = arrow_value_to_json(column_array, row, allocator);
          }
        } else {
          field_value = arrow_value_to_json(column_array, row, allocator);
        }
      }

      fields.PushBack(rapidjson::Value(field_value, allocator), allocator);
      field_lookup.AddMember(rapidjson::Value(column_name.c_str(), allocator),
                             rapidjson::Value(col), allocator);

      // Add to table
      if (column_array->IsNull(row)) {
        table_row.AddMember(rapidjson::Value(column_name.c_str(), allocator),
                            rapidjson::Value().SetNull(), allocator);
      } else {
        rapidjson::Value table_value;
        if (column_array->type()->id() == arrow::Type::SPARSE_UNION) {
          auto union_array =
              std::static_pointer_cast<arrow::UnionArray>(column_array);
          int8_t type_code = union_array->type_code(row);
          auto child_array = union_array->field(type_code);
          table_value = arrow_value_to_json(child_array, row, allocator);
        } else {
          table_value = arrow_value_to_json(column_array, row, allocator);
        }
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

std::string arrow_table_to_bolt_response(
    const std::shared_ptr<arrow::Table>& table) {
  if (!table) {
    return "{}";
  }
  std::vector<std::string> column_names;
  for (int i = 0; i < table->num_columns(); ++i) {
    column_names.push_back(table->schema()->field(i)->name());
  }

  return arrow_table_to_bolt_response(*table, column_names);
}

}  // namespace neug
