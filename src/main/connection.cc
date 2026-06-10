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

#include "neug/main/connection.h"

#include "neug/main/neug_db.h"
#include "neug/main/query_request.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

std::string Connection::GetSchema() const {
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot get schema.";
    THROW_RUNTIME_ERROR("Connection is closed, cannot get schema.");
  }
  auto yaml = graph_.schema().to_yaml();
  return neug::get_json_string_from_yaml(yaml.value()).value();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";

  auto temp_edges = graph_.schema().get_temporary_edge_triplet_keys();
  for (auto key : temp_edges) {
    auto [src, dst, edge] = graph_.schema().parse_edge_label(key);
    try {
      graph_.DeleteEdgeType(src, dst, edge);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp edge: " << e.what();
    }
  }

  auto temp_vertices = graph_.schema().get_temporary_vertex_labels();
  for (auto label : temp_vertices) {
    try {
      graph_.DeleteVertexType(label);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp vertex: " << e.what();
    }
  }

  if (!temp_edges.empty() || !temp_vertices.empty()) {
    query_processor_->clear_cache();
  }

  is_closed_.store(true);
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const execution::ParamsMap& parameters) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  return query_processor_->execute(query_string, access_mode, parameters);
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const rapidjson::Value& parameters_json) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  return query_processor_->execute(query_string, access_mode, parameters_json);
}

}  // namespace neug
