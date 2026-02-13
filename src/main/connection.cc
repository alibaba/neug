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
  is_closed_.store(true);
  // Necessary cleanup could be done here.
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const execution::ParamsMap& parameters) {
  LOG(INFO) << "Query: " << query_string;
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
  LOG(INFO) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  return query_processor_->execute(query_string, access_mode, parameters_json);
}

}  // namespace neug
