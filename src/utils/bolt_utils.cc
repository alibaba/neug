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

}  // namespace neug
