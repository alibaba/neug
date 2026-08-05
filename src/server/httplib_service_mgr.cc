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

#include "neug/server/httplib_service_mgr.h"

#include <glog/logging.h>

#include <algorithm>
#include <sstream>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/plan/error.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

int32_t status_code_to_http_code(neug::StatusCode code) {
  switch (code) {
  case neug::StatusCode::OK:
    return 200;
  case neug::StatusCode::ERR_PERMISSION:
    return 500;
  case neug::StatusCode::ERR_DATABASE_LOCKED:
    return 500;
  case neug::StatusCode::ERR_NOT_SUPPORTED:
    return 501;
  case neug::StatusCode::ERR_NOT_IMPLEMENTED:
    return 501;
  case neug::StatusCode::ERR_QUERY_SYNTAX:
    return 400;
  case neug::StatusCode::ERR_NOT_INITIALIZED:
    return 500;
  case neug::StatusCode::ERR_QUERY_EXECUTION:
    return 500;
  case neug::StatusCode::ERR_INTERNAL_ERROR:
    return 500;
  case neug::StatusCode::ERR_NOT_FOUND:
    return 500;
  case neug::StatusCode::ERR_NO_CHECKPOINT:
    return 404;
  case neug::StatusCode::ERR_INVALID_ARGUMENT:
    return 400;
  case neug::StatusCode::ERR_COMPILATION:
    return 500;
  default:
    return 500;
  }
}

HttplibServiceManager::HttplibServiceManager(
    neug::NeugDB& neug_db, TpExecutionSlotPool& execution_slot_pool)
    : neug_db_(neug_db),
      execution_slot_pool_(execution_slot_pool),
      planner_(neug_db_.GetPlanner()) {
  server_ = std::make_unique<httplib::Server>();
}

HttplibServiceManager::~HttplibServiceManager() {
  if (server_ && running_.load(std::memory_order_relaxed)) {
    Stop();
  }
}

void HttplibServiceManager::Init(const ServiceConfig& config) {
  service_config_ = config;

  auto* svr = server_.get();

  // POST /cypher — Execute Cypher queries
  svr->Post("/cypher", [this](const httplib::Request& req,
                              httplib::Response& res) {
    if (req.body.empty()) {
      LOG(ERROR) << "Query request is empty";
      res.status = 400;
      res.set_content("Query request is empty", "text/plain");
      return;
    }

    const std::string& query_request = req.body;

    auto slot_lease = execution_slot_pool_.AcquireExecutionSlot();
    auto result = slot_lease->ExecuteTransactionalRequest(query_request);

    if (result) {
      res.status = 200;
      const auto& results = result.value();
      res.set_content(results, "application/json");
    } else {
      const auto& status = result.error();
      LOG(ERROR) << "Query failed: " << status.ToString();
      res.status = status_code_to_http_code(status.error_code());
      res.set_content(status.ToString(), "text/plain");
    }
    VLOG(10) << "Query executed successfully, updating planner's schema and "
                "statistics";
  });

  // GET /schema — Retrieve graph schema
  svr->Get("/schema", [this](const httplib::Request&,
                               httplib::Response& res) {
    auto ret = GetSchemaImpl();
    if (ret) {
      res.status = 200;
      res.set_content(ret.value(), "application/json");
    } else {
      const auto& error = ret.error();
      LOG(ERROR) << "Error " << error.ToString();
      res.status = status_code_to_http_code(error.error_code());
      res.set_content(error.ToString(), "text/plain");
    }
  });

  // GET /service_status — Check service status
  svr->Get("/service_status", [this](const httplib::Request&,
                                       httplib::Response& res) {
    auto ret = GetServiceStatusImpl();
    if (ret) {
      res.status = 200;
      res.set_content(ret.value(), "application/json");
    } else {
      const auto& error = ret.error();
      LOG(ERROR) << "Error " << error.ToString();
      res.status = status_code_to_http_code(error.error_code());
      res.set_content(error.ToString(), "text/plain");
    }
  });
}

std::string HttplibServiceManager::Start() {
  LOG(INFO) << "Starting httplib server";
  std::string ip_port = service_config_.host_str + ":" +
                        std::to_string(service_config_.query_port);
  uint32_t num_threads = resolve_num_threads();
  LOG(INFO) << "Service config: db_max_thread_num="
            << neug_db_.config().max_thread_num
            << ", configured_thread_num=" << service_config_.thread_num
            << ", resolved_num_threads=" << num_threads;

  // httplib's new_task_queue returns a raw TaskQueue* (ownership transferred
  // to the caller via unique_ptr in the framework). ThreadPool is the default
  // concrete type.
  server_->new_task_queue = [num_threads]() {
    return new httplib::ThreadPool(num_threads);
  };

  running_.store(true, std::memory_order_relaxed);

  std::stringstream ss;
  ss << "http://" << service_config_.host_str << ":"
     << service_config_.query_port;
  return ss.str();
}

void HttplibServiceManager::Stop() {
  LOG(INFO) << "Stopping httplib server";
  if (server_) {
    server_->stop();
  }
  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Httplib server stopped";
}

void HttplibServiceManager::RunAndWaitForExit() {
  LOG(INFO) << "Httplib server is running, waiting for exit...";
  std::string ip_port = service_config_.host_str + ":" +
                        std::to_string(service_config_.query_port);

  uint32_t num_threads = resolve_num_threads();
  LOG(INFO) << "Service config: db_max_thread_num="
            << neug_db_.config().max_thread_num
            << ", configured_thread_num=" << service_config_.thread_num
            << ", resolved_num_threads=" << num_threads;

  server_->new_task_queue = [num_threads]() {
    return new httplib::ThreadPool(num_threads);
  };

  if (!server_->listen(service_config_.host_str.c_str(),
                       static_cast<int>(service_config_.query_port))) {
    THROW_RUNTIME_ERROR("Failed to start httplib server on " + ip_port);
  }

  running_.store(true, std::memory_order_relaxed);

  // listen() blocks until the server is stopped via stop()
  // When stop() is called, listen() returns and we fall through
  running_.store(false, std::memory_order_relaxed);
}

bool HttplibServiceManager::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

neug::result<std::string> HttplibServiceManager::GetSchemaImpl() {
  const auto& schema = neug_db_.schema();
  auto yaml = schema.to_yaml();
  if (!yaml) {
    RETURN_ERROR(yaml.error());
  }
  return neug::get_json_string_from_yaml(yaml.value());
}

neug::result<std::string> HttplibServiceManager::GetServiceStatusImpl() {
  return std::string("{\"status\": \"OK\", \"version\": \"" NEUG_VERSION "\"}");
}

uint32_t HttplibServiceManager::resolve_num_threads() const {
  if (service_config_.thread_num != 0) {
    return service_config_.thread_num;
  }
  const auto max_thread_num = neug_db_.config().max_thread_num;
  if (max_thread_num <= 0) {
    return 1;
  }
  return static_cast<uint32_t>(max_thread_num);
}

}  // namespace neug
