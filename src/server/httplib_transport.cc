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
#include "neug/server/httplib_transport.h"

#include <glog/logging.h>

#include <chrono>
#include <utility>

#include "neug/server/service_config.h"
#include "neug/server/tp_operations.h"
#include "neug/utils/exception/exception.h"
#include "service_http_common.h"

namespace neug {

HttplibControlExecutor::HttplibControlExecutor() {
  // Start the worker in the constructor body: members initialize in
  // declaration order and worker_ is declared first, so a thread started in
  // the initializer list could enter Loop() before mutex_, cv_, tasks_ and
  // stopped_ are constructed.
  worker_ = std::thread([this]() { Loop(); });
}

HttplibControlExecutor::~HttplibControlExecutor() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
  }
  cv_.notify_all();
  worker_.join();
}

void HttplibControlExecutor::Execute(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(std::move(task));
  }
  cv_.notify_one();
}

void HttplibControlExecutor::Loop() {
  for (;;) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this]() { return stopped_ || !tasks_.empty(); });
      if (tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }
    task();
  }
}

std::unique_ptr<IServiceTransport> CreateDefaultServiceTransport(
    ITpOperations& service, const ServiceConfig& config,
    int database_thread_num) {
  const auto thread_num =
      config.thread_num != 0
          ? config.thread_num
          : static_cast<uint32_t>(database_thread_num > 0 ? database_thread_num
                                                          : 1);
  return std::make_unique<HttplibTransport>(service, config.host_str,
                                            config.query_port, thread_num);
}

namespace {

void SendQueryResponse(httplib::Response& res, result<std::string>&& response) {
  if (response) {
    res.status = 200;
    // QueryResult::Serialize() emits protobuf bytes, not JSON; label them
    // accordingly so content-type-driven clients pick the right decoder.
    // Schema, status, and begin responses stay JSON.
    res.set_content(std::move(response).value(), "application/x-protobuf");
  } else {
    const auto& status = response.error();
    LOG(ERROR) << "Query failed: " << status.ToString();
    res.status = service_http::status_code_to_http_code(status.error_code());
    res.set_content(status.ToString(), "text/plain");
  }
}

void SendError(httplib::Response& res, const Status& status) {
  LOG(ERROR) << "Request failed: " << status.ToString();
  res.status = service_http::status_code_to_http_code(status.error_code());
  res.set_content(status.ToString(), "text/plain");
}

}  // namespace

HttplibTransport::HttplibTransport(ITpOperations& service, std::string host,
                                   uint32_t port, uint32_t thread_num)
    : tp_operations_(service),
      host_(std::move(host)),
      port_(port),
      thread_num_(thread_num),
      server_(std::make_unique<httplib::Server>()) {
  RegisterHandlers();
}

HttplibTransport::~HttplibTransport() { StopAndJoin(); }

void HttplibTransport::RegisterHandlers() {
  auto* svr = server_.get();

  // Non-POST requests on the transaction endpoints must answer 405 with an
  // Allow header to match the BRPC contract (asserted in test_db_svc.cc);
  // httplib would otherwise fall through to its default 404.
  svr->set_pre_routing_handler([](const httplib::Request& req,
                                  httplib::Response& res) {
    if (req.method != "POST" && (req.path == "/transactions" ||
                                 req.path.rfind("/transactions/", 0) == 0)) {
      res.status = 405;
      res.set_header("Allow", "POST");
      res.set_header("Cache-Control", "no-store");
      return httplib::Server::HandlerResponse::Handled;
    }
    return httplib::Server::HandlerResponse::Unhandled;
  });

  // POST /cypher — Execute Cypher queries.
  svr->Post("/cypher", [this](const httplib::Request& req,
                              httplib::Response& res) {
    if (req.body.empty()) {
      SendError(res, Status(StatusCode::ERR_INVALID_ARGUMENT,
                            "Query request is empty"));
      return;
    }
    SendQueryResponse(res, service_http::ParseAndExecuteQuery(
                               req.body, [this](const auto& request) {
                                 return tp_operations_.ExecuteQuery(request);
                               }));
  });

  // GET /schema — Retrieve graph schema.
  svr->Get("/schema", [this](const httplib::Request&, httplib::Response& res) {
    auto ret = tp_operations_.GetSchema();
    if (ret) {
      res.status = 200;
      res.set_content(std::move(ret).value(), "application/json");
    } else {
      SendError(res, ret.error());
    }
  });

  // GET /service_status — Check service status.
  svr->Get("/service_status",
           [this](const httplib::Request&, httplib::Response& res) {
             auto ret = tp_operations_.GetServiceStatus();
             if (ret) {
               res.status = 200;
               res.set_content(std::move(ret).value(), "application/json");
             } else {
               SendError(res, ret.error());
             }
           });

  // POST /transactions — Begin an explicit transaction.
  svr->Post("/transactions", [this](const httplib::Request& req,
                                    httplib::Response& res) {
    res.set_header("Cache-Control", "no-store");
    auto mode = service_http::ParseTransactionMode(req.body);
    if (!mode) {
      SendError(res, mode.error());
      return;
    }
    result<ServiceTransactionInfo> transaction;
    // Begin runs inline on this httplib worker instead of the control
    // worker: a read-only begin can block waiting for compaction to free
    // snapshots, while compaction waits for in-flight transactions to
    // release theirs via commit/rollback. Occupying the single control
    // worker would queue exactly those release operations behind the begin.
    transaction = tp_operations_.BeginTransaction(mode.value());
    if (!transaction) {
      SendError(res, transaction.error());
      return;
    }
    res.status = 201;
    res.set_content(
        service_http::SerializeBeginResponse(transaction.value(), mode.value()),
        "application/json");
    res.set_header("Location", "/transactions/" + transaction->transaction_id);
  });

  // POST /transactions/{id}/query — Execute inside an explicit transaction.
  svr->Post(R"(/transactions/([^/]+)/query)",
            [this](const httplib::Request& req, httplib::Response& res) {
              res.set_header("Cache-Control", "no-store");
              auto request = RequestParser::ParseFromString(req.body);
              if (!request) {
                SendError(res, request.error());
                return;
              }
              SendQueryResponse(
                  res, service_http::SerializeQueryResult(
                           tp_operations_.ExecuteInTransaction(
                               req.matches[1].str(), request.value())));
            });

  // POST /transactions/{id}/commit|rollback — Finish an explicit transaction.
  svr->Post(R"(/transactions/([^/]+)/commit)",
            [this](const httplib::Request& req, httplib::Response& res) {
              HandleFinishTransaction(req, res, /*commit=*/true);
            });
  svr->Post(R"(/transactions/([^/]+)/rollback)",
            [this](const httplib::Request& req, httplib::Response& res) {
              HandleFinishTransaction(req, res, /*commit=*/false);
            });
}

void HttplibTransport::RunOnControlWorker(std::function<void()> task) {
  // Blocks the calling (httplib worker) thread until the control worker has
  // run `task`. The control worker is independent of the httplib thread pool,
  // so transaction-control work stays executable even when every worker is
  // blocked waiting for an update lease held by an explicit transaction.
  std::mutex done_mutex;
  std::condition_variable done_cv;
  bool done = false;
  control_executor_.Execute([&] {
    task();
    std::lock_guard<std::mutex> lock(done_mutex);
    done = true;
    done_cv.notify_one();
  });
  std::unique_lock<std::mutex> lock(done_mutex);
  done_cv.wait(lock, [&done]() { return done; });
}

void HttplibTransport::HandleFinishTransaction(const httplib::Request& req,
                                               httplib::Response& res,
                                               bool commit) {
  res.set_header("Cache-Control", "no-store");
  // Hold the extracted string for the whole request lifetime:
  // TransactionIdFromPath returns a view into its argument, and
  // req.matches[1].str() is a temporary that would dangle otherwise.
  const std::string transaction_id_str = req.matches[1].str();
  auto transaction_id = service_http::TransactionIdFromPath(transaction_id_str);
  Status status = transaction_id ? service_http::RequireEmptyBody(req.body)
                                 : transaction_id.error();
  if (status.ok()) {
    RunOnControlWorker([&] {
      status = commit
                   ? tp_operations_.CommitTransaction(transaction_id.value())
                   : tp_operations_.RollbackTransaction(transaction_id.value());
    });
  }
  if (status.ok()) {
    res.status = 200;
    res.set_content("", "application/json");
  } else {
    SendError(res, status);
  }
}

std::string HttplibTransport::Start() {
  LOG(INFO) << "Starting httplib server";
  const auto address = host_ + ":" + std::to_string(port_);
  // httplib's new_task_queue returns a raw TaskQueue* (ownership transferred
  // to the server via unique_ptr in the framework). ThreadPool is the default
  // concrete type.
  server_->new_task_queue = [thread_num = thread_num_]() {
    return new httplib::ThreadPool(thread_num);
  };

  int bound_port = static_cast<int>(port_);
  if (bound_port == 0) {
    // Mirror the BRPC service behavior: port zero asks the OS for a free
    // port, and the endpoint returned to callers must carry the port that
    // was actually assigned.
    bound_port = server_->bind_to_any_port(host_.c_str());
    if (bound_port <= 0) {
      THROW_RUNTIME_ERROR("Failed to bind httplib server on " + host_ + ":0");
    }
  } else if (!server_->bind_to_port(host_.c_str(), bound_port)) {
    THROW_RUNTIME_ERROR("Failed to bind httplib server on " + address);
  }
  join_pending_.store(true, std::memory_order_relaxed);
  listen_thread_ = std::thread([this]() { server_->listen_after_bind(); });

  // Wait until the server has actually started listening. listen_after_bind()
  // sets is_running() to true once the listening loop is active.
  constexpr int kMaxWaitRetries = 200;
  constexpr auto kWaitInterval = std::chrono::milliseconds(10);
  for (int i = 0; i < kMaxWaitRetries && !server_->is_running(); ++i) {
    std::this_thread::sleep_for(kWaitInterval);
  }
  if (!server_->is_running()) {
    StopAndJoin();
    THROW_RUNTIME_ERROR("Httplib server failed to start listening on " +
                        address);
  }

  const auto endpoint = "http://" + host_ + ":" + std::to_string(bound_port);
  LOG(INFO) << "httplib server started at " << endpoint;
  return endpoint;
}

void HttplibTransport::StopAccepting() noexcept {
  LOG(INFO) << "Stopping httplib server";
  if (server_) {
    // httplib::Server::stop() is idempotent: it stops accepting new
    // connections and lets in-flight handlers finish.
    server_->stop();
  }
}

void HttplibTransport::Join() noexcept {
  if (join_pending_.exchange(false, std::memory_order_relaxed) &&
      listen_thread_.joinable()) {
    listen_thread_.join();
  }
  LOG(INFO) << "httplib server stopped";
}

}  // namespace neug
