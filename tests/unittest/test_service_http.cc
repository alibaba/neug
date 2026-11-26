/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Keep Winsock ahead of Windows headers pulled in by project dependencies.
#include "httplib.h"

#include <gtest/gtest.h>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "../../src/server/service_http_common.h"
#include "../../src/server/tp_service_runtime.h"
#include "neug/main/connection.h"
#include "neug/main/execution_slot.h"
#include "neug/main/neug_db.h"
#include "neug/transaction/insert_capacity.h"
#include "neug/utils/exception/exception.h"
#ifndef _WIN32
#include "../../src/server/brpc_transport.h"
#else
#include "neug/server/httplib_transport.h"
#endif

namespace neug {
namespace test {
namespace {

constexpr const char* kQuery =
    R"({"query":"RETURN 1", "access_mode":"read", "parameters":{}})";

TEST(ServiceHttp, ExecutionExceptionsBecomeErrorResults) {
  auto standard = service_http::ParseAndExecuteQuery(
      kQuery, [](const QueryRequest&) -> result<QueryResult> {
        throw std::runtime_error("injected execution failure");
      });
  ASSERT_FALSE(standard);
  EXPECT_EQ(standard.error().error_code(), StatusCode::ERR_QUERY_EXECUTION);
  EXPECT_NE(standard.error().ToString().find("injected execution failure"),
            std::string::npos);

  auto unknown = service_http::ParseAndExecuteQuery(
      kQuery, [](const QueryRequest&) -> result<QueryResult> { throw 42; });
  ASSERT_FALSE(unknown);
  EXPECT_EQ(unknown.error().error_code(), StatusCode::ERR_INTERNAL_ERROR);

  bool executed = false;
  auto invalid = service_http::ParseAndExecuteQuery(
      "not JSON", [&](const QueryRequest&) -> result<QueryResult> {
        executed = true;
        return tl::unexpected(Status::RuntimeError("must not execute"));
      });
  EXPECT_FALSE(invalid);
  EXPECT_FALSE(executed);

  auto expected_error = service_http::ParseAndExecuteQuery(
      kQuery, [](const QueryRequest&) -> result<QueryResult> {
        return tl::unexpected(Status(StatusCode::ERR_NOT_FOUND, "missing"));
      });
  ASSERT_FALSE(expected_error);
  EXPECT_EQ(expected_error.error().error_code(), StatusCode::ERR_NOT_FOUND);
}

TEST(ServiceHttp, ExecutionExceptionReleasesSlotAndAdmission) {
  NeugDBConfig db_config(":memory:", 1);
  db_config.checkpoint_on_close = false;
  NeugDB db;
  ASSERT_TRUE(db.Open(db_config));
  auto connection = db.Connect();
  ASSERT_TRUE(
      connection->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                        "INT64, PRIMARY KEY(id))"));
  connection->Close();
  ServiceConfig config;
  config.thread_num = 1;
  config.auto_compaction = false;
  TpServiceRuntime runtime(db, config);
  struct ResetInjection {
    ~ResetInjection() {
      SetInsertRetryHookForTest({});
      SetInsertReservationFailuresForTest(0);
    }
  } reset;
  SetInsertReservationFailuresForTest(1);
  SetInsertRetryHookForTest([](bool) {
    THROW_STORAGE_EXCEPTION("injected service storage failure");
  });
  const auto execute = [&](const QueryRequest& request) {
    return runtime.ExecuteQuery(request);
  };
  const auto label = db.schema().get_vertex_label_id("person");
  const auto before = db.graph().get_vertex_table(label).Size();
  const auto insert = RequestSerializer::SerializeRequest(
      "CREATE (:person {id:100, name:'exception', age:20})", "insert", {});
  // The existing fault hooks are thread-local, so execute on this thread.
  auto failed = service_http::ParseAndExecuteQuery(insert, execute);
  ASSERT_FALSE(failed);
  EXPECT_NE(failed.error().ToString().find("injected service storage failure"),
            std::string::npos);
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before);
  SetInsertRetryHookForTest({});
  SetInsertReservationFailuresForTest(0);

  // A single slot makes a leak immediately visible. An update also needs the
  // failed insert's admission/timestamp to have been released on unwinding.
  auto next = service_http::ParseAndExecuteQuery(
      RequestSerializer::SerializeRequest(
          "CREATE (:person {id:100, name:'recovered', age:20})", "update", {}),
      execute);
  ASSERT_TRUE(next) << next.error().ToString();
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before + 1);
}

class ThrowingOperations final : public ITpOperations {
 public:
  std::atomic<bool> fail{true};
  bool unknown = false;

  void MaybeThrow() const {
    if (fail.load()) {
      if (unknown) {
        throw 42;
      }
      throw std::runtime_error("injected service failure");
    }
  }
  result<QueryResult> ExecuteQuery(const QueryRequest&) override {
    MaybeThrow();
    return tl::unexpected(Status::RuntimeError("unused"));
  }
  result<std::string> GetSchema() override {
    MaybeThrow();
    return std::string("{}");
  }
  result<std::string> GetServiceStatus() override {
    MaybeThrow();
    return std::string("{\"status\":\"OK\"}");
  }
  result<ServiceTransactionInfo> BeginTransaction(TransactionMode) override {
    MaybeThrow();
    return ServiceTransactionInfo{"tx", std::nullopt};
  }
  result<QueryResult> ExecuteInTransaction(
      std::string_view, const QueryRequest& request) override {
    return ExecuteQuery(request);
  }
  Status CommitTransaction(std::string_view) override {
    MaybeThrow();
    return Status::OK();
  }
  Status RollbackTransaction(std::string_view) override {
    MaybeThrow();
    return Status::OK();
  }
};

class ServiceHttpExceptions : public ::testing::TestWithParam<bool> {};

TEST_P(ServiceHttpExceptions, EveryEndpointReturns500AndKeepsServing) {
  ThrowingOperations operations;
  operations.unknown = GetParam();
#ifndef _WIN32
  BrpcTransport transport(operations, "127.0.0.1", 0, 1);
#else
  HttplibTransport transport(operations, "127.0.0.1", 0, 1);
#endif
  const auto endpoint = transport.Start();
  httplib::Client client(endpoint);
  client.set_read_timeout(5, 0);
  client.set_connection_timeout(5, 0);

  struct Request {
    const char* path;
    const char* body;
  };
  const Request requests[] = {
      {"/cypher", kQuery},
      {"/schema", nullptr},
      {"/service_status", nullptr},
      {"/transactions", R"({"mode":"read_write"})"},
      {"/transactions/tx/query", kQuery},
      {"/transactions/tx/commit", ""},
      {"/transactions/tx/rollback", ""},
  };
  for (const auto& request : requests) {
    SCOPED_TRACE(request.path);
    operations.fail = true;
    auto response = request.body ? client.Post(request.path, request.body,
                                               "application/json")
                                 : client.Get(request.path);
    ASSERT_TRUE(response) << response.error();
    EXPECT_EQ(response->status, 500);
    EXPECT_NE(response->body.find(operations.unknown
                                      ? "Unhandled service request exception"
                                      : "injected service failure"),
              std::string::npos);

    operations.fail = false;
    auto health = client.Get("/service_status");
    ASSERT_TRUE(health);
    EXPECT_EQ(health->status, 200);
    EXPECT_NE(health->body.find("OK"), std::string::npos);
    // In particular, a throwing commit/rollback must not kill or strand the
    // dedicated httplib control worker; subsequent control requests finish.
    auto commit =
        client.Post("/transactions/tx/commit", "", "application/json");
    ASSERT_TRUE(commit);
    EXPECT_EQ(commit->status, 200);
  }
}

INSTANTIATE_TEST_SUITE_P(StandardAndUnknown, ServiceHttpExceptions,
                         ::testing::Bool());

}  // namespace
}  // namespace test
}  // namespace neug
