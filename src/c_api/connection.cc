/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "c_api_internal.h"

using namespace neug::capi;

neug_status neug_db_connect(neug_database* db, neug_connection** output,
                            neug_error** error) {
  return Guard(error, [&] {
    NewOutput(output);
    Require(db && db->impl, "Database is NULL");
    auto connection = std::make_unique<neug_connection>();
    connection->database = db->impl;
    connection->impl = db->impl->Connect();
    *output = connection.release();
  });
}
neug_status neug_connection_is_closed(const neug_connection* connection,
                                      uint8_t* output, neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    Require(connection && connection->impl, "Connection is NULL");
    out = connection->impl->IsClosed();
  });
}
neug_status neug_connection_destroy(neug_connection** connection,
                                    neug_error** error) {
  return Guard(error, [&] {
    Require(connection != nullptr, "Connection pointer is NULL");
    if (*connection) {
      (*connection)->impl->Close();
      delete *connection;
      *connection = nullptr;
    }
  });
}
neug_status neug_connection_begin(neug_connection* connection,
                                  uint8_t read_only, neug_error** error) {
  return Guard(error, [&] {
    Require(read_only <= 1, "read_only must be 0 or 1");
    CheckStatus(ConnectionImpl(connection)
                    .BeginTransaction(read_only
                                          ? neug::TransactionMode::kReadOnly
                                          : neug::TransactionMode::kReadWrite));
  });
}
neug_status neug_connection_commit(neug_connection* connection,
                                   neug_error** error) {
  return Guard(error,
               [&] { CheckStatus(ConnectionImpl(connection).Commit()); });
}
neug_status neug_connection_rollback(neug_connection* connection,
                                     neug_error** error) {
  return Guard(error,
               [&] { CheckStatus(ConnectionImpl(connection).Rollback()); });
}
neug_status neug_connection_has_transaction(const neug_connection* connection,
                                            uint8_t* output,
                                            neug_error** error) {
  return Guard(error, [&] {
    auto& out = Output(output);
    out = ConnectionImpl(connection).HasActiveTransaction();
  });
}
neug_status neug_query(neug_connection* connection, neug_string_view query,
                       neug_string_view parameters_json, neug_result** output,
                       neug_error** error) {
  return Guard(error, [&] {
    NewOutput(output);
    auto& impl = ConnectionImpl(connection);
    auto query_text = Text(query, false);
    Require(!query_text.empty(), "Query is empty");
    rapidjson::Document parameters(rapidjson::kObjectType);
    if (parameters_json.size) {
      ParseJson(parameters, parameters_json);
      Require(parameters.IsObject(), "Query parameters must be a JSON object");
    }
    // Allocate the public holder before executing a potentially mutating query.
    auto result = std::make_unique<neug_result>();
    auto response = impl.Query(query_text, "", parameters);
    if (!response) {
      CheckStatus(response.error());
    }
    result->response = response.value().shared_response();
    *output = result.release();
  });
}
