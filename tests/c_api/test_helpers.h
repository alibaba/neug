/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "neug/c/neug.h"

inline neug_string_view S(const char* text) {
  return {text, std::char_traits<char>::length(text)};
}
inline neug_string_view S(const std::string& text) {
  return {text.data(), text.size()};
}
inline std::string Str(neug_string_view text) {
  return std::string(text.data ? text.data : "", text.size);
}
inline ::testing::AssertionResult Ok(neug_status status, neug_error** error) {
  if (status == NEUG_OK)
    return ::testing::AssertionSuccess();
  const auto message = error && *error ? Str(neug_error_message(*error)) : "";
  if (error)
    neug_error_destroy(error);
  return ::testing::AssertionFailure() << status << ": " << message;
}
#define C_OK(call) ASSERT_TRUE(Ok((call), &error))

class CApiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> sequence{0};
    dir = std::filesystem::temp_directory_path() /
          ("neug-c-api-" +
           std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count()) +
           "-" + std::to_string(sequence++));
    neug_db_options_init(&options);
    path = dir.string();
    options.path = S(path);
    C_OK(neug_db_open(&options, &db, &error));
    C_OK(neug_db_connect(db, &connection, &error));
  }
  void TearDown() override {
    neug_error_destroy(&error);
    neug_result_destroy(&result);
    EXPECT_TRUE(Ok(neug_connection_destroy(&connection, &error), &error));
    if (db) {
      EXPECT_TRUE(Ok(neug_db_close(db, &error), &error));
      EXPECT_TRUE(Ok(neug_db_destroy(&db, &error), &error));
    }
    std::filesystem::remove_all(dir);
  }
  void Query(const char* text, const char* parameters = "") {
    neug_result_destroy(&result);
    C_OK(neug_query(connection, S(text), S(parameters), &result, &error));
  }
  neug_value_view Cell(uint64_t row = 0, uint64_t column = 0) {
    neug_value_view value{};
    EXPECT_TRUE(
        Ok(neug_result_get_value(result, row, column, &value, &error), &error));
    return value;
  }
  int64_t Integer(neug_value_view value) {
    int64_t out = 0;
    EXPECT_TRUE(Ok(neug_value_get_int64(&value, &out, &error), &error));
    return out;
  }
  neug_value_view Field(neug_value_view value, const char* name) {
    uint64_t count = 0;
    EXPECT_TRUE(Ok(neug_value_size(&value, &count, &error), &error));
    for (uint64_t i = 0; i < count; ++i) {
      neug_string_view field_name{};
      EXPECT_TRUE(
          Ok(neug_value_field_name(&value, i, &field_name, &error), &error));
      if (Str(field_name) == name) {
        return Child(value, i);
      }
    }
    ADD_FAILURE() << "Field not found: " << name;
    return {};
  }
  neug_value_view Child(neug_value_view value, uint64_t index) {
    neug_value_view out{};
    EXPECT_TRUE(Ok(neug_value_child(&value, index, &out, &error), &error));
    return out;
  }
  std::string String(neug_value_view value) {
    neug_string_view out{};
    EXPECT_TRUE(Ok(neug_value_get_string(&value, &out, &error), &error));
    return Str(out);
  }
  void Contacts() {
    Query(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    Query("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64);");
    Query("CREATE (:Person {id:1, name:'Alice', age:25});");
    Query("CREATE (:Person {id:2, name:'Bob', age:30});");
    Query(
        "MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS "
        "{since:2024}]->(b);");
  }
  neug_db_options options{};
  neug_database* db = nullptr;
  neug_connection* connection = nullptr;
  neug_result* result = nullptr;
  neug_error* error = nullptr;
  std::filesystem::path dir;
  std::string path;
};
