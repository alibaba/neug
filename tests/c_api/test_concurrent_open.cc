/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include <array>
#include <barrier>
#include <thread>

#include "test_helpers.h"

TEST(CApiConcurrency, ConcurrentFirstOpensAndQueries) {
  constexpr size_t kThreads = 8;
  std::barrier start(kThreads);
  std::array<std::thread, kThreads> threads;
  for (auto& thread : threads) {
    thread = std::thread([&] {
      neug_db_options options;
      neug_db_options_init(&options);
      start.arrive_and_wait();
      // Repeated opens exercise initialization while other databases query.
      for (int round = 0; round < 3; ++round) {
        neug_database* db = nullptr;
        neug_connection* connection = nullptr;
        neug_result* result = nullptr;
        neug_error* error = nullptr;
        const auto open_status = neug_db_open(&options, &db, &error);
        EXPECT_TRUE(Ok(open_status, &error));
        if (open_status != NEUG_OK)
          continue;
        const auto connect_status = neug_db_connect(db, &connection, &error);
        EXPECT_TRUE(Ok(connect_status, &error));
        if (connect_status == NEUG_OK) {
          const auto query_status =
              neug_query(connection, S("RETURN 42;"), {}, &result, &error);
          EXPECT_TRUE(Ok(query_status, &error));
          if (query_status == NEUG_OK) {
            neug_value_view value{};
            int64_t answer = 0;
            EXPECT_TRUE(Ok(neug_result_get_value(result, 0, 0, &value, &error),
                           &error));
            EXPECT_TRUE(
                Ok(neug_value_get_int64(&value, &answer, &error), &error));
            EXPECT_EQ(answer, 42);
          }
        }
        neug_result_destroy(&result);
        EXPECT_TRUE(Ok(neug_connection_destroy(&connection, &error), &error));
        EXPECT_TRUE(Ok(neug_db_close(db, &error), &error));
        EXPECT_TRUE(Ok(neug_db_destroy(&db, &error), &error));
        neug_error_destroy(&error);
      }
    });
  }
  for (auto& thread : threads)
    thread.join();
}
