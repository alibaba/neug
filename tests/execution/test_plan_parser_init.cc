/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include <array>
#include <barrier>
#include <thread>

#include <gtest/gtest.h>

#include "neug/execution/execute/plan_parser.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"

namespace neug::execution {

class PlanParserTestAccess {
 public:
  static size_t BuilderCount() {
    size_t count = 0;
    for (const auto& builders : PlanParser::get().op_builders_) {
      count += builders.size();
    }
    return count;
  }
};

TEST(PlanParserInitialization, ConcurrentFirstInit) {
  constexpr size_t kThreads = 8;
  std::barrier start(kThreads);
  std::array<std::thread, kThreads> threads;
  for (auto& thread : threads) {
    thread = std::thread([&] {
      start.arrive_and_wait();
      for (int round = 0; round < 3; ++round) {
        neug::execution::PlanParser::get().init();
      }
    });
  }
  for (auto& thread : threads)
    thread.join();
}

TEST(PlanParserInitialization, RepeatedInitAndSequentialDatabaseReopens) {
  PlanParser::get().init();
  const auto builder_count = PlanParserTestAccess::BuilderCount();
  ASSERT_GT(builder_count, 0u);
  for (int round = 0; round < 3; ++round) {
    PlanParser::get().init();
    EXPECT_EQ(PlanParserTestAccess::BuilderCount(), builder_count);
  }

  // Keep only one database open; initialization must survive close/reopen.
  neug::NeugDBConfig config(":memory:", 1);
  config.checkpoint_on_close = false;
  for (int round = 0; round < 3; ++round) {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(config));
    EXPECT_EQ(PlanParserTestAccess::BuilderCount(), builder_count);
    {
      auto connection = db.Connect();
      auto result = connection->Query("RETURN 42;");
      ASSERT_TRUE(result) << result.error().ToString();
      ASSERT_TRUE(result.value().hasNext());
      EXPECT_EQ(result.value().GetInt64(0), 42);
    }
    db.Close();
  }
}

}  // namespace neug::execution
