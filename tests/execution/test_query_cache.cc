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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "neug/execution/execute/query_cache.h"
#include "neug/storages/graph/property_graph.h"

namespace neug::execution {
namespace {

class BlockingPlanner final : public IGraphPlanner {
 public:
  std::string type() const override { return "blocking"; }

  result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query, const Schema*, const GraphStats&) override {
    total_compile_count_.fetch_add(1);
    if (query == kBlockedQuery) {
      blocked_compile_count_.fetch_add(1);
      std::unique_lock<std::mutex> lock(mutex_);
      blocked_compile_started_ = true;
      cv_.notify_all();
      cv_.wait(lock, [this]() { return release_blocked_compile_; });
    }
    physical::PhysicalPlan plan;
    plan.mutable_flag()->set_read(true);
    return std::make_pair(std::move(plan), std::string{});
  }

  QueryAnalysis analyzeQuery(const std::string&) const override {
    return QueryAnalysis{AccessMode::kRead, ExplainMode::kNone,
                         QueryKind::kRegular};
  }

  bool WaitUntilBlockedCompileStarts() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [this]() { return blocked_compile_started_; });
  }

  void ReleaseBlockedCompile() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      release_blocked_compile_ = true;
    }
    cv_.notify_all();
  }

  int total_compile_count() const { return total_compile_count_.load(); }
  int blocked_compile_count() const { return blocked_compile_count_.load(); }

  static constexpr const char* kBlockedQuery = "blocked-query";

 private:
  std::atomic<int> total_compile_count_{0};
  std::atomic<int> blocked_compile_count_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  bool blocked_compile_started_{false};
  bool release_blocked_compile_{false};
};

class QueryCacheTest : public ::testing::Test {
 protected:
  QueryCacheTest()
      : view_(graph_), old_stats_(view_, 0), new_stats_(view_, 1) {}

  PropertyGraph graph_;
  GraphView view_;
  GraphStats old_stats_;
  GraphStats new_stats_;
};

TEST_F(QueryCacheTest, OldCompileCannotRepopulateAfterLazyGenerationAdvance) {
  auto planner = std::make_shared<BlockingPlanner>();
  auto cache = std::make_shared<GlobalQueryCache>(planner);

  std::shared_ptr<CacheValue> blocked_value;
  std::thread blocked_compile([&]() {
    auto result = cache->Get(old_stats_, BlockingPlanner::kBlockedQuery);
    if (result) {
      blocked_value = result.value();
    }
  });

  const bool compile_started = planner->WaitUntilBlockedCompileStarts();
  auto current_value = cache->Get(new_stats_, "current-query");
  planner->ReleaseBlockedCompile();
  blocked_compile.join();

  ASSERT_TRUE(compile_started);
  ASSERT_TRUE(current_value) << current_value.error().ToString();
  ASSERT_NE(blocked_value, nullptr);
  EXPECT_EQ(planner->total_compile_count(), 2);

  auto current_value_again = cache->Get(new_stats_, "current-query");
  ASSERT_TRUE(current_value_again) << current_value_again.error().ToString();
  EXPECT_EQ(current_value.value().get(), current_value_again.value().get());
  EXPECT_EQ(planner->total_compile_count(), 2);

  auto old_value_again = cache->Get(old_stats_, BlockingPlanner::kBlockedQuery);
  ASSERT_TRUE(old_value_again) << old_value_again.error().ToString();
  EXPECT_NE(blocked_value.get(), old_value_again.value().get());
  EXPECT_EQ(planner->blocked_compile_count(), 2);
}

TEST_F(QueryCacheTest,
       PlanningGenerationSeparatesLocalCachesWithoutInvalidatingPinnedReaders) {
  auto planner = std::make_shared<BlockingPlanner>();
  auto global_cache = std::make_shared<GlobalQueryCache>(planner);
  LocalQueryCache first_local(global_cache);
  LocalQueryCache second_local(global_cache);

  auto first_value = first_local.Get(old_stats_, "stable-query");
  ASSERT_TRUE(first_value) << first_value.error().ToString();
  auto shared_value = second_local.Get(old_stats_, "stable-query");
  ASSERT_TRUE(shared_value) << shared_value.error().ToString();
  EXPECT_EQ(first_value.value().get(), shared_value.value().get());
  EXPECT_EQ(planner->total_compile_count(), 1);

  auto refreshed_value = second_local.Get(new_stats_, "stable-query");
  ASSERT_TRUE(refreshed_value) << refreshed_value.error().ToString();
  EXPECT_NE(shared_value.value().get(), refreshed_value.value().get());
  EXPECT_EQ(planner->total_compile_count(), 2);

  auto refreshed_value_again = second_local.Get(new_stats_, "stable-query");
  ASSERT_TRUE(refreshed_value_again)
      << refreshed_value_again.error().ToString();
  EXPECT_EQ(refreshed_value.value().get(), refreshed_value_again.value().get());
  EXPECT_EQ(planner->total_compile_count(), 2);

  // The first slot may still execute against its pinned old snapshot. A
  // planning-generation advance must not evict that slot's valid local plan.
  auto old_snapshot_value = first_local.Get(old_stats_, "stable-query");
  ASSERT_TRUE(old_snapshot_value) << old_snapshot_value.error().ToString();
  EXPECT_EQ(first_value.value().get(), old_snapshot_value.value().get());
  EXPECT_EQ(planner->total_compile_count(), 2);
}

}  // namespace
}  // namespace neug::execution
