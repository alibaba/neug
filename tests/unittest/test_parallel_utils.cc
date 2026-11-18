/** Copyright 2020 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <limits>
#include <stdexcept>
#include <vector>

#include "neug/utils/parallel_utils.h"
#include "utils/parallel_utils.h"

namespace neug {
TEST(ParallelFor, EmptyAndInvalidRanges) {
  auto never = [](size_t, size_t) { ADD_FAILURE(); };
  EXPECT_TRUE(parallel_for(0, 0, {4, 1}, never).ok());
  EXPECT_FALSE(parallel_for(1, 0, {4, 1}, never).ok());
  EXPECT_FALSE(parallel_for(0, 1, {0, 1}, never).ok());
  EXPECT_FALSE(parallel_for(0, 1, {4, 0}, never).ok());
}

TEST(ParallelFor, ExclusiveWorkerStatesAndCoverage) {
  std::barrier ready(4);
  std::vector<size_t> counts(4);
  std::vector<size_t> visits(1000);
  EXPECT_TRUE(
      parallel_for(0, visits.size(), {4, 1}, [&](size_t i, size_t worker) {
        if (counts[worker]++ == 0)
          ready.arrive_and_wait();
        ++visits[i];
      }).ok());
  for (auto count : counts)
    EXPECT_GT(count, 0);
  for (auto visited : visits)
    EXPECT_EQ(visited, 1);
}

TEST(ParallelFor, StatusFailureAndExceptionJoinWorkers) {
  for (bool throws : {false, true}) {
    std::atomic<size_t> active{0};
    std::barrier ready(4);
    auto body = [&](size_t, size_t worker) -> Status {
      ++active;
      ready.arrive_and_wait();
      --active;
      if (throws)
        throw std::runtime_error("worker failure");
      return Status::RuntimeError("worker " + std::to_string(worker));
    };
    if (throws) {
      EXPECT_THROW(parallel_for(0, 4, {4, 1}, body), std::runtime_error);
    } else {
      auto result = parallel_for(0, 4, {4, 1}, body);
      EXPECT_FALSE(result.ok());
      EXPECT_NE(result.error_message().find("worker "), std::string::npos);
    }
    EXPECT_EQ(active.load(), 0);
  }
}

TEST(ParallelFor, RangeNearSizeLimit) {
  const auto end = std::numeric_limits<size_t>::max();
  std::atomic<size_t> visits{0};
  EXPECT_TRUE(parallel_for(end - 5, end, {4, 3}, [&](size_t i, size_t) {
                EXPECT_GE(i, end - 5);
                ++visits;
              }).ok());
  EXPECT_EQ(visits.load(), 5);
}

TEST(ParallelFor, GDSAdaptersPreserveValuesAndVisibility) {
  std::vector<vid_t> values{7, 3, 9, 1};
  std::atomic<size_t> sum{0};
  gds::ParallelUtils::parallel_for(
      values.data(), values.size(), [&](vid_t v, int) { sum += v; }, 4, 1);
  EXPECT_EQ(sum.load(), 20);
  gds::ParallelUtils::parallel_for(
      values.data(), 0, [](vid_t, int) { ADD_FAILURE(); }, 4);
  VertexTimestamp timestamps;
  timestamps.Init(4, 8);
  VertexSet vertices(8, timestamps, 0);
  for (int workers : {1, 4}) {
    sum = 0;
    gds::ParallelUtils::parallel_for(
        vertices, [&](vid_t v, int) { sum += v; }, workers, 1);
    EXPECT_EQ(sum.load(), 6);
  }
  EXPECT_THROW(
      gds::ParallelUtils::parallel_for(
          values.data(), values.size(),
          [](vid_t, int) { throw std::runtime_error("gds failure"); }, 4, 1),
      std::runtime_error);
  EXPECT_THROW(
      gds::ParallelUtils::parallel_for(
          vertices,
          [](vid_t, int) { throw std::runtime_error("gds vertex failure"); }, 4,
          1),
      std::runtime_error);
}
}  // namespace neug
