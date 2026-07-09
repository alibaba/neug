/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "column_assertions.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "utils.h"

namespace neug {
namespace test {

/**
 * ExplainProfileTest: Tests for PROFILE and EXPLAIN execution modes
 *
 * This test suite verifies:
 * 1. Query execution returns ProfileResult in PROFILE mode
 * 2. Query execution returns execution tree in EXPLAIN mode (no data)
 * 3. ProfileResult structure is correctly populated
 */
class ExplainProfileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string dir_name =
        "neug_test_explain_profile_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    test_dir_ = std::filesystem::temp_directory_path() / dir_name;
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
    db_path_ = (test_dir_ / "graph").string();

    // Load modern graph with a local DB instance, then close it.
    // Each test reopens a fresh DB — same pattern as test_checkpoint.cc.
    neug::NeugDB db;
    db.Open(db_path_, 4);
    auto conn = db.Connect();
    load_modern_graph(conn);
    conn->Close();
    db.Close();
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::string db_path_;
  std::filesystem::path test_dir_;
};

// ============================================================================
// Test Group 1: Regular Query (without PROFILE/EXPLAIN)
// ============================================================================

// Test 1.1: Regular query without PROFILE should not have ProfileResult
TEST_F(ExplainProfileTest, RegularQueryWithoutProfile) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("MATCH (n:person) RETURN n.name LIMIT 1");
  EXPECT_TRUE(result) << "Regular query should execute successfully";

  if (result) {
    const auto& response = result.value().response();

    // Verify regular query doesn't include ProfileResult
    if (response.has_profile_result()) {
      EXPECT_EQ(response.profile_result().operators_size(), 0)
          << "Regular query should not build operator tree";
    }

    // Verify response structure
    EXPECT_EQ(response.row_count(), 1) << "LIMIT 1 should return exactly 1 row";
    EXPECT_EQ(response.arrays_size(), 1)
        << "SELECT n.name should return 1 column";

    // Verify data content: first person is marko (id=1, name="marko")
    neug::test::AssertStringColumn(response, 0, {"marko"});
  }
  conn->Close();
  db.Close();
}

// Test 1.2: Regular query returns actual data
TEST_F(ExplainProfileTest, RegularQueryReturnsData) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("MATCH (n:person) RETURN n.name");
  EXPECT_TRUE(result);

  if (result) {
    const auto& response = result.value().response();

    // Verify response structure
    EXPECT_EQ(response.row_count(), 4) << "Should return all 4 person nodes";
    EXPECT_EQ(response.arrays_size(), 1)
        << "SELECT n.name should return 1 column";

    // Verify data content: person names in insertion order
    neug::test::AssertStringColumn(response, 0,
                                   {"marko", "vadas", "josh", "peter"});
  }
  conn->Close();
  db.Close();
}

// ============================================================================
// Test Group 2: PROFILE Mode - Verify ProfileResult is Generated
// ============================================================================

// Test 2.1: Regular query with PROFILE returns ProfileResult
TEST_F(ExplainProfileTest, ProfileModeSingleScan) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("PROFILE MATCH (n:person) RETURN n.name");
  EXPECT_TRUE(result) << "Query execution should succeed";

  if (result) {
    const auto& response = result.value().response();
    EXPECT_TRUE(response.has_profile_result())
        << "PROFILE mode should generate ProfileResult";
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GT(profile.operators_size(), 0)
          << "ProfileResult should contain operator metrics";
      EXPECT_GE(profile.total_elapsed_ms(), 0)
          << "total_elapsed_ms should be non-negative";

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

// Test 2.2: Join query with PROFILE (cross product join)
TEST_F(ExplainProfileTest, ProfileModeWithJoin) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  // Real join: two independent MATCH clauses create cross product
  // MATCH (n:person), (m:person) produces 4*4=16 result rows
  auto result =
      conn->Query("PROFILE MATCH (n:person), (m:person) RETURN n.name, m.name");
  EXPECT_TRUE(result) << "Join query execution should succeed";

  if (result) {
    const auto& response = result.value().response();
    EXPECT_TRUE(response.has_profile_result())
        << "PROFILE mode with join should generate ProfileResult";
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GT(profile.operators_size(), 0)
          << "ProfileResult should show multiple operators for join";
      EXPECT_GE(profile.total_output_rows(), 16)
          << "Join of 4 persons should produce at least 16 rows";

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT (Cross Product Join) ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

// Test 2.3: Aggregation query with PROFILE
TEST_F(ExplainProfileTest, ProfileModeWithAggregation) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("PROFILE MATCH (n:person) RETURN COUNT(*) as cnt");
  EXPECT_TRUE(result) << "Aggregation query execution should succeed";

  if (result) {
    const auto& response = result.value().response();
    EXPECT_TRUE(response.has_profile_result())
        << "PROFILE mode with aggregation should generate ProfileResult";

    // Print profile_result_text() for inspection
    std::cout << "\n=== PROFILE OUTPUT ===\n"
              << result.value().profile_result_text() << "=== END PROFILE ===\n"
              << std::endl;
  }
  conn->Close();
  db.Close();
}

// ============================================================================
// Test Group 3: EXPLAIN Mode - Verify Execution Tree is Built
// ============================================================================

// Test 3.1: EXPLAIN mode returns execution tree structure
TEST_F(ExplainProfileTest, ExplainModeSingleScan) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("EXPLAIN MATCH (n:person) RETURN n.name");
  EXPECT_TRUE(result) << "EXPLAIN query execution should succeed";

  if (result) {
    const auto& response = result.value().response();
    EXPECT_TRUE(response.has_profile_result())
        << "EXPLAIN mode should generate ProfileResult with execution tree";
    EXPECT_EQ(response.row_count(), 0)
        << "EXPLAIN mode should not return data rows";
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GT(profile.operators_size(), 0)
          << "EXPLAIN mode should show operator tree";
      EXPECT_EQ(profile.total_elapsed_ms(), 0)
          << "EXPLAIN mode should have 0 elapsed time (no execution)";
      EXPECT_EQ(profile.total_output_rows(), 0)
          << "EXPLAIN mode should have 0 output rows (no execution)";

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

// Test 3.2: EXPLAIN with join (cross product join)
TEST_F(ExplainProfileTest, ExplainModeWithJoin) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  // Real join: EXPLAIN shows query plan without executing it
  auto result =
      conn->Query("EXPLAIN MATCH (n:person), (m:person) RETURN n.name, m.name");
  EXPECT_TRUE(result) << "EXPLAIN join query should succeed";

  if (result) {
    const auto& response = result.value().response();
    EXPECT_TRUE(response.has_profile_result())
        << "EXPLAIN should generate execution tree";
    EXPECT_EQ(response.row_count(), 0)
        << "EXPLAIN should not execute or return rows";
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GT(profile.operators_size(), 0)
          << "EXPLAIN should show join operator tree";
      EXPECT_EQ(profile.total_elapsed_ms(), 0)
          << "EXPLAIN should not execute, so elapsed time is 0";
      EXPECT_EQ(profile.total_output_rows(), 0)
          << "EXPLAIN should not execute, so no output rows";

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

// ============================================================================
// Test Group 4: ProfileResult Structure Validation
// ============================================================================

// Test 4.1: ProfileResult contains required fields
TEST_F(ExplainProfileTest, ProfileResultFieldsPresent) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();

  auto result = conn->Query("PROFILE MATCH (n:person) RETURN n.age");
  EXPECT_TRUE(result);

  if (result) {
    const auto& response = result.value().response();
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GT(profile.operators_size(), 0);
      for (int i = 0; i < profile.operators_size(); ++i) {
        const auto& op = profile.operators(i);
        EXPECT_FALSE(op.operator_name().empty())
            << "Operator name should not be empty";
        (void) op.operator_id();
        (void) op.parent_id();
        (void) op.elapsed_ms();
        (void) op.output_rows();
      }

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

// Test 4.2: Verify ProfileResult metrics accumulate correctly
TEST_F(ExplainProfileTest, ProfileResultMetricsAccumulate) {
  neug::NeugDB db;
  db.Open(db_path_, 4);
  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  auto result = conn->Query("PROFILE MATCH (n:person) RETURN n.name, n.age");
  EXPECT_TRUE(result);

  if (result) {
    const auto& response = result.value().response();
    if (response.has_profile_result()) {
      const auto& profile = response.profile_result();
      EXPECT_GE(profile.total_output_rows(),
                static_cast<uint64_t>(response.row_count()))
          << "ProfileResult total_output_rows should be >= returned rows";
      EXPECT_GT(profile.total_elapsed_ms(), 0)
          << "PROFILE execution should have non-zero elapsed time";

      // Print profile_result_text() for inspection
      std::cout << "\n=== PROFILE OUTPUT ===\n"
                << result.value().profile_result_text()
                << "=== END PROFILE ===\n"
                << std::endl;
    }
  }
  conn->Close();
  db.Close();
}

}  // namespace test
}  // namespace neug
