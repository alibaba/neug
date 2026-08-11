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

#include <memory>
#include <string>
#include "gopt_test.h"
#include "neug/compiler/planner/gopt_planner.h"

namespace neug {
namespace gopt {

class ExplainTest : public GOptTest {
 public:
  // Helper: test that QueryAnalysis correctly identifies EXPLAIN/PROFILE
  // Now that explain_mode is determined at analysis time (not planning time),
  // we verify the analyzer sets it correctly.
  GOptPlanner planner_;
};

// Test 1: Regular query has explain_mode = NONE
TEST_F(ExplainTest, RegularQueryNoExplainMode) {
  auto analysis = planner_.analyzeQuery("MATCH (n:person) RETURN n.name");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kNone);
}

// Test 2: EXPLAIN query has explain_mode = EXPLAIN
TEST_F(ExplainTest, ExplainQueryAnalysis) {
  auto analysis =
      planner_.analyzeQuery("EXPLAIN MATCH (n:person) RETURN n.name");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kExplain);
}

// Test 3: PROFILE query has explain_mode = PROFILE
TEST_F(ExplainTest, ProfileQueryAnalysis) {
  auto analysis =
      planner_.analyzeQuery("PROFILE MATCH (n:person) RETURN n.name");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kProfile);
}

// Test 4: EXPLAIN with join
TEST_F(ExplainTest, ExplainWithJoin) {
  auto analysis = planner_.analyzeQuery(
      "EXPLAIN MATCH (n:person)-[e:knows]->(m:person) RETURN n.name, m.name");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kExplain);
}

// Test 5: PROFILE with join
TEST_F(ExplainTest, ProfileWithJoin) {
  auto analysis = planner_.analyzeQuery(
      "PROFILE MATCH (n:person)-[e:knows]->(m:person) RETURN n.name, m.name");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kProfile);
}

// Test 6: EXPLAIN with CHECKPOINT
TEST_F(ExplainTest, ExplainCheckpoint) {
  auto analysis = planner_.analyzeQuery("EXPLAIN CHECKPOINT");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kExplain);
}

// Test 7: PROFILE with CHECKPOINT
TEST_F(ExplainTest, ProfileCheckpoint) {
  auto analysis = planner_.analyzeQuery("PROFILE CHECKPOINT");
  EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kProfile);
}

// Test 8: All query types with EXPLAIN
TEST_F(ExplainTest, AllQueryTypesWithExplain) {
  std::vector<std::string> queries = {
      "EXPLAIN MATCH (n:person) RETURN n.name",
      "EXPLAIN MATCH (n:person)-[e:knows]->(m:person) RETURN n.name",
      "EXPLAIN MATCH (n:person) SET n.age = 30",
      "EXPLAIN CREATE NODE TABLE person(id INT64, PRIMARY KEY(id))",
  };

  for (const auto& query : queries) {
    auto analysis = planner_.analyzeQuery(query);
    EXPECT_EQ(analysis.explain_mode, neug::ExplainMode::kExplain)
        << "Query: " << query;
  }
}

}  // namespace gopt
}  // namespace neug
