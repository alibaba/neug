/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <neug/main/neug_db.h>

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

namespace neug {
namespace test {

namespace {

std::filesystem::path GetExecutablePath() {
#if defined(__APPLE__)
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string buf(size, '\0');
  if (_NSGetExecutablePath(buf.data(), &size) != 0)
    return {};
  return std::filesystem::canonical(buf.c_str());
#else
  return std::filesystem::read_symlink("/proc/self/exe");
#endif
}

std::string FindBuildRoot() {
  auto dir = GetExecutablePath().parent_path();
  const std::string target =
      "extension/pattern_matching/libpattern_matching.neug_extension";
  for (int i = 0; i < 8; ++i) {
    if (std::filesystem::exists(dir / target))
      return dir.string();
    if (dir == dir.parent_path())
      break;
    dir = dir.parent_path();
  }
  return "";
}

std::filesystem::path MakeUniqueTempDir() {
  auto tmpl =
      (std::filesystem::temp_directory_path() / "neug_pattern_matching_XXXXXX")
          .string();
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr)
    return {};
  return std::filesystem::path(buf.data());
}

}  // namespace

class PatternMatchingTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    const std::string build_root = FindBuildRoot();
    ASSERT_FALSE(build_root.empty())
        << "Could not locate libpattern_matching.neug_extension near "
        << GetExecutablePath();
    setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1);
  }

  void SetUp() override {
    test_dir_ = MakeUniqueTempDir();
    ASSERT_FALSE(test_dir_.empty());

    db_ = std::make_unique<neug::NeugDB>();
    ASSERT_TRUE(db_->Open(test_dir_ / "db"));
    conn_ = db_->Connect();
    ASSERT_TRUE(conn_);

    const std::pair<std::string, std::string> setup_queries[] = {
        {"schema Person",
         "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age "
         "INT32);"},
        {"schema Company",
         "CREATE NODE TABLE Company(id INT32 PRIMARY KEY, name STRING);"},
        {"schema knows",
         "CREATE REL TABLE person_knows_person("
         "FROM Person TO Person, weight DOUBLE);"},
        {"schema works_at",
         "CREATE REL TABLE person_works_at_company("
         "FROM Person TO Company, since INT32);"},
        {"insert Alice",
         "CREATE (n:Person {id: 0, name: 'Alice', age: 20})"},
        {"insert Bob", "CREATE (n:Person {id: 1, name: 'Bob', age: 30})"},
        {"insert Carol",
         "CREATE (n:Person {id: 2, name: 'Carol', age: 20})"},
        {"insert Dave", "CREATE (n:Person {id: 3, name: 'Dave', age: 40})"},
        {"insert Acme", "CREATE (n:Company {id: 0, name: 'Acme'})"},
        {"insert Globex", "CREATE (n:Company {id: 1, name: 'Globex'})"},
        {"knows 0 1",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"knows 1 2",
         "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 "
         "CREATE (a)-[:person_knows_person {weight: 1.5}]->(b)"},
        {"knows 2 0",
         "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"knows 0 3",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 3 "
         "CREATE (a)-[:person_knows_person {weight: 2.5}]->(b)"},
        {"knows 3 1",
         "MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"works 0 0",
         "MATCH (p:Person), (c:Company) WHERE p.id = 0 AND c.id = 0 "
         "CREATE (p)-[:person_works_at_company {since: 2020}]->(c)"},
    };
    for (const auto& [label, query] : setup_queries) {
      auto res = conn_->Query(query);
      ASSERT_TRUE(res.has_value()) << label << ": " << res.error().ToString();
    }

    auto load = conn_->Query("LOAD pattern_matching;");
    ASSERT_TRUE(load.has_value()) << load.error().ToString();
  }

  void TearDown() override {
    conn_.reset();
    if (db_) {
      db_->Close();
      db_.reset();
    }
    for (const auto& dir : scratch_dirs_) {
      std::error_code ec;
      std::filesystem::remove_all(dir, ec);
    }
    if (!test_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(test_dir_, ec);
    }
  }

 protected:
  std::string WritePattern(const std::string& pattern_json) {
    auto dir = MakeUniqueTempDir();
    if (dir.empty())
      return "";
    scratch_dirs_.push_back(dir);
    auto path = dir / "pattern.json";
    std::ofstream ofs(path);
    if (!ofs.is_open())
      return "";
    ofs << pattern_json;
    return path.string();
  }

  const QueryResult& QueryOk(const std::string& query,
                             result<QueryResult>* holder) {
    *holder = conn_->Query(query);
    EXPECT_TRUE(holder->has_value())
        << (holder->has_value() ? "" : holder->error().ToString());
    return holder->value();
  }

  void ExpectAliasColumns(const QueryResult& result,
                          const std::vector<std::string>& names) {
    EXPECT_EQ(result.ColumnNames(), names);
    ASSERT_EQ(result.response().arrays_size(), static_cast<int>(names.size()));
  }

  void ExpectVertexEdgeVertexArrays(const QueryResult& result) {
    const auto& response = result.response();
    ASSERT_EQ(response.arrays_size(), 3);
    ASSERT_TRUE(response.arrays(0).has_vertex_array());
    ASSERT_TRUE(response.arrays(1).has_edge_array());
    ASSERT_TRUE(response.arrays(2).has_vertex_array());
    EXPECT_EQ(response.arrays(0).vertex_array().values_size(),
              static_cast<int>(result.length()));
    EXPECT_EQ(response.arrays(1).edge_array().values_size(),
              static_cast<int>(result.length()));
    EXPECT_EQ(response.arrays(2).vertex_array().values_size(),
              static_cast<int>(result.length()));
  }

  std::filesystem::path test_dir_;
  std::vector<std::filesystem::path> scratch_dirs_;
  std::unique_ptr<neug::NeugDB> db_;
  std::shared_ptr<neug::Connection> conn_;
};

TEST_F(PatternMatchingTest, PatternMatchDslReturnsNativeAliasColumns) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 10) "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 5u);
  EXPECT_NE(result.response().arrays(0).vertex_array().values(0).find("Person"),
            std::string::npos);
  EXPECT_NE(result.response().arrays(1).edge_array().values(0).find("weight"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, PatternMatchLimitIsRespected) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 2) "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, PatternMatchJsonFallsBackToDeterministicNames) {
  const auto path = WritePattern(R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })");
  ASSERT_FALSE(path.empty());

  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('" + path + "', 10) RETURN *;", &holder);

  ExpectAliasColumns(result, {"v0", "e0", "v1"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 5u);
}

TEST_F(PatternMatchingTest, PatternMatchJsonAliasesOverrideFallbackNames) {
  const auto path = WritePattern(R"({
    "vertices": [
      {"id": 0, "label": "Person", "alias": "src"},
      {"id": 1, "label": "Company", "alias": "dst"}
    ],
    "edges": [
      {
        "source": 0,
        "target": 1,
        "label": "person_works_at_company",
        "alias": "rel"
      }
    ]
  })");
  ASSERT_FALSE(path.empty());

  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('" + path + "', 10) RETURN *;", &holder);

  ExpectAliasColumns(result, {"src", "rel", "dst"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 1u);
  EXPECT_NE(result.response().arrays(1).edge_array().values(0).find("since"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, SampledPatternMatchUsesSameNativeOutputShape) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL SAMPLED_PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 8) "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_GT(result.length(), 0u);
}

}  // namespace test
}  // namespace neug
