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
#include <set>
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

constexpr const char* kSingleEdgePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"}
  ]
})";

constexpr const char* kTrianglePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"},
    {"id": 2, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
})";

std::string SingleEdgePatternWith(const std::string& vertex0_extra,
                                  const std::string& edge_extra) {
  return R"({
    "vertices": [
      {"id": 0, "label": "Person")" +
         vertex0_extra +
         R"(},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person")" +
         edge_extra + R"(}
    ]
  })";
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : line) {
    if (c == ',') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

std::vector<std::vector<std::string>> ReadCsv(const std::string& path) {
  std::vector<std::vector<std::string>> rows;
  std::ifstream ifs(path);
  std::string line;
  while (std::getline(ifs, line)) {
    if (!line.empty()) {
      rows.push_back(SplitCsvLine(line));
    }
  }
  return rows;
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
        {"insert Alice", "CREATE (n:Person {id: 0, name: 'Alice', age: 20})"},
        {"insert Bob", "CREATE (n:Person {id: 1, name: 'Bob', age: 30})"},
        {"insert Carol", "CREATE (n:Person {id: 2, name: 'Carol', age: 20})"},
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
        {"works 1 0",
         "MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 0 "
         "CREATE (p)-[:person_works_at_company {since: 2018}]->(c)"},
        {"works 2 1",
         "MATCH (p:Person), (c:Company) WHERE p.id = 2 AND c.id = 1 "
         "CREATE (p)-[:person_works_at_company {since: 2019}]->(c)"},
        {"works 3 1",
         "MATCH (p:Person), (c:Company) WHERE p.id = 3 AND c.id = 1 "
         "CREATE (p)-[:person_works_at_company {since: 2021}]->(c)"},
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

  // Exact subgraph matching: the unified PATTERN_MATCH with a single argument
  // (no sample size) runs the exact matcher.
  result<QueryResult> QueryPatternJson(const std::string& pattern_json,
                                       int64_t /*unused_limit*/ = 1000) {
    const auto path = WritePattern(pattern_json);
    EXPECT_FALSE(path.empty());
    return conn_->Query("CALL PATTERN_MATCH('" + path + "') RETURN *;");
  }

  // Sampled subgraph matching: the unified PATTERN_MATCH with a size and
  // is_sampled=true runs the FaSTest sampler.
  result<QueryResult> QuerySampledPatternJson(const std::string& pattern_json,
                                              int64_t sample_size = 1000) {
    const auto path = WritePattern(pattern_json);
    EXPECT_FALSE(path.empty());
    return conn_->Query("CALL PATTERN_MATCH('" + path + "', " +
                        std::to_string(sample_size) + ", true) RETURN *;");
  }

  // Exact subgraph matching with early termination after `size` matches:
  // the unified PATTERN_MATCH with a size and is_sampled=false.
  result<QueryResult> QueryExactEarlyTerminate(const std::string& pattern_json,
                                               int64_t size) {
    const auto path = WritePattern(pattern_json);
    EXPECT_FALSE(path.empty());
    return conn_->Query("CALL PATTERN_MATCH('" + path + "', " +
                        std::to_string(size) + ", false) RETURN *;");
  }

  void ExpectPatternJsonCount(const std::string& pattern_json,
                              size_t expected_count, int64_t limit = 1000) {
    auto result = QueryPatternJson(pattern_json, limit);
    ASSERT_TRUE(result.has_value()) << result.error().ToString();
    EXPECT_EQ(result->length(), expected_count);
  }

  void ExpectPatternJsonFails(const std::string& pattern_json) {
    auto result = QueryPatternJson(pattern_json);
    if (result.has_value()) {
      EXPECT_EQ(result->length(), 0u);
    }
  }

  std::string QueryResultFile(const std::string& query) {
    auto result = conn_->Query(query);
    if (!result.has_value()) {
      ADD_FAILURE() << result.error().ToString();
      return "";
    }
    const auto& response = result->response();
    if (response.arrays_size() == 0 || !response.arrays(0).has_string_array() ||
        response.arrays(0).string_array().values_size() == 0) {
      ADD_FAILURE() << "query did not return result_file";
      return "";
    }
    return response.arrays(0).string_array().values(0);
  }

  std::string RunGetVertexProperty(const std::string& ids_json,
                                   const std::string& label,
                                   const std::string& props_json) {
    return QueryResultFile("CALL GET_VERTEX_PROPERTY('" + ids_json + "', '" +
                           label + "', '" + props_json +
                           "') RETURN result_file;");
  }

  std::string RunGetEdgeProperty(const std::string& keys_json,
                                 const std::string& label,
                                 const std::string& props_json) {
    return QueryResultFile("CALL GET_EDGE_PROPERTY('" + keys_json + "', '" +
                           label + "', '" + props_json +
                           "') RETURN result_file;");
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

TEST_F(PatternMatchingTest, PatternMatchCypherReturnsNativeAliasColumns) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)') "
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

TEST_F(PatternMatchingTest, BarePatternWithoutMatchKeyword) {
  // The leading MATCH keyword is optional: a bare pattern is accepted and a
  // MATCH is prepended automatically. The explicit-MATCH form still works.
  result<QueryResult> bare_holder;
  const auto& bare = QueryOk(
      "CALL PATTERN_MATCH("
      "'(a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;",
      &bare_holder);
  ExpectAliasColumns(bare, {"a", "r", "b"});
  EXPECT_EQ(bare.length(), 5u);

  // A bare pattern may still carry inline WHERE / RETURN.
  result<QueryResult> where_holder;
  const auto& where = QueryOk(
      "CALL PATTERN_MATCH("
      "'(a:Person)-[r:person_knows_person]->(b:Person) "
      "WHERE a.age >= 30 RETURN a, r, b') RETURN *;",
      &where_holder);
  EXPECT_EQ(where.length(), 2u);

  // The explicit MATCH form (and lowercase) remain valid (backward compat).
  EXPECT_TRUE(
      conn_
          ->Query("CALL PATTERN_MATCH('MATCH (a:Person)-"
                  "[r:person_knows_person]->(b:Person)') RETURN count(a);")
          .has_value());
  EXPECT_TRUE(
      conn_
          ->Query("CALL PATTERN_MATCH('match (a:Person)-"
                  "[r:person_knows_person]->(b:Person)') RETURN count(a);")
          .has_value());
}

TEST_F(PatternMatchingTest, PatternMatchLimitIsRespected) {
  // Exact matching no longer takes a positional limit argument; the in-Cypher
  // LIMIT bounds the result instead.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "RETURN a, r, b LIMIT 2') "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, PatternMatchCypherPropertiesAndWhereAreTranslated) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person {age: 20})-[r:person_knows_person]->(b:Person) "
      "WHERE r.weight >= 0.5 RETURN a, r, b') RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 3u);
}

TEST_F(PatternMatchingTest, PatternMatchRejectsUnsupportedParseableCypher) {
  const std::vector<std::pair<std::string, std::string>> cases = {
      {"or_where",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "WHERE a.age = 20 OR b.age = 30"},
      {"optional_match",
       "OPTIONAL MATCH (a:Person)-[r:person_knows_person]->(b:Person)"},
      {"undirected_relationship",
       "MATCH (a:Person)-[r:person_knows_person]-(b:Person)"},
      {"variable_length_relationship",
       "MATCH (a:Person)-[r:person_knows_person*1..2]->(b:Person)"},
      {"multi_label_node",
       "MATCH (a:Person:Company)-[r:person_knows_person]->(b:Person)"},
      {"multi_type_relationship",
       "MATCH (a:Person)-[r:person_knows_person|:person_works_at_company]->"
       "(b:Person)"},
      {"cross_variable_comparison",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "WHERE a.age = b.age"},
      {"path_variable",
       "MATCH p = (a:Person)-[r:person_knows_person]->(b:Person)"},
      {"with_clause",
       "MATCH (a:Person) WITH a "
       "MATCH (a)-[r:person_knows_person]->(b:Person) RETURN *"},
      {"computed_return",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "RETURN a.age + b.age"},
      {"distinct_return",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "RETURN DISTINCT a"},
      {"computed_order_by",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "RETURN a, r, b ORDER BY a.age + b.age"},
      {"computed_limit",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
       "RETURN a, r, b LIMIT 1 + 1"},
  };

  for (const auto& [name, pattern] : cases) {
    SCOPED_TRACE(name + ": " + pattern);
    auto result =
        conn_->Query("CALL PATTERN_MATCH('" + pattern + "') RETURN *;");

    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().ToString().find("Failed to parse pattern input"),
              std::string::npos);
  }
}

TEST_F(PatternMatchingTest, InitializeReportsGraphCounts) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;",
      &holder);

  ASSERT_EQ(result.length(), 1u);
  const auto& response = result.response();
  ASSERT_EQ(response.arrays_size(), 5);
  EXPECT_EQ(response.arrays(0).string_array().values(0), "success");
  EXPECT_EQ(response.arrays(1).int64_array().values(0), 6);
  EXPECT_TRUE(response.arrays(2).int64_array().values(0) == 9 ||
              response.arrays(2).int64_array().values(0) == 18);
}

TEST_F(PatternMatchingTest, PatternMatchRejectsUnknownVertexLabel) {
  ExpectPatternJsonFails(R"({
    "vertices": [
      {"id": 0, "label": "Alien"},
      {"id": 1, "label": "Alien"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })");
}

TEST_F(PatternMatchingTest, PatternMatchRejectsUnknownEdgeLabel) {
  ExpectPatternJsonFails(R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_loves_person"}
    ]
  })");
}

TEST_F(PatternMatchingTest, PatternMatchRejectsMalformedPatternJson) {
  ExpectPatternJsonFails("{ this is not json: ,, }");
}

TEST_F(PatternMatchingTest, PatternMatchRejectsPatternMissingVertices) {
  ExpectPatternJsonFails(R"({"edges": []})");
}

TEST_F(PatternMatchingTest, PatternMatchRejectsPatternMissingEdges) {
  ExpectPatternJsonFails(R"({"vertices": [{"id": 0, "label": "Person"}]})");
}

TEST_F(PatternMatchingTest, PatternMatchRejectsNonexistentPatternFile) {
  const std::string path = "/tmp/pm_no_such_file.json";
  ASSERT_FALSE(std::filesystem::exists(path));
  auto result = conn_->Query("CALL PATTERN_MATCH('" + path + "') RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, EmptyConstraintArrayActsAsNoConstraint) {
  ExpectPatternJsonCount(SingleEdgePatternWith(R"(, "constraints": [])", ""),
                         5);
}

TEST_F(PatternMatchingTest, VertexConstraintOnUnknownPropertyFiltersAll) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "no_such_prop", "operator": "=", "value": 42}
           ])",
                             ""),
                         0);
}

TEST_F(PatternMatchingTest, EdgeConstraintOnUnknownPropertyFiltersAll) {
  ExpectPatternJsonCount(SingleEdgePatternWith("",
                                               R"(, "constraints": [
             {"property": "no_such_edge_prop", "operator": "=", "value": 1}
           ])"),
                         0);
}

TEST_F(PatternMatchingTest, VertexConstraintNameRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "name", "operator": "=", "value": "Alice"}
           ])",
                             ""),
                         2);
}

TEST_F(PatternMatchingTest, MultipleVertexConstraintsAreAndedMatch) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "name", "operator": "=", "value": "Alice"},
             {"property": "age", "operator": "=", "value": 20}
           ])",
                             ""),
                         2);
}

TEST_F(PatternMatchingTest, MultipleVertexConstraintsAreAndedNoMatch) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "name", "operator": "=", "value": "Alice"},
             {"property": "age", "operator": "=", "value": 99}
           ])",
                             ""),
                         0);
}

TEST_F(PatternMatchingTest, TypeMismatchConstraintExcludesCandidates) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "age", "operator": "=", "value": "twenty"}
           ])",
                             ""),
                         0);
}

TEST_F(PatternMatchingTest, UnknownOperatorFallsBackToEqual) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "name", "operator": "or", "value": "Alice"}
           ])",
                             ""),
                         2);
}

TEST_F(PatternMatchingTest, InOperatorIsRejected) {
  // 'in' is not implemented; it must fail loudly rather than silently match
  // nothing (which would look like a legitimately empty result).
  auto result = QueryPatternJson(SingleEdgePatternWith(
      R"(, "constraints": [
             {"property": "name", "operator": "in", "value": "Alice"}
           ])",
      ""));
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, NotInOperatorIsRejected) {
  auto result = QueryPatternJson(SingleEdgePatternWith(
      R"(, "constraints": [
             {"property": "name", "operator": "not_in", "value": "Alice"}
           ])",
      ""));
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, UppercaseInOperatorIsAlsoRejected) {
  // Operator matching is case-insensitive: "IN" must hit the same rejection as
  // "in" rather than silently degrading to '=' (Cypher keywords are
  // case-insensitive).
  auto result = QueryPatternJson(SingleEdgePatternWith(
      R"(, "constraints": [
             {"property": "name", "operator": "IN", "value": "Alice"}
           ])",
      ""));
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, TwoLabelPatternPersonWorksAtCompany) {
  ExpectPatternJsonCount(R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Company"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_works_at_company"}
    ]
  })",
                         4);
}

TEST_F(PatternMatchingTest, EdgeConstraintWeightRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith("",
                                               R"(, "constraints": [
             {"property": "weight", "operator": "=", "value": 0.5}
           ])"),
                         3);
}

TEST_F(PatternMatchingTest, ConstraintWithoutValueFallsBackToZero) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "name", "operator": "="}
           ])",
                             ""),
                         0);
}

TEST_F(PatternMatchingTest, SingleVertexPatternMatchesAllPersons) {
  ExpectPatternJsonCount(R"({
    "vertices": [
      {"id": 0, "label": "Person"}
    ],
    "edges": []
  })",
                         4);
}

TEST_F(PatternMatchingTest, PatternMatchRejectsEmptyVerticesArray) {
  ExpectPatternJsonFails(R"({"vertices": [], "edges": []})");
}

TEST_F(PatternMatchingTest, EdgeConstraintExactWeightMatchesSingleEdge) {
  ExpectPatternJsonCount(SingleEdgePatternWith("",
                                               R"(, "constraints": [
             {"property": "weight", "operator": "=", "value": 1.5}
           ])"),
                         1);
}

TEST_F(PatternMatchingTest, VertexConstraintGreaterThanRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "age", "operator": ">", "value": 25}
           ])",
                             ""),
                         2);
}

TEST_F(PatternMatchingTest, VertexConstraintLessThanRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "age", "operator": "<", "value": 25}
           ])",
                             ""),
                         3);
}

TEST_F(PatternMatchingTest, GreaterAndGreaterEqualHandleBoundary) {
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "age", "operator": ">", "value": 40}
           ])",
                             ""),
                         0);
  ExpectPatternJsonCount(SingleEdgePatternWith(
                             R"(, "constraints": [
             {"property": "age", "operator": ">=", "value": 40}
           ])",
                             ""),
                         1);
}

TEST_F(PatternMatchingTest, EdgeConstraintGreaterEqualRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith("",
                                               R"(, "constraints": [
             {"property": "weight", "operator": ">=", "value": 1.5}
           ])"),
                         2);
}

TEST_F(PatternMatchingTest, EdgeConstraintLessEqualRestrictsResults) {
  ExpectPatternJsonCount(SingleEdgePatternWith("",
                                               R"(, "constraints": [
             {"property": "weight", "operator": "<=", "value": 0.5}
           ])"),
                         3);
}

TEST_F(PatternMatchingTest, TrianglePatternFindsDirectedCycleEmbeddings) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('" + WritePattern(kTrianglePattern) + "') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"v0", "e0", "v1", "e1", "v2", "e2"});
  ASSERT_EQ(result.response().arrays_size(), 6);
  EXPECT_EQ(result.length(), 3u);
}

TEST_F(PatternMatchingTest, PathPatternThreeVerticesNoClosure) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r1:person_knows_person]->(b:Person)"
      "-[r2:person_knows_person]->(c:Person)') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r1", "b", "r2", "c"});
  EXPECT_EQ(result.length(), 6u);
}

TEST_F(PatternMatchingTest, CypherReverseEdgeDirection) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (b:Person)<-[r:person_knows_person]-(a:Person)') "
      "RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 5u);
}

TEST_F(PatternMatchingTest, PathPatternFourVerticesNoClosure) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r1:person_knows_person]->(b:Person)"
      "-[r2:person_knows_person]->(c:Person)"
      "-[r3:person_knows_person]->(d:Person)') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r1", "b", "r2", "c", "r3", "d"});
  EXPECT_GT(result.length(), 0u);
}

TEST_F(PatternMatchingTest, CypherWhereOnStringPropertyRestrictsResults) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "WHERE a.name = \"Alice\"') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, CypherMultipleWhereClausesAreAnded) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "WHERE a.age >= 18 AND a.name = \"Alice\"') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, CypherOrderBySkipLimitSortsExactResults) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "RETURN a, r, b ORDER BY a.age DESC, b.name ASC SKIP 1 LIMIT 2') "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  ASSERT_EQ(result.length(), 2u);
  const auto& response = result.response();
  EXPECT_NE(response.arrays(0).vertex_array().values(0).find("Bob"),
            std::string::npos);
  EXPECT_NE(response.arrays(2).vertex_array().values(0).find("Carol"),
            std::string::npos);
  EXPECT_NE(response.arrays(0).vertex_array().values(1).find("Carol"),
            std::string::npos);
  EXPECT_NE(response.arrays(2).vertex_array().values(1).find("Alice"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, CypherOrderByEdgePropertySortsExactResults) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "RETURN a, r, b ORDER BY r.weight DESC LIMIT 2') RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  ASSERT_EQ(result.length(), 2u);
  const auto& response = result.response();
  EXPECT_NE(response.arrays(0).vertex_array().values(0).find("Alice"),
            std::string::npos);
  EXPECT_NE(response.arrays(1).edge_array().values(0).find("2.5"),
            std::string::npos);
  EXPECT_NE(response.arrays(0).vertex_array().values(1).find("Bob"),
            std::string::npos);
  EXPECT_NE(response.arrays(1).edge_array().values(1).find("1.5"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, CypherEdgeInlinePropsMatchJsonEdgeConstraint) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person {weight: 1.5}]->"
      "(b:Person)') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 1u);
}

TEST_F(PatternMatchingTest, CypherReturnPropertyIsAccepted) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person) "
      "RETURN a.name, r.weight') RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 5u);
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
  const auto& result =
      QueryOk("CALL PATTERN_MATCH('" + path + "') RETURN *;", &holder);

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
  const auto& result =
      QueryOk("CALL PATTERN_MATCH('" + path + "') RETURN *;", &holder);

  ExpectAliasColumns(result, {"src", "rel", "dst"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 4u);
  EXPECT_NE(result.response().arrays(1).edge_array().values(0).find("since"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, SampledPatternMatchUsesSameNativeOutputShape) {
  // size + is_sampled=true selects the sampled FaSTest matcher.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 8, true) "
      "RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_GT(result.length(), 0u);
}

TEST_F(PatternMatchingTest, ExactEarlyTerminationStopsAtSize) {
  // is_sampled=false runs exact matching that stops after `size` matches.
  // The full pattern has 5 directed knows edges; size=2 must yield exactly 2.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 2, false) "
      "RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  ExpectVertexEdgeVertexArrays(result);
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, ExactEarlyTerminationLargerSizeReturnsAll) {
  // A size larger than the match count returns all matches (5).
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 100, false) "
      "RETURN *;",
      &holder);
  EXPECT_EQ(result.length(), 5u);
}

TEST_F(PatternMatchingTest, SizeBelowOneIsRejected) {
  // size must be >= 1 in both modes; 0 (and negatives) error at bind time.
  auto zero_sampled = conn_->Query(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 0, true) "
      "RETURN *;");
  EXPECT_FALSE(zero_sampled.has_value());

  auto zero_exact = conn_->Query(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 0, false) "
      "RETURN *;");
  EXPECT_FALSE(zero_exact.has_value());

  auto negative = conn_->Query(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', -3, true) "
      "RETURN *;");
  EXPECT_FALSE(negative.has_value());

  // size == 1 is the minimum valid value.
  auto one = conn_->Query(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r:person_knows_person]->(b:Person)', 1, false) "
      "RETURN *;");
  EXPECT_TRUE(one.has_value()) << one.error().ToString();
}

TEST_F(PatternMatchingTest, LongInlineCypherRoundTripsThroughCall) {
  const std::string cypher =
      "MATCH (a:Person)-[r1:person_knows_person]->"
      "(b:Person)-[r2:person_knows_person]->"
      "(c:Person)-[r3:person_knows_person]->(a:Person)";
  ASSERT_GT(cypher.size(), 48u);

  result<QueryResult> holder;
  const auto& result =
      QueryOk("CALL PATTERN_MATCH('" + cypher + "') RETURN *;", &holder);

  ExpectAliasColumns(result, {"a", "r1", "b", "r2", "c", "r3"});
  EXPECT_EQ(result.length(), 3u);
}

TEST_F(PatternMatchingTest, PatternCypherFileFormAndInlineFormAgree) {
  const std::string cypher =
      "MATCH (a:Person)-[r1:person_knows_person]->"
      "(b:Person)-[r2:person_knows_person]->"
      "(c:Person)-[r3:person_knows_person]->(a:Person)";
  auto path = WritePattern(cypher);
  ASSERT_FALSE(path.empty());

  result<QueryResult> file_holder;
  const auto& file_result =
      QueryOk("CALL PATTERN_MATCH('" + path + "') RETURN *;", &file_holder);

  result<QueryResult> inline_holder;
  const auto& inline_result =
      QueryOk("CALL PATTERN_MATCH('" + cypher + "') RETURN *;", &inline_holder);

  ExpectAliasColumns(file_result, {"a", "r1", "b", "r2", "c", "r3"});
  ExpectAliasColumns(inline_result, {"a", "r1", "b", "r2", "c", "r3"});
  EXPECT_EQ(file_result.length(), inline_result.length());
  EXPECT_EQ(file_result.length(), 3u);
}

TEST_F(PatternMatchingTest, PatternCypherSupportsWhereAndInlineProps) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH("
      "'MATCH (a:Person)-[r1:person_knows_person]->"
      "(b:Person {age: 30})-[r2:person_knows_person]->"
      "(c:Person)-[r3:person_knows_person]->(a:Person) "
      "WHERE a.age >= 18') RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"a", "r1", "b", "r2", "c", "r3"});
  EXPECT_EQ(result.length(), 1u);
  EXPECT_NE(result.response().arrays(2).vertex_array().values(0).find("Bob"),
            std::string::npos);
}

TEST_F(PatternMatchingTest, NativeOutputHasExpectedTriangleColumns) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('" + WritePattern(kTrianglePattern) + "') RETURN *;",
      &holder);

  ExpectAliasColumns(result, {"v0", "e0", "v1", "e1", "v2", "e2"});
  ASSERT_EQ(result.response().arrays_size(), 6);
  EXPECT_TRUE(result.response().arrays(0).has_vertex_array());
  EXPECT_TRUE(result.response().arrays(1).has_edge_array());
  EXPECT_TRUE(result.response().arrays(2).has_vertex_array());
  EXPECT_TRUE(result.response().arrays(3).has_edge_array());
  EXPECT_TRUE(result.response().arrays(4).has_vertex_array());
  EXPECT_TRUE(result.response().arrays(5).has_edge_array());
  EXPECT_EQ(result.length(), 3u);
}

TEST_F(PatternMatchingTest,
       SampledPatternMatchSampleSizeOneReturnsAtMostOneRow) {
  auto result = QuerySampledPatternJson(kSingleEdgePattern, 1);
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ExpectAliasColumns(*result, {"v0", "e0", "v1"});
  ExpectVertexEdgeVertexArrays(*result);
  EXPECT_LE(result->length(), 1u);
}

TEST_F(PatternMatchingTest, SampledPatternMatchSampleSizeFarExceedsEmbeddings) {
  auto result = QuerySampledPatternJson(kSingleEdgePattern, 100000);
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ExpectAliasColumns(*result, {"v0", "e0", "v1"});
  ExpectVertexEdgeVertexArrays(*result);
  EXPECT_GT(result->length(), 0u);
}

TEST_F(PatternMatchingTest, SampledPatternMatchSampleSizeZeroIsRejected) {
  // A sample size of 0 is no longer a silently-empty sampled call; it is
  // rejected cleanly at bind time (size must be >= 1).
  auto result = QuerySampledPatternJson(kSingleEdgePattern, 0);
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, GetVertexPropertyBasicReturnsRequestedProps) {
  result<QueryResult> init_holder;
  QueryOk(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;",
      &init_holder);

  auto path = RunGetVertexProperty("[0,1,2,3]", "Person", R"(["name","age"])");
  ASSERT_FALSE(path.empty());
  ASSERT_TRUE(std::filesystem::exists(path));

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 5u);
  ASSERT_EQ(rows[0].size(), 3u);
  EXPECT_EQ(rows[0][0], "vertex_id");
  EXPECT_EQ(rows[0][1], "name");
  EXPECT_EQ(rows[0][2], "age");

  const std::vector<std::pair<std::string, std::string>> expected = {
      {"Alice", "20"}, {"Bob", "30"}, {"Carol", "20"}, {"Dave", "40"}};
  for (int i = 0; i < 4; ++i) {
    const auto& row = rows[i + 1];
    ASSERT_EQ(row.size(), 3u);
    EXPECT_EQ(row[0], std::to_string(i));
    EXPECT_EQ(row[1], expected[i].first);
    EXPECT_EQ(row[2], expected[i].second);
  }
}

TEST_F(PatternMatchingTest, GetVertexPropertyUnknownLabelFailsQuery) {
  auto result = conn_->Query(
      "CALL GET_VERTEX_PROPERTY('[0]', 'Alien', '[\"name\"]') "
      "RETURN result_file;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, GetVertexPropertyUnknownPropertyYieldsEmptyCells) {
  auto path = RunGetVertexProperty("[0,1]", "Person", R"(["no_such_prop"])");
  ASSERT_FALSE(path.empty());

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 3u);
  ASSERT_EQ(rows[0].size(), 2u);
  EXPECT_EQ(rows[0][1], "no_such_prop");
  ASSERT_EQ(rows[1].size(), 2u);
  ASSERT_EQ(rows[2].size(), 2u);
  EXPECT_TRUE(rows[1][1].empty());
  EXPECT_TRUE(rows[2][1].empty());
}

TEST_F(PatternMatchingTest, GetVertexPropertyEmptyIdsYieldsHeaderOnly) {
  auto path = RunGetVertexProperty("[]", "Person", R"(["name"])");
  ASSERT_FALSE(path.empty());

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].size(), 2u);
  EXPECT_EQ(rows[0][0], "vertex_id");
  EXPECT_EQ(rows[0][1], "name");
}

TEST_F(PatternMatchingTest, GetEdgePropertyBasicReturnsWeight) {
  auto path = RunGetEdgeProperty(R"(["0:1:0"])", "person_knows_person",
                                 R"(["weight"])");
  ASSERT_FALSE(path.empty());
  ASSERT_TRUE(std::filesystem::exists(path));

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 2u);
  ASSERT_EQ(rows[0].size(), 4u);
  EXPECT_EQ(rows[0][0], "edge_key");
  EXPECT_EQ(rows[0][1], "src_id");
  EXPECT_EQ(rows[0][2], "dst_id");
  EXPECT_EQ(rows[0][3], "weight");
  EXPECT_EQ(rows[1][0], "0:1:0");
  EXPECT_EQ(rows[1][1], "0");
  EXPECT_EQ(rows[1][2], "1");
  EXPECT_DOUBLE_EQ(std::stod(rows[1][3]), 0.5);
}

TEST_F(PatternMatchingTest, GetEdgePropertyMalformedKeyYieldsBlankCells) {
  auto path = RunGetEdgeProperty(R"(["not-an-edge-key"])",
                                 "person_knows_person", R"(["weight"])");
  ASSERT_FALSE(path.empty());

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 2u);
  ASSERT_EQ(rows[0].size(), 4u);
  EXPECT_EQ(rows[1][0], "not-an-edge-key");
  ASSERT_GE(rows[1].size(), 4u);
  EXPECT_TRUE(rows[1][3].empty());
}

TEST_F(PatternMatchingTest, SaveCheckpointReturnsSuccessAndWritesFiles) {
  result<QueryResult> init_holder;
  QueryOk(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;",
      &init_holder);

  auto ckpt_dir = test_dir_ / "checkpoint_save";
  std::filesystem::create_directories(ckpt_dir);

  result<QueryResult> holder;
  const auto& result =
      QueryOk("CALL SAVE_SAMPLEDMATCH_CHECKPOINT('" + ckpt_dir.string() +
                  "') RETURN status, checkpoint_dir;",
              &holder);

  ASSERT_EQ(result.length(), 1u);
  ASSERT_GE(result.response().arrays_size(), 2);
  EXPECT_EQ(result.response().arrays(0).string_array().values(0), "success");
  EXPECT_EQ(result.response().arrays(1).string_array().values(0),
            ckpt_dir.string());
  EXPECT_TRUE(std::filesystem::exists(ckpt_dir / "data_graph_meta.bin"));
  EXPECT_TRUE(std::filesystem::exists(ckpt_dir / "schema_graph.bin"));
}

TEST_F(PatternMatchingTest, InitializeFromCheckpointRoundtrip) {
  result<QueryResult> init_holder;
  const auto& init = QueryOk(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;",
      &init_holder);
  const auto orig_vertices = init.response().arrays(1).int64_array().values(0);
  const auto orig_edges = init.response().arrays(2).int64_array().values(0);

  auto ckpt_dir = test_dir_ / "checkpoint_roundtrip";
  std::filesystem::create_directories(ckpt_dir);

  result<QueryResult> save_holder;
  const auto& save = QueryOk("CALL SAVE_SAMPLEDMATCH_CHECKPOINT('" +
                                 ckpt_dir.string() + "') RETURN status;",
                             &save_holder);
  ASSERT_EQ(save.response().arrays(0).string_array().values(0), "success");

  result<QueryResult> reinit_holder;
  const auto& reinit =
      QueryOk("CALL INITIALIZE('" + ckpt_dir.string() +
                  "') RETURN status, num_vertices, num_edges, max_degree, "
                  "degeneracy;",
              &reinit_holder);
  EXPECT_EQ(reinit.response().arrays(0).string_array().values(0), "success");
  EXPECT_EQ(reinit.response().arrays(1).int64_array().values(0), orig_vertices);
  EXPECT_EQ(reinit.response().arrays(2).int64_array().values(0), orig_edges);

  auto match = QuerySampledPatternJson(kTrianglePattern, 1000);
  ASSERT_TRUE(match.has_value()) << match.error().ToString();
  EXPECT_GT(match->length(), 0u);
}

// ---------------------------------------------------------------------------
// Outer (NeuG-pipeline) ORDER BY / LIMIT / property projection / aggregation
// applied to PATTERN_MATCH output. These exercise that the table-function
// output vertex/edge variables carry the same catalog/property metadata as a
// MATCH-bound node, so `a.prop`, `ORDER BY a.prop`, and `count(a)` resolve.
// ---------------------------------------------------------------------------

TEST_F(PatternMatchingTest, OuterLimitOnlyReturnsRows) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN * LIMIT 2;",
      &holder);
  EXPECT_EQ(result.length(), 2u);
}

TEST_F(PatternMatchingTest, OuterVertexPropertyProjectsScalar) {
  // a.age is a real Int32 property now; project it as a scalar column.
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN a.age AS age LIMIT 100;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  EXPECT_TRUE(result->response().arrays(0).has_int32_array() ||
              result->response().arrays(0).has_int64_array());
}

TEST_F(PatternMatchingTest, OuterOrderByVertexPropertySortsResults) {
  // Source vertices of the 5 directed edges have ages
  // {Alice20, Bob30, Carol20, Alice20, Dave40}; ORDER BY a.age DESC LIMIT 2
  // must surface Dave(40) then Bob(30).
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN a.name AS name, a.age AS age ORDER BY a.age DESC "
      "LIMIT 2;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 2u);
  const auto& names = result->response().arrays(0).string_array();
  EXPECT_NE(names.values(0).find("Dave"), std::string::npos);
  EXPECT_NE(names.values(1).find("Bob"), std::string::npos);
}

TEST_F(PatternMatchingTest, OuterCountAggregatesMatches) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN count(a) AS cnt;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 1u);
  EXPECT_EQ(result->response().arrays(0).int64_array().values(0), 5);
}

TEST_F(PatternMatchingTest, OuterOrderByEdgePropertySorts) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN r.weight AS w ORDER BY r.weight DESC LIMIT 1;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 1u);
  EXPECT_DOUBLE_EQ(result->response().arrays(0).double_array().values(0), 2.5);
}

TEST_F(PatternMatchingTest, OuterOrderByWholeVertexStillRejected) {
  // Ordering by a whole VERTEX object remains unsupported by NeuG's binder
  // (this is an explicit rule, not the previous crash) — must error cleanly.
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " RETURN * ORDER BY a LIMIT 2;");
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// YIELD support: CALL PATTERN_MATCH(...) YIELD <col> [AS <alias>] RETURN ...
// The executor materializes every pattern column positionally, so YIELD only
// controls which columns are visible (and under what name) to the trailing
// RETURN; it must still read the correct underlying column.
// ---------------------------------------------------------------------------

TEST_F(PatternMatchingTest, YieldAllColumnsBehavesLikeNoYield) {
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD a, r, b RETURN *;",
      &holder);
  ExpectAliasColumns(result, {"a", "r", "b"});
  EXPECT_EQ(result.length(), 5u);
}

TEST_F(PatternMatchingTest, YieldRenameWithAs) {
  // YIELD a AS x renames the bound variable; x.age must resolve to a's age.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD a AS x, b AS y "
      "RETURN x.name AS src, y.name AS dst ORDER BY x.age DESC LIMIT 1;",
      &holder);
  ASSERT_EQ(result.length(), 1u);
  // Highest-age source vertex is Dave(40), who knows Bob.
  EXPECT_EQ(result.response().arrays(0).string_array().values(0), "Dave");
  EXPECT_EQ(result.response().arrays(1).string_array().values(0), "Bob");
}

TEST_F(PatternMatchingTest, YieldSubsetSelectsCorrectColumn) {
  // YIELD b must read the destination vertex, not the positionally-first one.
  // Edge (3->1) Dave->Bob has the max source age; ordering by b.age puts the
  // youngest destination first. Just assert the projected values are real
  // destination names and the column count is 1.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD b RETURN b.name AS dst ORDER BY b.name;",
      &holder);
  ASSERT_EQ(result.length(), 5u);
  // Destinations across the 5 edges are {Bob, Carol, Alice, Dave, Bob}.
  const auto& dst = result.response().arrays(0).string_array();
  EXPECT_EQ(dst.values(0), "Alice");
  EXPECT_EQ(dst.values(1), "Bob");
  EXPECT_EQ(dst.values(2), "Bob");
  EXPECT_EQ(dst.values(3), "Carol");
  EXPECT_EQ(dst.values(4), "Dave");
}

TEST_F(PatternMatchingTest, YieldSubsetEdgeReadsEdgeColumn) {
  // YIELD r (edge only) must read the edge column, not crash on a vertex.
  result<QueryResult> holder;
  const auto& result = QueryOk(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD r RETURN r.weight AS w ORDER BY r.weight DESC LIMIT 1;",
      &holder);
  ASSERT_EQ(result.length(), 1u);
  EXPECT_DOUBLE_EQ(result.response().arrays(0).double_array().values(0), 2.5);
}

TEST_F(PatternMatchingTest, YieldHiddenVariableIsNotInScope) {
  // A column not listed in YIELD is not visible to the trailing RETURN.
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD a RETURN b.name;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, YieldUnknownVariableIsRejected) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH('MATCH "
      "(a:Person)-[r:person_knows_person]->(b:Person)')"
      " YIELD nosuchvar RETURN nosuchvar;");
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// Malformed raw-JSON pattern inputs must be rejected with a clean error, not
// crash the process (heap OOB / null-deref / unguarded stoi / wrong-type JSON
// access). A passing test here means the query returned without aborting.
// ---------------------------------------------------------------------------

TEST_F(PatternMatchingTest, SampledOutOfRangeVertexIdIsRejected) {
  // id 5 with only one vertex would index past vertex_label/adj_list buffers.
  auto result = conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":5,\"label\":\"Person\"}],\"edges\":[]}', "
      "100, true) RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, SampledOutOfRangeEdgeEndpointIsRejected) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":0,\"label\":\"Person\"}],"
      "\"edges\":[{\"source\":0,\"target\":9,"
      "\"label\":\"person_knows_person\"}]}', 100, true) RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, SampledMissingVertexLabelIsRejected) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":0}],\"edges\":[]}', 10, true) RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, SampledNonIntegerVertexIdStringIsRejected) {
  auto result = conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":\"abc\",\"label\":\"Person\"}],\"edges\":[]}', "
      "10, true) RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, UnlabeledJsonVertexPropertyAccessIsRejected) {
  // Unlabeled vertex bound as a bare kVertex would null-deref on a.name; the
  // bind must reject it instead.
  auto result = conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":0,\"alias\":\"a\"},"
      "{\"id\":1,\"alias\":\"b\",\"label\":\"Person\"}],"
      "\"edges\":[{\"source\":0,\"target\":1,"
      "\"label\":\"person_knows_person\"}]}') RETURN *;");
  EXPECT_FALSE(result.has_value());
}

TEST_F(PatternMatchingTest, MalformedConstraintJsonDoesNotCrash) {
  // A non-object constraint element (42) and a non-string property must not
  // crash ParseConstraints (it skips the malformed entry). The query returning
  // at all — with or without rows — proves the process did not abort.
  (void) conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":0,\"label\":\"Person\","
      "\"constraints\":[42]}],\"edges\":[]}') RETURN *;");
  (void) conn_->Query(
      "CALL PATTERN_MATCH("
      "'{\"vertices\":[{\"id\":0,\"label\":\"Person\","
      "\"constraints\":[{\"property\":5,\"operator\":\"=\",\"value\":1}]}],"
      "\"edges\":[]}') RETURN *;");
  SUCCEED();  // reached here without aborting
}

}  // namespace test
}  // namespace neug
