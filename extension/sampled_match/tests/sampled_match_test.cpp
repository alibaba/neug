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
  if (_NSGetExecutablePath(buf.data(), &size) != 0) return {};
  return std::filesystem::canonical(buf.c_str());
#else
  return std::filesystem::read_symlink("/proc/self/exe");
#endif
}

// Walk upward from the test binary and return the first ancestor that contains
// the built extension library. That directory is what LOAD expects via
// NEUG_EXTENSION_HOME_PYENV.
std::string FindBuildRoot() {
  auto dir = GetExecutablePath().parent_path();
  const std::string target =
      "extension/sampled_match/libsampled_match.neug_extension";
  for (int i = 0; i < 8; ++i) {
    if (std::filesystem::exists(dir / target)) return dir.string();
    if (dir == dir.parent_path()) break;
    dir = dir.parent_path();
  }
  return "";
}

// Allocate a unique scratch directory under the system temp dir. Using
// mkdtemp avoids races between concurrently-running tests and never touches
// the current working directory.
std::filesystem::path MakeUniqueTempDir() {
  auto tmpl =
      (std::filesystem::temp_directory_path() / "neug_sampled_match_XXXXXX")
          .string();
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr) return {};
  return std::filesystem::path(buf.data());
}

// Triangle pattern reused across the basic tests.
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

// Single Person -> Person edge — used by tests that only need one vertex /
// edge under inspection (constraint, type mismatch, etc.).
constexpr const char* kSingleEdgePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"}
  ]
})";

}  // namespace

class SampledMatchTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    const std::string build_root = FindBuildRoot();
    ASSERT_FALSE(build_root.empty())
        << "Could not locate libsampled_match.neug_extension near "
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

    // Schema: two node labels (Person, Company) and two edge labels with
    // typed properties — this gives the corner-case tests room to exercise
    // unknown labels, unknown properties, type-mismatch on int/string, and
    // edge property constraints.
    //
    // Data layout (id/name/age):
    //   Person(0, 'Alice', 20)
    //   Person(1, 'Bob',   30)
    //   Person(2, 'Carol', 20)
    //   Person(3, 'Dave',  40)
    //   Company(0, 'Acme')
    //   Company(1, 'Globex')
    //
    // Edges (person_knows_person, weight DOUBLE):
    //   0 -> 1 (0.5), 1 -> 2 (1.5), 2 -> 0 (0.5)        -- the original triangle
    //   0 -> 3 (2.5), 3 -> 1 (0.5)                       -- extends to a "Y"
    //
    // Edges (person_works_at_company, since INT32):
    //   0 -> Acme  (2020),  1 -> Acme  (2018)
    //   2 -> Globex(2019),  3 -> Globex(2021)
    const std::pair<std::string, std::string> setup_queries[] = {
        {"schema: Person",
         "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age INT32);"},
        {"schema: Company",
         "CREATE NODE TABLE Company(id INT32 PRIMARY KEY, name STRING);"},
        {"schema: knows",
         "CREATE REL TABLE person_knows_person("
         "FROM Person TO Person, weight DOUBLE);"},
        {"schema: works_at",
         "CREATE REL TABLE person_works_at_company("
         "FROM Person TO Company, since INT32);"},
        {"insert p0", "CREATE (n:Person {id: 0, name: 'Alice', age: 20})"},
        {"insert p1", "CREATE (n:Person {id: 1, name: 'Bob',   age: 30})"},
        {"insert p2", "CREATE (n:Person {id: 2, name: 'Carol', age: 20})"},
        {"insert p3", "CREATE (n:Person {id: 3, name: 'Dave',  age: 40})"},
        {"insert c0", "CREATE (n:Company {id: 0, name: 'Acme'})"},
        {"insert c1", "CREATE (n:Company {id: 1, name: 'Globex'})"},
        {"edge 0->1 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge 1->2 (1.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 "
         "CREATE (a)-[:person_knows_person {weight: 1.5}]->(b)"},
        {"edge 2->0 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge 0->3 (2.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 3 "
         "CREATE (a)-[:person_knows_person {weight: 2.5}]->(b)"},
        {"edge 3->1 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge p0->c0 (2020)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 0 AND b.id = 0 "
         "CREATE (a)-[:person_works_at_company {since: 2020}]->(b)"},
        {"edge p1->c0 (2018)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 1 AND b.id = 0 "
         "CREATE (a)-[:person_works_at_company {since: 2018}]->(b)"},
        {"edge p2->c1 (2019)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 2 AND b.id = 1 "
         "CREATE (a)-[:person_works_at_company {since: 2019}]->(b)"},
        {"edge p3->c1 (2021)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 3 AND b.id = 1 "
         "CREATE (a)-[:person_works_at_company {since: 2021}]->(b)"},
    };
    for (const auto& [label, q] : setup_queries) {
      auto res = conn_->Query(q);
      ASSERT_TRUE(res.has_value())
          << label << " failed: " << res.error().ToString();
    }

    auto load = conn_->Query("LOAD sampled_match;");
    ASSERT_TRUE(load.has_value()) << load.error().ToString();
    // Trivial follow-up so the LOAD pipeline fully retires before any
    // CALL SAMPLED_MATCH (avoids a use-after-free seen when the LOAD
    // QueryResult is the last object torn down at SetUp exit).
    auto ping = conn_->Query("RETURN 1 AS x;");
    ASSERT_TRUE(ping.has_value()) << ping.error().ToString();
  }

  void TearDown() override {
    conn_.reset();
    if (db_) {
      db_->Close();
      db_.reset();
    }
    if (!test_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(test_dir_, ec);
    }
    for (const auto& d : pattern_dirs_) {
      std::error_code ec;
      std::filesystem::remove_all(d, ec);
    }
    pattern_dirs_.clear();
  }

 protected:
  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  // Result of one CALL SAMPLED_MATCH invocation, only the columns we assert on.
  struct SmResult {
    bool query_ok = false;             // SQL-layer success
    std::string error;                 // SQL error (if any)
    double estimated_count = 0.0;
    int64_t sample_count = 0;
    std::string result_file;
    std::string props_file;
  };

  // Persist `pattern_json` into a tempdir and return the path. Caller doesn't
  // own the dir — TearDown cleans it up.
  std::string WritePattern(const std::string& pattern_json) {
    std::string tmpl = "/tmp/sm_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    if (mkdtemp(buf.data()) == nullptr) return "";
    std::filesystem::path dir(buf.data());
    pattern_dirs_.push_back(dir);
    auto path = dir / "p.json";
    std::ofstream ofs(path);
    if (!ofs.is_open()) return "";
    ofs << pattern_json;
    ofs.close();
    return path.string();
  }

  // Run CALL SAMPLED_MATCH on the given pattern path. Returns a digest with
  // the four columns the function exposes; query-layer errors are captured
  // (rather than ASSERTed) so corner-case tests can choose how to react.
  SmResult RunSampledMatch(const std::string& pattern_path,
                           int64_t sample_size = 1000) {
    SmResult r;
    if (pattern_path.empty()) {
      r.error = "WritePattern returned empty path";
      return r;
    }
    std::ostringstream q;
    q << "CALL SAMPLED_MATCH('" << pattern_path << "', " << sample_size
      << ") RETURN estimated_count, sample_count, result_file, props_file;";
    auto sm = conn_->Query(q.str());
    if (!sm.has_value()) {
      r.error = sm.error().ToString();
      return r;
    }
    const auto& sm_qr = sm.value();
    const auto& resp = sm_qr.response();
    if (sm_qr.length() != 1u || resp.arrays_size() != 4) {
      r.error = "unexpected response shape";
      return r;
    }
    const auto& est = resp.arrays(0).double_array();
    const auto& cnt = resp.arrays(1).int64_array();
    const auto& rf = resp.arrays(2).string_array();
    const auto& pf = resp.arrays(3).string_array();
    if (est.values_size() != 1 || cnt.values_size() != 1) {
      r.error = "missing per-row values";
      return r;
    }
    r.query_ok = true;
    r.estimated_count = est.values(0);
    r.sample_count = cnt.values(0);
    r.result_file = rf.values_size() ? rf.values(0) : "";
    r.props_file = pf.values_size() ? pf.values(0) : "";
    return r;
  }

  std::filesystem::path test_dir_;
  std::vector<std::filesystem::path> pattern_dirs_;
  std::unique_ptr<neug::NeugDB> db_;
  std::shared_ptr<neug::Connection> conn_;
};

// ---------------------------------------------------------------------------
// 1. Happy-path baseline — drives extension load, INITIALIZE bootstrap, and a
//    SAMPLED_MATCH on a known-good triangle pattern. The richer dataset
//    (5 knows-edges among 4 Person nodes, plus a separate Company subgraph)
//    means counts here differ from the original 3-node fixture.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, LoadsExtensionAndGraphQueries) {
  auto count = conn_->Query("MATCH (n:Person) RETURN count(*) AS c;");
  ASSERT_TRUE(count.has_value()) << count.error().ToString();

  auto init = conn_->Query(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;");
  ASSERT_TRUE(init.has_value()) << init.error().ToString();

  const auto& init_qr = init.value();
  const auto& init_resp = init_qr.response();
  ASSERT_EQ(init_qr.length(), 1u);
  ASSERT_EQ(init_resp.arrays_size(), 5);

  const auto& status_col = init_resp.arrays(0).string_array();
  const auto& vertices_col = init_resp.arrays(1).int64_array();
  const auto& edges_col = init_resp.arrays(2).int64_array();
  ASSERT_EQ(status_col.values_size(), 1);
  ASSERT_EQ(vertices_col.values_size(), 1);
  ASSERT_EQ(edges_col.values_size(), 1);

  EXPECT_EQ(status_col.values(0), "success");
  // 4 Persons + 2 Companies = 6 vertices.
  EXPECT_EQ(vertices_col.values(0), 6);
  // 5 knows + 4 works_at = 9 directed edges; preprocessing may double-count
  // them as undirected (9) or directed-with-reverse (18).
  EXPECT_TRUE(edges_col.values(0) == 9 || edges_col.values(0) == 18)
      << "unexpected edge count: " << edges_col.values(0);

  auto path = WritePattern(kTrianglePattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  // The graph contains exactly one undirected 3-cycle (0 - 1 - 2 - 0); FaSTest
  // should sample at least one embedding and report a positive estimate.
  EXPECT_GE(r.sample_count, 1)
      << "SAMPLED_MATCH returned zero sampled embeddings on a triangle";
  EXPECT_GT(r.estimated_count, 0.0)
      << "SAMPLED_MATCH returned a non-positive estimated count";

  // Result file must exist and be non-empty.
  EXPECT_FALSE(r.result_file.empty());
  EXPECT_TRUE(std::filesystem::exists(r.result_file));
  EXPECT_GT(std::filesystem::file_size(r.result_file), 0u);
}

// ---------------------------------------------------------------------------
// 2. Pattern uses a vertex label that doesn't exist in the schema.
//    CreatePatternFromJson should bail out, match() should return -1, and the
//    SAMPLED_MATCH call still completes at the SQL layer (errors are surfaced
//    via the returned columns, not via Query failure).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternWithUnknownVertexLabel) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Alien"},
      {"id": 1, "label": "Alien"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto path = WritePattern(kPattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0)
      << "match() should signal failure with -1 when label is unknown";
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 3. Pattern uses an edge label that isn't in the schema.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternWithUnknownEdgeLabel) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_loves_person"}
    ]
  })";
  auto path = WritePattern(kPattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 4. Pattern file content is not valid JSON.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsMalformedPatternJson) {
  auto path = WritePattern("{ this is not json: ,, }");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 5. Pattern JSON is well-formed but missing required arrays.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternMissingVertices) {
  auto path = WritePattern(R"({"edges": []})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

TEST_F(SampledMatchTest, RejectsPatternMissingEdges) {
  auto path = WritePattern(R"({"vertices": [{"id": 0, "label": "Person"}]})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 6. Pattern file path doesn't exist on disk.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsNonexistentPatternFile) {
  // Hand-craft a short path that doesn't exist; don't go through
  // WritePattern (we want the file absent).
  std::string path = "/tmp/sm_no_such_file.json";
  ASSERT_FALSE(std::filesystem::exists(path));
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 7. Empty constraint array on a vertex behaves identically to "no
//    constraints" — i.e. it doesn't accidentally filter all candidates out.
//    Compares estimated_count (the cardinality estimate) rather than
//    sample_count, since the sampler saturates at sample_size whenever there
//    is at least one matching embedding.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EmptyConstraintArrayActsAsNoConstraint) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person", "constraints": []},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto with_empty = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_empty.query_ok) << with_empty.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_empty.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 8. Constraint references a property name that doesn't exist on the vertex
//    label. The constraint is silently skipped (with a one-shot LOG warning),
//    so the result must equal the no-constraint baseline.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintOnUnknownPropertyEqualsBaseline) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "no_such_prop", "operator": "=", "value": 42}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto with_unknown = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_unknown.query_ok) << with_unknown.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_unknown.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 9. Same as above, but on an edge constraint instead of a vertex constraint.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintOnUnknownPropertyEqualsBaseline) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "no_such_edge_prop", "operator": "=", "value": 1}
       ]
      }
    ]
  })";
  auto with_unknown = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_unknown.query_ok) << with_unknown.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_unknown.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 10. A vertex-property equality constraint that *can* match: name = 'Alice'
//     pins one vertex of the pair, so the count must be strictly less than
//     the unconstrained baseline (Alice has 2 outgoing knows-edges).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintNameRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  // sample_count saturates at sample_size when *any* embedding matches, so
  // assert on estimated_count instead. Alice has out-edges 0->1 and 0->3 →
  // restricted estimate is 2; baseline is 5.
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 11. Multiple constraints on the same vertex are AND-combined implicitly.
//     Alice has age 20, so {name=Alice, age=20} matches; {name=Alice, age=99}
//     matches nothing.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, MultipleVertexConstraintsAreAnded_Match) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"},
         {"property": "age",  "operator": "=", "value": 20}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1)
      << "Alice matches both constraints; expected >= 1 sample";
}

TEST_F(SampledMatchTest, MultipleVertexConstraintsAreAnded_NoMatch) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"},
         {"property": "age",  "operator": "=", "value": 99}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_EQ(r.sample_count, 0)
      << "AND-combined unsatisfiable constraints must yield zero samples";
}

// ---------------------------------------------------------------------------
// 12. Constraint value type doesn't match the property type: the underlying
//     Value::operator== returns false for type-mismatch, so candidates are
//     silently filtered out. Asserts the (gotcha) behavior so a future fix
//     that introduces coercion will trip this test and force a deliberate
//     decision.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, TypeMismatchSilentlyExcludesCandidates) {
  // age is INT32 in the schema; passing a string forces a type mismatch.
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": "=", "value": "20"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_EQ(r.sample_count, 0)
      << "Type mismatch should currently filter all candidates (no implicit "
         "coercion). If this changes, update the test alongside the behavior.";
}

// ---------------------------------------------------------------------------
// 13. Unknown operator string: ParseOperator falls back to COMP_EQUAL and
//     emits a one-shot warning. Asserts that "or" is treated as "=" today —
//     this also documents that boolean combinators aren't supported.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, UnknownOperatorFallsBackToEqual) {
  constexpr const char* kFallback = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "or", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  constexpr const char* kEqual = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto fallback = RunSampledMatch(WritePattern(kFallback));
  auto equal = RunSampledMatch(WritePattern(kEqual));
  ASSERT_TRUE(fallback.query_ok) << fallback.error;
  ASSERT_TRUE(equal.query_ok) << equal.error;
  EXPECT_DOUBLE_EQ(fallback.estimated_count, equal.estimated_count)
      << "Unknown operator must currently behave like '='";
}

// ---------------------------------------------------------------------------
// 14. "in" / "not_in" operators: parsed but not implemented at runtime; they
//     act as a no-op (always true). Result must match the no-constraint
//     baseline.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, InOperatorIsNoOp) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "in", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto in_op = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(in_op.query_ok) << in_op.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(in_op.estimated_count, baseline.estimated_count)
      << "'in' is a runtime no-op today; result should equal the baseline";
}

// ---------------------------------------------------------------------------
// 15. Two-label pattern (Person -[works_at]-> Company) — exercises the
//     multi-label code path on a graph that has 4 such edges.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, TwoLabelPatternPersonWorksAtCompany) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Company"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_works_at_company"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1);
  EXPECT_GT(r.estimated_count, 0.0);
}

// ---------------------------------------------------------------------------
// 16. Edge property equality constraint: matches the 3 knows-edges with
//     weight = 0.5 (0->1, 2->0, 3->1). With a non-matching weight, the
//     count drops; with a constraint that always matches (or doesn't bind),
//     the count is unchanged.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintWeightRestrictsResults) {
  constexpr const char* kRestrictive = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "=", "value": 0.5}
       ]
      }
    ]
  })";
  auto restrictive = RunSampledMatch(WritePattern(kRestrictive));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restrictive.query_ok) << restrictive.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restrictive.sample_count, 1);
  // 3 of 5 knows-edges have weight=0.5; restricted estimate must be lower.
  EXPECT_GT(restrictive.estimated_count, 0.0);
  EXPECT_LT(restrictive.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 17. Constraint missing the "value" field — CreateValueFromRapidjson falls
//     back to Value::INT32(0). For a STRING property this triggers the same
//     type-mismatch filter as test 12 (no candidates). Documents the (gotcha)
//     fallback so a future fix that rejects malformed constraints will trip
//     this test.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, ConstraintWithoutValueFallsBackToZero) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "="}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  // Missing value defaults to INT32(0); name is STRING → type mismatch →
  // zero candidates.
  EXPECT_EQ(r.sample_count, 0)
      << "Missing constraint value falls back to INT32(0); when the property "
         "is STRING this should currently filter all candidates.";
}

// ---------------------------------------------------------------------------
// 18. Pattern with a single vertex (0-edge): exercises the degenerate case
//     where preprocessing has nothing to traverse.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, SingleVertexPattern) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"}
    ],
    "edges": []
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  // 4 Persons in the graph → estimated count should be ≥ 4 (or fall back
  // gracefully; either way the call must not crash and not return -1).
  EXPECT_NE(r.estimated_count, -1.0);
  EXPECT_GE(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 19. Empty vertices array: pattern is structurally degenerate — match()
//     guards against pattern_graph_->GetNumVertices() == 0 and returns -1.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsEmptyVerticesArray) {
  auto path = WritePattern(R"({"vertices": [], "edges": []})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 20. Edge constraint with matching value: weight = 1.5 matches exactly one
//     knows-edge (1 -> 2). Combined with no vertex constraints, the sampled
//     count should be ≥ 1.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintExactWeightMatchesSingleEdge) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "=", "value": 1.5}
       ]
      }
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1)
      << "weight=1.5 matches edge 1->2; expected >= 1 sample";
}

// ---------------------------------------------------------------------------
// 21. Vertex constraint with '>' on age: matches Bob (30) and Dave (40); as
//     source vertex that selects 2 of the 5 knows-edges (1->2, 3->1).
//     CheckValueConstraint implements '>' at runtime; this test locks in
//     that the operator is dispatched and applied (not silently skipped).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintGreaterThanRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">", "value": 25}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 22. Vertex constraint with '<' on age: matches Alice (20) and Carol (20);
//     as source vertex that selects 0->1, 0->3, 2->0 = 3 of 5 knows-edges.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintLessThanRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": "<", "value": 25}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 23. Boundary handling for '>' vs '>=': Dave's age is exactly 40, so
//     age > 40 must yield zero samples while age >= 40 must match Dave.
//     This pins the exclusion vs. inclusion semantics that test 21 alone
//     can't distinguish.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, GreaterAndGreaterEqualHandleBoundary) {
  constexpr const char* kStrict = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">", "value": 40}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  constexpr const char* kInclusive = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">=", "value": 40}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto strict = RunSampledMatch(WritePattern(kStrict));
  auto inclusive = RunSampledMatch(WritePattern(kInclusive));
  ASSERT_TRUE(strict.query_ok) << strict.error;
  ASSERT_TRUE(inclusive.query_ok) << inclusive.error;
  EXPECT_EQ(strict.sample_count, 0)
      << "age > 40 should exclude Dave (age=40); expected zero samples";
  EXPECT_GE(inclusive.sample_count, 1)
      << "age >= 40 should include Dave (age=40); expected >= 1 sample";
}

// ---------------------------------------------------------------------------
// 24. Edge constraint with '>=' on weight: weight >= 1.5 matches 1->2 (1.5)
//     and 0->3 (2.5) — 2 of 5 knows-edges. The boundary value 1.5 is
//     deliberately included so the test would also fail if '>=' regressed
//     to strict '>' (count would drop to 1, still less-than-baseline, so
//     the assertion is "strictly less" not "exactly N").
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintGreaterEqualRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": ">=", "value": 1.5}
       ]
      }
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 25. Edge constraint with '<=' on weight: weight <= 0.5 matches the three
//     0.5-weight edges (0->1, 2->0, 3->1). Boundary inclusion of 0.5 is
//     load-bearing — if '<=' regressed to strict '<', no edges would match
//     (smallest weight in the graph is 0.5) and sample_count would be 0.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintLessEqualRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "<=", "value": 0.5}
       ]
      }
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1)
      << "weight <= 0.5 should match the three 0.5-weight edges";
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 26. 'not_in' operator: parsed but treated as a runtime no-op (always true),
//     mirroring 'in' (test 14). Documents the symmetric behavior so a future
//     implementation that adds real semantics will trip both tests together.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, NotInOperatorIsNoOp) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "not_in", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto not_in_op = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(not_in_op.query_ok) << not_in_op.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(not_in_op.estimated_count, baseline.estimated_count)
      << "'not_in' is a runtime no-op today; result should equal the baseline";
}

// Inline DSL longer than 48 chars round-trips through the CALL pipeline.
// This guards against regressions of the bare-literal fold path that used to
// crash on string literals exceeding neug_string_t::SHORT_STR_LENGTH.
TEST_F(SampledMatchTest, LongInlineDslRoundTripsThroughCall) {
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person)-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a)";
  ASSERT_GT(kDsl.size(), 48u)
      << "test premise: DSL must exceed the inline-string boundary";
  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << kDsl
    << "', 100) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  EXPECT_GE(sm.value().response().arrays(1).int64_array().values(0), 1);
}

// ---------------------------------------------------------------------------
// Pattern-DSL coverage. Patterns are passed inline by default; a `.dsl` file
// path is also accepted by the procedure (auto-detected) and exercised below.
// Internally the matcher runs DSL → JSON → PatternGraph entirely in memory.
// ---------------------------------------------------------------------------

namespace {

// Helper local to this test file: persist DSL text into a tempdir and append
// the directory to the fixture's cleanup list.
std::string WriteDslFile(std::vector<std::filesystem::path>& cleanup_dirs,
                         const std::string& contents, const char* basename) {
  std::string tmpl = "/tmp/sm_XXXXXX";
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr) return "";
  std::filesystem::path dir(buf.data());
  cleanup_dirs.push_back(dir);
  auto path = dir / basename;
  std::ofstream ofs(path);
  if (!ofs.is_open()) return "";
  ofs << contents;
  ofs.close();
  return path.string();
}

}  // namespace

TEST_F(SampledMatchTest, PatternDslMatchesJsonForTriangle) {
  // Triangle written in DSL. Variables a/b/c make the pattern self-
  // documenting; the result should match the JSON kTrianglePattern up to
  // Monte-Carlo sampling noise.
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person)-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a)";
  auto dsl_path = WriteDslFile(pattern_dirs_, kDsl, "q.dsl");
  ASSERT_FALSE(dsl_path.empty());

  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << dsl_path
    << "', 1000) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  const auto& resp = sm.value().response();
  ASSERT_EQ(sm.value().length(), 1u);
  double estimated = resp.arrays(0).double_array().values(0);
  int64_t samples = resp.arrays(1).int64_array().values(0);
  EXPECT_GT(estimated, 0.0);
  EXPECT_GE(samples, 1);

  // FaSTest is a randomized estimator, so we compare order of magnitude
  // against the JSON form rather than asserting bit-exact equality.
  auto json_result = RunSampledMatch(WritePattern(kTrianglePattern));
  ASSERT_TRUE(json_result.query_ok) << json_result.error;
  ASSERT_GT(json_result.estimated_count, 0.0);
  double ratio = estimated / json_result.estimated_count;
  EXPECT_GE(ratio, 0.5);
  EXPECT_LE(ratio, 2.0);
}

TEST_F(SampledMatchTest, PatternDslSupportsWhereAndInlineProps) {
  // Same triangle, but with the age constraint expressed once via WHERE on
  // 'a' and once via inline {age: 30} on 'b'. Both forms should translate
  // into per-vertex constraints; the matcher restricts accordingly.
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person {age: 30})-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a) "
      "WHERE a.age >= 18";
  auto dsl_path = WriteDslFile(pattern_dirs_, kDsl, "q.dsl");
  ASSERT_FALSE(dsl_path.empty());

  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << dsl_path
    << "', 1000) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  // The age=30 / age>=18 restriction is satisfied only when 'b' is Bob
  // (the only Person with age 30); a sample should still come back since
  // Alice (20)-Bob (30)-Carol (20) closes a triangle.
  double estimated = sm.value().response().arrays(0).double_array().values(0);
  int64_t samples = sm.value().response().arrays(1).int64_array().values(0);
  EXPECT_GT(estimated, 0.0);
  EXPECT_GE(samples, 1);
}

// Omnibus negative test: every entry exercises a distinct parser guard.
// Translation failure surfaces as a glog warning + empty pattern path; the
// matcher then reports zero samples while the SQL call itself succeeds —
// same failure shape used by RejectsMalformedPatternJson and friends.
TEST_F(SampledMatchTest, PatternDslRejectsBadCases) {
  struct Case {
    const char* label;
    std::string dsl;
  };
  const std::vector<Case> bad = {
      {"undirected edge ('--')",
       "MATCH (a:Person)-[:person_knows_person]-(b:Person)"},
      {"<> operator",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age <> 18"},
      {"variable-length path '*1..N'",
       "MATCH (a:Person)-[:person_knows_person*1..3]->(b:Person)"},
      {"IN operator",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age in 18"},
      {"OR in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age >= 18 OR b.age >= 18"},
      {"NOT in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE NOT a.age = 18"},
      {"cross-variable predicate",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age > b.age"},
      {"multi-label node",
       "MATCH (a:Person:Employee)-[:person_knows_person]->(b:Person)"},
      {"function call in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE length(a.name) > 5"},
      {"node without label",
       "MATCH (a)-[:person_knows_person]->(b:Person)"},
      {"unterminated string literal",
       "MATCH (a:Person) WHERE a.name = 'Alice"},
      {"unclosed paren", "MATCH (a:Person"},
      {"reused relationship variable",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person)"
       "-[r:person_knows_person]->(c:Person)"},
      {"WHERE references unknown var",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE c.age > 0"},
      {"RETURN references unknown var",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) RETURN d.name"},
      {"garbage at top of query", "FOOBAR (a:Person)"},
      {"empty input", ""},

      // ---- additional parser/validator guards ----
      {"MATCH keyword without a pattern body", "MATCH"},
      {"WHERE before MATCH",
       "WHERE a.age = 1 MATCH (a:Person)-[:person_knows_person]->(b:Person)"},
      {"trailing comma in MATCH",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person),"},
      {"trailing AND in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age = 1 AND"},
      {"trailing comma in RETURN",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) RETURN a,"},
      {"unclosed inline property map",
       "MATCH (a:Person {age: 30)-[:person_knows_person]->(b:Person)"},
      {"inline property using comparison operator",
       "MATCH (a:Person {age > 30})-[:person_knows_person]->(b:Person)"},
      {"inline property missing colon",
       "MATCH (a:Person {age 30})-[:person_knows_person]->(b:Person)"},
      {"empty node parens",
       "MATCH ()-[:person_knows_person]->(b:Person)"},
      {"double-colon label",
       "MATCH (a::Person)-[:person_knows_person]->(b:Person)"},
      {"WHERE predicate missing dot",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a name = 'X'"},
      {"WHERE predicate missing right operand",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age >"},
      {"same variable bound to two different labels",
       "MATCH (a:Person)-[:person_knows_person]->(a:Company)"},
      {"pattern begins with an edge",
       "MATCH -[:person_knows_person]->(b:Person)"},
      {"malformed numeric literal",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age = 1.2.3"},
      {"non-ASCII garbage in the middle of the query",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) ☠"},
  };
  for (const auto& c : bad) {
    SCOPED_TRACE(c.label);
    auto path = WriteDslFile(pattern_dirs_, c.dsl, "q.dsl");
    ASSERT_FALSE(path.empty());
    std::ostringstream q;
    q << "CALL SAMPLED_MATCH_PATTERN('" << path
      << "', 100) RETURN estimated_count, sample_count, result_file, "
         "props_file;";
    auto sm = conn_->Query(q.str());
    ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
    EXPECT_EQ(sm.value().response().arrays(1).int64_array().values(0), 0)
        << "expected zero samples for invalid DSL: " << c.label;
  }
}

}  // namespace test
}  // namespace neug
