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
#include <string>
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

    // Schema: a single Person node label and a single knows edge label.
    // Data: a 3-node directed triangle (0->1, 1->2, 2->0).
    const std::pair<std::string, std::string> setup_queries[] = {
        {"schema: Person",
         "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING);"},
        {"schema: knows",
         "CREATE REL TABLE person_knows_person(FROM Person TO Person);"},
        {"insert p0", "CREATE (n:Person {id: 0, name: 'A'})"},
        {"insert p1", "CREATE (n:Person {id: 1, name: 'B'})"},
        {"insert p2", "CREATE (n:Person {id: 2, name: 'C'})"},
        {"edge 0->1",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person]->(b)"},
        {"edge 1->2",
         "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 "
         "CREATE (a)-[:person_knows_person]->(b)"},
        {"edge 2->0",
         "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0 "
         "CREATE (a)-[:person_knows_person]->(b)"},
    };
    for (const auto& [label, q] : setup_queries) {
      auto res = conn_->Query(q);
      ASSERT_TRUE(res.has_value())
          << label << " failed: " << res.error().ToString();
    }

    auto load = conn_->Query("LOAD sampled_match;");
    ASSERT_TRUE(load.has_value()) << load.error().ToString();
    // Run a trivial follow-up query so the LOAD pipeline fully retires before
    // the test body issues CALL SAMPLED_MATCH (avoids a use-after-free seen
    // when the LOAD QueryResult is the last object torn down at SetUp exit).
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
  }

 protected:
  std::filesystem::path test_dir_;
  std::unique_ptr<neug::NeugDB> db_;
  std::shared_ptr<neug::Connection> conn_;
};

// End-to-end coverage for the sampled_match extension on a 3-node triangle:
//   1. extension LOAD,
//   2. CALL INITIALIZE() — drives DataGraphMeta::Preprocess and asserts the
//      triangle's structural counts (3 vertices, 3 edges),
//   3. CALL SAMPLED_MATCH('p.json', 1000) — runs the FaSTest cardinality
//      estimator and asserts at least one sampled embedding plus a positive
//      estimated count.
TEST_F(SampledMatchTest, LoadsExtensionAndGraphQueries) {
  auto pattern_path = test_dir_ / "triangle.json";
  {
    std::ofstream ofs(pattern_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << kTrianglePattern;
  }
  EXPECT_TRUE(std::filesystem::exists(pattern_path));

  auto count = conn_->Query("MATCH (n:Person) RETURN count(*) AS c;");
  ASSERT_TRUE(count.has_value()) << count.error().ToString();

  // Drive the algorithm: CALL INITIALIZE() runs FaSTest's full graph
  // preprocessing (label mappings + DataGraphMeta) and surfaces the vertex /
  // edge counts and structural stats. This proves the algorithm pipeline
  // loads the storage and produces sensible numbers on a known graph.
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
  EXPECT_EQ(vertices_col.values(0), 3);
  // Triangle has 3 directed edges; FaSTest preprocessing may treat them
  // undirected (3) or directed (3 + 3 reverse = 6). Accept either as long
  // as the count matches one of the two well-defined values.
  EXPECT_TRUE(edges_col.values(0) == 3 || edges_col.values(0) == 6)
      << "unexpected edge count: " << edges_col.values(0);

  // End-to-end CALL SAMPLED_MATCH. The pattern path is embedded as a string
  // literal in the query, so it must stay short (≤ neug_string_t::
  // SHORT_STR_LENGTH = 48) — the macOS temp dir alone is well past that, and
  // the long-string overflow path in the planner is currently broken
  // (InMemOverflowBuffer::allocateNewBlock is a stub, segfaults on first use).
  // Keep the file under /tmp/sm_XXXXXX/p.json so the literal stays short.
  auto short_tmpl = std::string("/tmp/sm_XXXXXX");
  std::vector<char> tbuf(short_tmpl.begin(), short_tmpl.end());
  tbuf.push_back('\0');
  ASSERT_NE(mkdtemp(tbuf.data()), nullptr);
  std::filesystem::path short_dir(tbuf.data());
  auto short_pattern = short_dir / "p.json";
  {
    std::ofstream ofs(short_pattern);
    ASSERT_TRUE(ofs.is_open());
    ofs << kTrianglePattern;
  }
  ASSERT_LE(short_pattern.string().size(), 48u)
      << "pattern path must fit in short-string budget; got: "
      << short_pattern.string();

  const std::string sm_query =
      "CALL SAMPLED_MATCH('" + short_pattern.string() +
      "', 1000) RETURN estimated_count, sample_count, result_file, props_file;";
  auto sm = conn_->Query(sm_query);
  std::error_code ec;
  std::filesystem::remove_all(short_dir, ec);
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();

  const auto& sm_qr = sm.value();
  const auto& sm_resp = sm_qr.response();
  ASSERT_EQ(sm_qr.length(), 1u);
  ASSERT_EQ(sm_resp.arrays_size(), 4);

  const auto& estimated_col = sm_resp.arrays(0).double_array();
  const auto& sample_col = sm_resp.arrays(1).int64_array();
  ASSERT_EQ(estimated_col.values_size(), 1);
  ASSERT_EQ(sample_col.values_size(), 1);

  // The triangle has exactly one undirected 3-cycle; FaSTest should sample
  // at least one embedding and report a non-zero estimated count.
  EXPECT_GE(sample_col.values(0), 1)
      << "SAMPLED_MATCH returned zero sampled embeddings on a triangle";
  EXPECT_GT(estimated_col.values(0), 0.0)
      << "SAMPLED_MATCH returned a non-positive estimated count";

  // Result file should exist and be non-empty.
  const auto& result_file_col = sm_resp.arrays(2).string_array();
  ASSERT_EQ(result_file_col.values_size(), 1);
  const auto& result_file = result_file_col.values(0);
  EXPECT_FALSE(result_file.empty());
  EXPECT_TRUE(std::filesystem::exists(result_file));
  EXPECT_GT(std::filesystem::file_size(result_file), 0u);
}

}  // namespace test
}  // namespace neug
