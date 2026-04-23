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

// Minimal end-to-end smoke test for the sampled_match extension.
//
// Flow:
//   1. Create a fresh on-disk DB.
//   2. Build a trivial schema (Person + person_knows_person).
//   3. Insert a 3-node triangle.
//   4. LOAD the extension from the build tree (NEUG_EXTENSION_HOME_PYENV is
//      discovered automatically by walking up from the test executable).
//   5. Write a triangle pattern JSON in the current working directory.
//   6. CALL SAMPLED_MATCH and require the result to be non-zero.
//   7. Clean up the pattern file and DB directory on exit.

#include <neug/main/neug_db.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

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

bool RunQuery(neug::Connection& conn, const std::string& cypher,
              const std::string& label) {
  auto res = conn.Query(cypher);
  if (!res.has_value()) {
    std::cerr << "FAILED: " << label << "\n  query: " << cypher
              << "\n  error: " << res.error().ToString() << std::endl;
    return false;
  }
  return true;
}

// Guarantee cleanup of temporary artifacts even on early failure.
struct Cleanup {
  std::string db_path;
  std::string pattern_file;
  ~Cleanup() {
    std::error_code ec;
    std::filesystem::remove(pattern_file, ec);
    std::filesystem::remove_all(db_path, ec);
  }
};

constexpr const char* kPatternJson = R"({
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

}  // namespace

int main() {
  const std::string db_path = "/tmp/neug_smd_test";
  const std::string pattern_file = "pattern.json";
  Cleanup cleanup{db_path, pattern_file};

  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);

  const std::string build_root = FindBuildRoot();
  if (build_root.empty()) {
    std::cerr << "Could not locate libsampled_match.neug_extension near "
              << GetExecutablePath() << std::endl;
    return 1;
  }
  std::cout << "Build root: " << build_root << std::endl;
  setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1);

  neug::NeugDB db;
  if (!db.Open(db_path)) {
    std::cerr << "Failed to open DB at " << db_path << std::endl;
    return 1;
  }
  auto conn = db.Connect();
  if (!conn) {
    std::cerr << "Failed to connect to DB" << std::endl;
    return 1;
  }

  const std::pair<std::string, std::string> setup_queries[] = {
      {"schema: Person", "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, "
                         "name STRING);"},
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
    if (!RunQuery(*conn, q, label)) return 1;
  }

  if (!RunQuery(*conn, "LOAD sampled_match;", "LOAD sampled_match")) return 1;

  {
    std::ofstream ofs(pattern_file);
    if (!ofs) {
      std::cerr << "Cannot write pattern file: " << pattern_file << std::endl;
      return 1;
    }
    ofs << kPatternJson;
  }

  const std::string call =
      "CALL SAMPLED_MATCH('" + pattern_file + "', 1000000) RETURN *;";
  std::cout << "Query: " << call << std::endl;
  auto res = conn->Query(call);
  if (!res.has_value()) {
    std::cerr << "SAMPLED_MATCH failed: " << res.error().ToString() << std::endl;
    return 1;
  }

  std::cout << "\n=== Result ===\n" << res.value().ToString() << std::endl;

  if (res.value().response().row_count() == 0) {
    std::cerr << "SAMPLED_MATCH returned no rows" << std::endl;
    return 1;
  }

  conn.reset();
  db.Close();

  std::cout << "\nPASSED" << std::endl;
  return 0;
}
