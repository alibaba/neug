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
#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

std::vector<std::string> read_lines_from_file(const std::string& file_path) {
  std::ifstream file(file_path);
  std::vector<std::string> lines;
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      lines.push_back(line);
    }
    file.close();
  } else {
    throw std::runtime_error("Could not open file: " + file_path);
  }
  return lines;
}

TEST(StorageDDLTest, ExportTest) {
  std::string data_path = "/tmp/test_batch_loading";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db;
  db.Open(data_path);
  const char* flex_data_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (flex_data_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  std::string flex_data_dir = flex_data_dir_ptr;
  LOG(INFO) << "Flex data dir: " << flex_data_dir;
  auto conn = db.Connect();
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                  "INT64, PRIMARY "
                  "KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
      "PRIMARY "
      "KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE created(FROM person TO software, "
                  "weight DOUBLE, "
                  "since INT64);"));
  EXPECT_TRUE(
      conn->Query("COPY person from \"" + flex_data_dir + "/person.csv\";"));
  EXPECT_TRUE(conn->Query("COPY software from \"" + flex_data_dir +
                          "/software.csv\";"));
  EXPECT_TRUE(conn->Query("COPY knows from \"" + flex_data_dir +
                          "/person_knows_person.csv\" (from=\"person\", "
                          "to=\"person\");"));
  EXPECT_TRUE(conn->Query("COPY created from \"" + flex_data_dir +
                          "/person_created_software.csv\" (from=\"person\", "
                          "to=\"software\");"));

  if (std::filesystem::exists("/tmp/person.csv")) {
    std::filesystem::remove("/tmp/person.csv");
  }
  EXPECT_TRUE(
      conn->Query("COPY (MATCH (v:person) RETURN v) to "
                  "'/tmp/person.csv' (HEADER = true);"));
  // Read from file /tmp/person.csv
  {
    std::ifstream file("/tmp/person.csv");
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading.";
    std::string line;
    int count = 0;
    std::vector<std::string> expected_lines =
        read_lines_from_file(flex_data_dir + "/person_export.csv");
    while (std::getline(file, line)) {
      EXPECT_EQ(line, expected_lines[count])
          << "Line " << count << " does not match expected.";
      count++;
    }
    EXPECT_EQ(count, 5);  // Assuming there are 5 persons in the dataset
    file.close();
  }

  if (std::filesystem::exists("/tmp/person_knows_person.csv")) {
    std::filesystem::remove("/tmp/person_knows_person.csv");
  }
  EXPECT_TRUE(
      conn->Query("COPY (MATCH (v:person)-[e:knows]->(v2:person) "
                  "RETURN e) to "
                  "'/tmp/person_knows_person.csv' (HEADER = true);"));
  // Read from file /tmp/person_knows_person.csv
  {
    std::ifstream file("/tmp/person_knows_person.csv");
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading.";
    std::string line;
    int count = 0;
    std::vector<std::string> expected_lines =
        read_lines_from_file(flex_data_dir + "/person_knows_person_export.csv");
    while (std::getline(file, line)) {
      EXPECT_EQ(line, expected_lines[count])
          << "Line " << count << " does not match expected.";
      count++;
    }
    EXPECT_EQ(count, 3);  // Assuming there are 8 relationships in the dataset
    file.close();
  }
}
