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

  neug::NeugDB db;
  db.Open(data_path);
  const char* flex_data_dir_ptr = std::getenv("COMPREHENSIVE_GRAPH_DATA_DIR");
  if (flex_data_dir_ptr == nullptr) {
    throw std::runtime_error(
        "COMPREHENSIVE_GRAPH_DATA_DIR environment variable is not set");
  }
  std::string flex_data_dir = flex_data_dir_ptr;
  LOG(INFO) << "Flex data dir: " << flex_data_dir;
  auto conn = db.Connect();
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE node_a(id INT64, i32_property INT32, "
                  "i64_property INT64, u32_property UINT32,u64_property "
                  "UINT64, f32_property FLOAT, f64_property DOUBLE, "
                  "str_property STRING, date_property DATE, datetime_property "
                  "TIMESTAMP, interval_property INTERVAL, "
                  "PRIMARY "
                  "KEY(id));"));
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE node_b(id INT64, i32_property INT32, "
                  "i64_property INT64, u32_property UINT32,u64_property "
                  "UINT64, f32_property FLOAT, f64_property DOUBLE, "
                  "str_property STRING, date_property DATE, datetime_property "
                  "TIMESTAMP, interval_property INTERVAL, "
                  "PRIMARY "
                  "KEY(id));"));
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE rel_a(FROM node_a TO node_a, double_weight "
                  "DOUBLE, i32_weight INT32, i64_weight INT64, datetime_weight "
                  "TIMESTAMP);"));
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE rel_b(FROM node_a TO node_b, double_weight "
                  "DOUBLE, i32_weight INT32, i64_weight INT64, datetime_weight "
                  "TIMESTAMP);"));
  EXPECT_TRUE(
      conn->Query("COPY node_a from \"" + flex_data_dir + "/node_a.csv\";"));
  EXPECT_TRUE(
      conn->Query("COPY node_b from \"" + flex_data_dir + "/node_b.csv\";"));
  EXPECT_TRUE(conn->Query("COPY rel_a from \"" + flex_data_dir +
                          "/rel_a.csv\" (from=\"node_a\", "
                          "to=\"node_a\");"));
  EXPECT_TRUE(conn->Query("COPY rel_b from \"" + flex_data_dir +
                          "/rel_b.csv\" (from=\"node_a\", "
                          "to=\"node_b\");"));

  if (std::filesystem::exists("/tmp/node_a.csv")) {
    std::filesystem::remove("/tmp/node_a.csv");
  }
  EXPECT_TRUE(
      conn->Query("COPY (MATCH (v:node_a) RETURN v.*) to "
                  "'/tmp/node_a.csv' (HEADER = true);"));
  LOG(INFO) << "Finished export";
  // Read from file /tmp/person.csv
  {
    std::ifstream file("/tmp/node_a.csv");
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading.";
    std::string line;
    int count = 0;
    std::vector<std::string> expected_lines =
        read_lines_from_file(flex_data_dir + "/node_a_export.csv");
    while (std::getline(file, line)) {
      EXPECT_EQ(line, expected_lines[count])
          << "Line " << count << " does not match expected.";
      count++;
    }
    EXPECT_EQ(count, 11);  // Assuming there are 5 persons in the dataset
    file.close();
  }

  {
    auto res = conn->Query(
        "MATCH (v:node_a)-[e:rel_a]->(v2:node_a) "
        "RETURN e;");
    ASSERT_TRUE(res);
    const auto& query_result = res.value();
    int count = query_result.length();
    LOG(INFO) << "Total knows relationships: " << count;
  }

  if (std::filesystem::exists("/tmp/rel_a.csv")) {
    std::filesystem::remove("/tmp/rel_a.csv");
  }
  EXPECT_TRUE(
      conn->Query("COPY (MATCH (v:node_a)-[e:rel_a]->(v2:node_a) "
                  "RETURN e) to "
                  "'/tmp/rel_a.csv' (HEADER = true);"));
  {
    std::ifstream file("/tmp/rel_a.csv");
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading.";
    std::string line;
    int count = 0;
    std::vector<std::string> expected_lines =
        read_lines_from_file(flex_data_dir + "/rel_a_export.csv");
    while (std::getline(file, line)) {
      EXPECT_EQ(line, expected_lines[count])
          << "Line " << count << " does not match expected.";
      count++;
    }
    EXPECT_EQ(count, 11);  // Assuming there are 8 relationships in the dataset
    file.close();
  }

  if (std::filesystem::exists("/tmp/path_a.csv")) {
    std::filesystem::remove("/tmp/path_a.csv");
  }
  EXPECT_TRUE(
      conn->Query("COPY (MATCH (v:node_a)-[e:rel_a*0..1]->(v2:node_a) "
                  "RETURN e) to "
                  "'/tmp/path_a.csv' (HEADER = true);"));
  {
    std::ifstream file("/tmp/path_a.csv");
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading.";
    std::string line;
    int count = 0;
    std::vector<std::string> expected_lines =
        read_lines_from_file(flex_data_dir + "/path_a_export.csv");
    while (std::getline(file, line)) {
      EXPECT_EQ(line, expected_lines[count])
          << "Line " << count << " does not match expected.";
      count++;
    }
    EXPECT_EQ(count, 21);
    file.close();
  }
}
