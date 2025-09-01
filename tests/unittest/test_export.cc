#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "neug/main/database.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/schema.h"

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

  gs::NeugDB db(data_path, 1, "w", "gopt", "");
  const char* flex_data_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (flex_data_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  std::string flex_data_dir = flex_data_dir_ptr;
  LOG(INFO) << "Flex data dir: " << flex_data_dir;
  auto conn = db.connect();
  EXPECT_TRUE(conn->query("CREATE NODE TABLE person(id INT64, name STRING, age "
                          "INT64, PRIMARY "
                          "KEY(id));")
                  .ok());
  EXPECT_TRUE(
      conn->query(
              "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
              "PRIMARY "
              "KEY(id));")
          .ok());
  EXPECT_TRUE(
      conn->query(
              "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
          .ok());
  EXPECT_TRUE(conn->query("CREATE REL TABLE created(FROM person TO software, "
                          "weight DOUBLE, "
                          "since INT64);")
                  .ok());
  EXPECT_TRUE(
      conn->query("COPY person from \"" + flex_data_dir + "/person.csv\";")
          .ok());
  EXPECT_TRUE(
      conn->query("COPY software from \"" + flex_data_dir + "/software.csv\";")
          .ok());
  EXPECT_TRUE(conn->query("COPY knows from \"" + flex_data_dir +
                          "/person_knows_person.csv\" (from=\"person\", "
                          "to=\"person\");")
                  .ok());
  EXPECT_TRUE(conn->query("COPY created from \"" + flex_data_dir +
                          "/person_created_software.csv\" (from=\"person\", "
                          "to=\"software\");")
                  .ok());

  if (std::filesystem::exists("/tmp/person.csv")) {
    std::filesystem::remove("/tmp/person.csv");
  }
  EXPECT_TRUE(conn->query("COPY (MATCH (v:person) RETURN v) to "
                          "'/tmp/person.csv' (HEADER = true);")
                  .ok());
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
  EXPECT_TRUE(conn->query("COPY (MATCH (v:person)-[e:knows]->(v2:person) "
                          "RETURN e) to "
                          "'/tmp/person_knows_person.csv' (HEADER = true);")
                  .ok());
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
