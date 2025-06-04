#include <iostream>
#include <string>
#include "src/main/nexg_db.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/schema.h"

void test_open_close() {
  std::string dir = "/tmp/test_open_close";
  // remove the directory if it exists
  if (std::filesystem::exists(dir)) {
    std::filesystem::remove_all(dir);
  }
  // create the directory
  std::filesystem::create_directories(dir);
  // Get the path of current source file
  LOG(INFO) << "Current source file path: " << __FILE__;
  LOG(INFO) << "Current source file directory: "
            << std::filesystem::path(__FILE__).parent_path();
  std::string cur_dir = std::filesystem::current_path().string();
  std::string compiler_jar_path =
      cur_dir + "../../tools/python_bind/nexg/resources/compiler.jar";
  LOG(INFO) << "Compiler JAR path: " << compiler_jar_path;
  std::string planner_config_path =
      cur_dir + "../../tools/python_bind/nexg/resources/planner_config.yaml";
  LOG(INFO) << "Planner config path: " << planner_config_path;
  std::string resource_path =
      cur_dir + "../../tools/python_bind/nexg/resources";
  LOG(INFO) << "Resource path: " << resource_path;

  gs::NexgDB db(dir, 1, "rw", "jni", compiler_jar_path, planner_config_path,
                resource_path);
  auto conn = db.connect();
  LOG(INFO) << "Before close db1";
  db.close();
  LOG(INFO) << "After close db1";
  gs::NexgDB db2(dir, 1, "r", "jni", compiler_jar_path, planner_config_path,
                 resource_path);
  LOG(INFO) << "After open db2 in read-only mode";
  gs::NexgDB db3(dir, 1, "r", "jni", compiler_jar_path, planner_config_path,
                 resource_path);
  LOG(INFO) << "After open db3 in read-only mode";

  db2.close();
  LOG(INFO) << "After close db2";
  db3.close();
  LOG(INFO) << "After close db3";
}

bool test_database(const std::string& data_path) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NexgDB db(data_path, 1, "w", "dummy", "", "", "");
  auto conn = db.connect();
  {
    auto res = conn->query(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create edge table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query("COPY person from \"person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query(
        "COPY knows [person->person] from \"person_knows_person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy edge table: " << res.status().ToString();
      return false;
    }
  }
  auto res = conn->query("MATCH (v) RETURN v;");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  auto res_val = res.value();
  while (res_val.hasNext()) {
    auto row = res_val.next();
    LOG(INFO) << "Row: " << row.ToString();
  }
  return true;
}

bool test_dangling(const std::string& data_path) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NexgDB db(data_path, 1, "w", "dummy", "", "", "");
  auto conn = db.connect();
  {
    auto res = conn->query(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to create edge table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query("COPY person from \"person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res = conn->query(
        "COPY knows [person->person] from \"person_knows_person.csv\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy edge table: " << res.status().ToString();
      return false;
    }
  }

  // close database, and connection should be dangling
  db.close();
  LOG(INFO) << "Database closed, connection should be dangling now.";
  auto res = conn->query("MATCH (v) RETURN v;");
  CHECK(!res.ok() &&
        res.status().error_code() == gs::StatusCode::ERR_CONNECTION_BROKEN)
      << "Expected connection to be broken, but got: "
      << res.status().ToString();
  return true;
}

int main(int argc, char** argv) {
  // Expect 1 args, data path
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <data_path>" << std::endl;
    return 1;
  }

  gs::setup_signal_handler();

  // Test parse date
  std::string date_str = "1900-01-01";
  gs::Date date(date_str);
  LOG(INFO) << "date str: " << date.to_string();

  test_open_close();
  LOG(INFO) << "------------------------------------";

  std::string data_path = argv[1];
  CHECK(test_database(data_path)) << "Database test failed.";
  LOG(INFO) << "------------------------------------";

  CHECK(test_dangling(data_path)) << "Dangling connection test failed.";
  LOG(INFO) << "------------------------------------";
  LOG(INFO) << "All tests passed successfully.";
  return 0;
}