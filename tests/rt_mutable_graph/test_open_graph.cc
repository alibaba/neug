#include <iostream>
#include <string>
#include "src/main/neug_db.h"
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

  gs::NeugDB db(dir, 1, "rw", "gopt");
  auto conn = db.connect();
  LOG(INFO) << "Before close db1";
  db.close();
  LOG(INFO) << "After close db1";
  gs::NeugDB db2(dir, 1, "r", "gopt");

  LOG(INFO) << "After open db2 in read-only mode";
  gs::NeugDB db3(dir, 1, "r", "gopt");
  LOG(INFO) << "After open db3 in read-only mode";

  db2.close();
  LOG(INFO) << "After close db2";
  db3.close();
  LOG(INFO) << "After close db3";
}

bool test_database(const std::string& data_path, const std::string& csv_dir) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "gopt");
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
  auto person_csv_path = csv_dir + "/person.csv";
  auto person_knows_person_csv_path = csv_dir + "/person_knows_person.csv";
  {
    auto res = conn->query("COPY person from \"" + person_csv_path + "\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res =
        conn->query("COPY knows from \"" + person_knows_person_csv_path + "\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy edge table: " << res.status().ToString();
      return false;
    }
  }
  auto res = conn->query("MATCH (v) RETURN count(v);");
  LOG(INFO) << "Query result: " << res.ok() << ", "
            << res.status().error_message();
  auto res_val = res.value();
  while (res_val.hasNext()) {
    auto row = res_val.next();
    LOG(INFO) << "Row: " << row.ToString();
  }
  return true;
}

bool test_dangling(const std::string& data_path, const std::string& csv_dir) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "gopt");
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
  auto person_csv_path = csv_dir + "/person.csv";
  auto person_knows_person_csv_path = csv_dir + "/person_knows_person.csv";
  {
    auto res = conn->query("COPY person from \"" + person_csv_path + "\"");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to copy node table: " << res.status().ToString();
      return false;
    }
  }
  {
    auto res =
        conn->query("COPY knows from \"" + person_knows_person_csv_path + "\"");
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

bool test_rw_conflict(const std::string& data_path) {
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db(data_path, 1, "w", "gopt");
  {
    try {
      auto db2 = gs::NeugDB(data_path, 1, "w", "gopt");
    } catch (const std::runtime_error& e) {
      LOG(INFO) << "Caught expected error: " << e.what();
      return true;  // Expected error, test passes
    } catch (const std::exception& e) {
      LOG(ERROR) << "Unexpected exception: " << e.what();
      return false;  // Unexpected error, test fails
    } catch (...) {
      LOG(ERROR) << "Caught an unknown exception.";
      return false;  // Unknown error, test fails
    }
  }
  {
    try {
      auto db2 = gs::NeugDB(data_path, 1, "r", "gopt");
      LOG(ERROR) << "Expected an error when opening in read mode while write "
                    "lock is held, but got success.";
      return false;  // Test fails if no error is thrown
    } catch (const std::runtime_error& e) {
      LOG(INFO) << "Caught expected error: " << e.what();
      return true;  // Expected error, test passes
    } catch (const std::exception& e) {
      LOG(ERROR) << "Unexpected exception: " << e.what();
      return false;  // Unexpected error, test fails
    } catch (...) {
      LOG(ERROR) << "Caught an unknown exception.";
      return false;  // Unknown error, test fails
    }
  }
}

int main(int argc, char** argv) {
  // Expect 2 args, data path, and csv directory
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <data_path> <csv_dir>" << std::endl;
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
  std::string csv_dir = argv[2];
  CHECK(test_database(data_path, csv_dir)) << "Database test failed.";
  LOG(INFO) << "------------------------------------";

  CHECK(test_dangling(data_path, csv_dir))
      << "Dangling connection test failed.";

  CHECK(test_rw_conflict(data_path)) << "Read-write conflict test failed.";
  LOG(INFO) << "------------------------------------";
  LOG(INFO) << "All tests passed successfully.";
  return 0;
}