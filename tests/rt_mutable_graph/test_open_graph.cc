#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/schema.h"

#include <glog/logging.h>

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

  {
    gs::NeugDB db4("", 1, "w", "gopt");
    gs::NeugDB db5("", 1, "r", "gopt");
    gs::NeugDB db6("", 1, "rw", "gopt");

    db6.close();
    LOG(INFO) << "After close db6";
    db5.close();
    LOG(INFO) << "After close db5";
    db4.close();
    LOG(INFO) << "After close db4";
  }
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
        res.status().error_code() == gs::StatusCode::ERR_CONNECTION_CLOSED)
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
    } catch (const gs::exception::DatabaseLockedException& e) {
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
    } catch (const gs::exception::DatabaseLockedException& e) {
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

bool test_update_record_view() {
  // Create a new database with multi-prop on edge, and update the edge property
  // as a whole property(RecordView).
  std::string data_path = "/tmp/test_update_record_view";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }

  {
    gs::NeugDB db(data_path, 1, "w", "gopt");
    auto conn = db.connect();

    CHECK(conn->query("CREATE NODE TABLE person(id INT64, name STRING, age "
                      "INT64, PRIMARY "
                      "KEY(id));")
              .ok());
    CHECK(conn->query("CREATE REL TABLE knows(FROM person TO person, weight "
                      "DOUBLE, since "
                      "INT64);")
              .ok());
    CHECK(conn->query("CREATE (t: person {id: 1, name: 'Alice', age: 30});")
              .ok());
    CHECK(
        conn->query("CREATE (t: person {id: 2, name: 'Bob', age: 25});").ok());
    CHECK(conn->query("CREATE (t: person {id: 3, name: 'Charlie', age: 35});")
              .ok());
    CHECK(conn->query(
                  "MATCH(u1: person), (u2: person) WHERE u1.id = 1 AND u2.id = "
                  "2 CREATE (u1)-[:knows {weight: 0.5, since: 2020}]->(u2);")
              .ok());

    auto& graph_db = db.db();
    auto txn = graph_db.GetUpdateTransaction(0);
    std::vector<gs::Any> props = {gs::Any::From<double>(0.8),
                                  gs::Any::From<int64_t>(2021)};
    gs::Record record(props);
    CHECK(txn.AddEdge(0, 0, 0, 2, 0, record));
    CHECK(txn.Commit());

    auto res = conn->query(
        "MATCH (u1: person)-[r:knows]->(u2: person) "
        "RETURN r.weight;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to query after adding edge: "
                 << res.status().ToString();
      return false;
    }
    auto res_val = res.value();
    auto row1 = res_val.next();
    CHECK(row1.ToString() == "<element { object { f64: 0.5 } }>");
    auto row2 = res_val.next();
    CHECK(row2.ToString() == "<element { object { f64: 0.8 } }>");

    // Now update the property with whole new record
    props = {gs::Any::From<double>(0.9), gs::Any::From<int64_t>(2022)};
    record = gs::Record(props);
    auto txn2 = graph_db.GetUpdateTransaction(0);
    txn2.SetEdgeData(true, 0, 0, 0, 2, 0, record, -1);
    CHECK(txn2.Commit());

    res = conn->query(
        "MATCH (u1: person)-[r:knows]->(u2: person) "
        "RETURN r.weight;");
    if (!res.ok()) {
      LOG(ERROR) << "Failed to query after adding edge: "
                 << res.status().ToString();
      return false;
    }
    res_val = res.value();
    auto row3 = res_val.next();
    CHECK(row3.ToString() == "<element { object { f64: 0.5 } }>");
    auto row4 = res_val.next();
    CHECK(row4.ToString() == "<element { object { f64: 0.9 } }>");
    return true;
  }
}

void test_temporal() {
  gs::Date date("2023-10-01");
  CHECK(date.to_timestamp() == 1696118400000);
  gs::Date date2;
  date2.from_timestamp(1696118400000);
  CHECK(date2.to_string() == "2023-10-01");

  gs::DateTime datetime("2023-10-01 12:34:56");
  LOG(INFO) << "DateTime: " << datetime.to_string();
  CHECK(datetime.to_string() == "2023-10-01 12:34:56.000");
}

void test_split_string_into_vec() {
  std::string str = "a,b,c,d";
  std::string delimeter = ",";
  auto vec = gs::split_string_into_vec(str, delimeter);
  CHECK(vec.size() == 4) << "Expected 4 elements, got " << vec.size();
  CHECK(vec[0] == "a");
  CHECK(vec[1] == "b");
  CHECK(vec[2] == "c");
  CHECK(vec[3] == "d");

  str = "a,,c,,e";
  std::string delimeter2 = ",";
  vec = gs::split_string_into_vec(str, delimeter2);
  CHECK(vec.size() == 5);
  CHECK(vec[0] == "a");
  CHECK(vec[1] == "");
  CHECK(vec[2] == "c");
  CHECK(vec[3] == "");
  CHECK(vec[4] == "e");
}

int main(int argc, char** argv) {
  // Expect 2 args, data path, and csv directory
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <data_path> <csv_dir>" << std::endl;
    return 1;
  }

  gs::setup_signal_handler();

  test_temporal();
  test_split_string_into_vec();

  test_open_close();
  LOG(INFO) << "------------------------------------";

  std::string data_path = argv[1];
  std::string csv_dir = argv[2];
  CHECK(test_database(data_path, csv_dir)) << "Database test failed.";
  LOG(INFO) << "------------------------------------";

  CHECK(test_dangling(data_path, csv_dir))
      << "Dangling connection test failed.";

  CHECK(test_rw_conflict(data_path)) << "Read-write conflict test failed.";

  CHECK(test_update_record_view()) << "Update record view test failed.";
  LOG(INFO) << "------------------------------------";
  LOG(INFO) << "All tests passed successfully.";
  return 0;
}