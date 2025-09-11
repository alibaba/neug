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

#include <glog/logging.h>

TEST(DatabaseTest, OpenClose) {
  std::string dir = "/tmp/test_open_close";
  // remove the directory if it exists
  if (std::filesystem::exists(dir)) {
    std::filesystem::remove_all(dir);
  }
  // create the directory
  std::filesystem::create_directories(dir);
  // Get the path of current source file

  gs::NeugDB db;
  db.Open(dir);
  auto conn = db.Connect();
  LOG(INFO) << "Before close db1";
  db.Close();
  LOG(INFO) << "After close db1";
  gs::NeugDB db2;
  db2.Open(dir, 1, gs::DBMode::READ_ONLY);

  LOG(INFO) << "After open db2 in read-only mode";
  gs::NeugDB db3;
  db3.Open(dir, 1, gs::DBMode::READ_ONLY);
  LOG(INFO) << "After open db3 in read-only mode";

  db2.Close();
  LOG(INFO) << "After close db2";
  db3.Close();
  LOG(INFO) << "After close db3";

  {
    gs::NeugDB db4;
    db4.Open("", 1, gs::DBMode::READ_WRITE);
    gs::NeugDB db5;
    db5.Open("", 1, gs::DBMode::READ_ONLY);
    gs::NeugDB db6;
    db6.Open("", 1, gs::DBMode::READ_WRITE, "gopt");

    db6.Close();
    LOG(INFO) << "After close db6";
    db5.Close();
    LOG(INFO) << "After close db5";
    db4.Close();
    LOG(INFO) << "After close db4";
  }
}

TEST(DatabaseTest, TestDangling) {
  const char* csv_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (csv_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  LOG(INFO) << "CSV data dir: " << csv_dir_ptr;
  std::string csv_dir = csv_dir_ptr;
  std::string data_path = "/tmp/test_dangling";
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db;
  db.Open(data_path, 1, gs::DBMode::READ_WRITE);
  auto conn = db.Connect();
  EXPECT_TRUE(conn->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                          "INT64, PRIMARY "
                          "KEY(id));")
                  .ok());
  EXPECT_TRUE(conn->Query("CREATE REL TABLE knows(FROM person TO person, "
                          "weight DOUBLE);")
                  .ok());
  auto person_csv_path = csv_dir + "/person.csv";
  auto person_knows_person_csv_path = csv_dir + "/person_knows_person.csv";
  EXPECT_TRUE(
      conn->Query("COPY person from \"" + person_csv_path + "\";").ok());
  EXPECT_TRUE(
      conn->Query("COPY knows from \"" + person_knows_person_csv_path + "\";")
          .ok());
  // close database, and connection should be dangling
  db.Close();
  LOG(INFO) << "Database closed, connection should be dangling now.";
  auto res = conn->Query("MATCH (v) RETURN v;");
  // The query should fail because the connection is dangling
  EXPECT_FALSE(res.ok());
  EXPECT_EQ(res.status().error_code(), gs::StatusCode::ERR_CONNECTION_CLOSED)
      << "Expected connection to be closed, but got: "
      << res.status().ToString();
}

TEST(DatabaseTest, TestReadWriteConflict) {
  std::string data_path = "/tmp/test_rw_conflict";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);

  gs::NeugDB db;
  db.Open(data_path);
  bool res = false;
  {
    try {
      auto db2 = gs::NeugDB();
      db2.Open(data_path);
    } catch (const gs::exception::DatabaseLockedException& e) {
      LOG(INFO) << "Caught expected error: " << e.what();
      res = true;  // Expected error, test passes
    } catch (const std::exception& e) {
      LOG(ERROR) << "Unexpected exception: " << e.what();
      res = false;  // Unexpected error, test fails
    } catch (...) {
      LOG(ERROR) << "Caught an unknown exception.";
      res = false;  // Unknown error, test fails
    }
    EXPECT_TRUE(res)
        << "Expected a DatabaseLockedException when trying to open "
           "a write lock while another write lock is held.";
  }
  {
    res = false;
    try {
      auto db2 = gs::NeugDB();
      db2.Open(data_path, 1, gs::DBMode::READ_ONLY);
      LOG(ERROR) << "Expected an error when opening in read mode while write "
                    "lock is held, but got success.";
    } catch (const gs::exception::DatabaseLockedException& e) {
      LOG(INFO) << "Caught expected error: " << e.what();
      res = true;  // Expected error, test passes
    } catch (const std::exception& e) {
      LOG(ERROR) << "Unexpected exception: " << e.what();
      res = false;  // Unexpected error, test fails
    } catch (...) {
      LOG(ERROR) << "Caught an unknown exception.";
      res = false;  // Unknown error, test fails
    }
    EXPECT_TRUE(res)
        << "Expected a DatabaseLockedException when trying to open "
           "a read lock while write lock is held.";
  }
}

TEST(DatabaseTest, TestUpdateRecordView) {
  // Create a new database with multi-prop on edge, and update the edge property
  // as a whole property(RecordView).
  std::string data_path = "/tmp/test_update_record_view";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }

  {
    gs::NeugDB db;
    db.Open(data_path);
    auto conn = db.Connect();

    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                    "INT64, PRIMARY "
                    "KEY(id));")
            .ok());
    EXPECT_TRUE(
        conn->Query("CREATE REL TABLE knows(FROM person TO person, weight "
                    "DOUBLE, since "
                    "INT64);")
            .ok());
    EXPECT_TRUE(
        conn->Query("CREATE (t: person {id: 1, name: 'Alice', age: 30});")
            .ok());
    EXPECT_TRUE(
        conn->Query("CREATE (t: person {id: 2, name: 'Bob', age: 25});").ok());
    EXPECT_TRUE(
        conn->Query("CREATE (t: person {id: 3, name: 'Charlie', age: 35});")
            .ok());
    EXPECT_TRUE(
        conn->Query(
                "MATCH(u1: person), (u2: person) WHERE u1.id = 1 AND u2.id = "
                "2 CREATE (u1)-[:knows {weight: 0.5, since: 2020}]->(u2);")
            .ok());

    auto txn = db.GetUpdateTransaction(0);
    std::vector<gs::Any> props = {gs::Any::From<double>(0.8),
                                  gs::Any::From<int64_t>(2021)};
    gs::Record record(props);
    CHECK(txn.AddEdge(0, 0, 0, 2, 0, record));
    CHECK(txn.Commit());

    auto res = conn->Query(
        "MATCH (u1: person)-[r:knows]->(u2: person) "
        "RETURN r.weight;");
    EXPECT_TRUE(res.ok());
    auto res_val = res.value();
    auto row1 = res_val.next();
    EXPECT_EQ(row1.ToString(), "<element { object { f64: 0.5 } }>");
    EXPECT_TRUE(res_val.hasNext());
    auto row2 = res_val.next();
    EXPECT_EQ(row2.ToString(), "<element { object { f64: 0.8 } }>");

    // Now update the property with whole new record
    props = {gs::Any::From<double>(0.9), gs::Any::From<int64_t>(2022)};
    record = gs::Record(props);
    auto txn2 = db.GetUpdateTransaction(0);
    txn2.SetEdgeData(true, 0, 0, 0, 2, 0, record, -1);
    CHECK(txn2.Commit());

    res = conn->Query(
        "MATCH (u1: person)-[r:knows]->(u2: person) "
        "RETURN r.weight;");
    EXPECT_TRUE(res.ok());
    EXPECT_TRUE(res.value().hasNext());
    res_val = res.value();
    auto row3 = res_val.next();
    EXPECT_EQ(row3.ToString(), "<element { object { f64: 0.5 } }>");
    auto row4 = res_val.next();
    EXPECT_EQ(row4.ToString(), "<element { object { f64: 0.9 } }>");
  }
}

TEST(DatabaseTest, TestTemporal) {
  gs::Date date("2023-10-01");
  CHECK(date.to_timestamp() == 1696118400000);
  gs::Date date2;
  date2.from_timestamp(1696118400000);
  EXPECT_EQ(date2.to_string(), "2023-10-01");

  gs::DateTime datetime("2023-10-01 12:34:56");
  LOG(INFO) << "DateTime: " << datetime.to_string();
  EXPECT_EQ(datetime.to_string(), "2023-10-01 12:34:56.000");
}

TEST(DatabaseTest, TestSplitStringIntoVec) {
  std::string str = "a,b,c,d";
  std::string delimeter = ",";
  auto vec = gs::split_string_into_vec(str, delimeter);
  EXPECT_EQ(vec.size(), 4) << "Expected 4 elements, got " << vec.size();
  EXPECT_EQ(vec[0], "a");
  EXPECT_EQ(vec[1], "b");
  EXPECT_EQ(vec[2], "c");
  EXPECT_EQ(vec[3], "d");

  str = "a,,c,,e";
  std::string delimeter2 = ",";
  vec = gs::split_string_into_vec(str, delimeter2);
  EXPECT_EQ(vec.size(), 5);
  EXPECT_EQ(vec[0], "a");
  EXPECT_EQ(vec[1], "");
  EXPECT_EQ(vec[2], "c");
  EXPECT_EQ(vec[3], "");
  EXPECT_EQ(vec[4], "e");
}

TEST(DatabaseTest, TestPersist) {
  std::string db_dir = "/tmp/test_persist";
  {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    gs::NeugDB db;
    db.Open(db_dir, 1, gs::DBMode::READ_WRITE, "gopt", false, false, true);
    auto conn = db.Connect();
    std::string flex_data_dir = std::getenv("FLEX_DATA_DIR");
    EXPECT_FALSE(flex_data_dir.empty());
    EXPECT_TRUE(
        conn->Query(
                "CREATE NODE TABLE person(id INT64, name STRING, age INT64, "
                "PRIMARY KEY(id));")
            .ok());
    EXPECT_TRUE(
        conn->Query("COPY person from \"" + flex_data_dir + "/person.csv\";")
            .ok());
    db.Close();
  }
  {
    gs::NeugDB db2;
    db2.Open(db_dir, 1, gs::DBMode::READ_ONLY);
    auto conn = db2.Connect();
    auto res = conn->Query("MATCH (n: person) return n.id, n.name, n.age;");
    EXPECT_TRUE(res.ok());
    conn->Close();
    db2.Close();
  }
}