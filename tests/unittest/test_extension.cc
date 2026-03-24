#include <gtest/gtest.h>
#include <cstdlib>
#include <filesystem>
#include "column_assertions.h"
#include "neug/main/neug_db.h"

class TestJsonExtension : public ::testing::Test {
 protected:
  std::string db_path_base = "/tmp/test_json_extension_db";

  // Data dir from env: FLEX_DATA_DIR/json/vPerson.json and vPerson.jsonl
  std::string flex_data_dir;
  std::string vperson_json;
  std::string vperson_jsonl;

  void SetUp() override {
    const char* dir = std::getenv("FLEX_DATA_DIR");
    ASSERT_NE(dir, nullptr) << "FLEX_DATA_DIR environment variable is not set";
    ASSERT_NE(dir[0], '\0') << "FLEX_DATA_DIR environment variable is empty";
    flex_data_dir = dir;
    vperson_json = flex_data_dir + "/json/vPerson.json";
    vperson_jsonl = flex_data_dir + "/json/vPerson.jsonl";
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/extension_unittest_data/json/test_data");
  }

  void SetupVPersonDatabase(const std::string& db_path) {
    std::filesystem::remove_all(db_path);

    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();

    // vPerson schema: ID, fName, gender, age, eyeSight, height, etc.
    auto res = conn->Query(
        "CREATE NODE TABLE person("
        "ID INT64, "
        "fName STRING, "
        "gender INT64, "
        "age INT64, "
        "eyeSight DOUBLE, "
        "height DOUBLE, "
        "PRIMARY KEY(ID)"
        ");");
    ASSERT_TRUE(res.has_value())
        << "Failed to create person table: " << res.error().ToString();

    conn.reset();
    db.Close();
  }
};

TEST_F(TestJsonExtension, VPersonJson) {
  ASSERT_TRUE(std::filesystem::exists(vperson_json))
      << "Test file not found: " << vperson_json;

  std::string db_path = db_path_base + "_vperson_json";
  SetupVPersonDatabase(db_path);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(db_path));
  auto conn = db.Connect();
  ASSERT_TRUE(conn != nullptr);

  auto load_res = conn->Query("LOAD json");
  ASSERT_TRUE(load_res.has_value())
      << "LOAD json failed: " << load_res.error().ToString();

  std::string import_query =
      "COPY person FROM (LOAD FROM \"" + vperson_json +
      "\" RETURN ID, fName, gender, age, eyesight, height);";
  auto import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import vPerson.json failed: " << import_res.error().ToString();

  auto count_res = conn->Query("MATCH (p:person) RETURN count(p);");
  EXPECT_TRUE(count_res);
  auto count_table = count_res.value().response();
  EXPECT_EQ(count_table.row_count(), 1);
  EXPECT_EQ(count_table.arrays_size(), 1);
  neug::test::AssertInt64Column(count_table, 0, {8});

  auto q_alice = conn->Query("MATCH (p:person) WHERE p.ID = 0 RETURN p.fName;");
  EXPECT_TRUE(q_alice);
  auto q_alice_table = q_alice.value().response();
  EXPECT_EQ(q_alice_table.row_count(), 1);
  EXPECT_EQ(q_alice_table.arrays_size(), 1);
  neug::test::AssertStringColumn(q_alice_table, 0, {"Alice"});

  auto q_bob =
      conn->Query("MATCH (p:person) WHERE p.ID = 2 RETURN p.fName, p.age;");
  EXPECT_TRUE(q_bob);
  auto q_bob_table = q_bob.value().response();
  EXPECT_EQ(q_bob_table.row_count(), 1);
  EXPECT_EQ(q_bob_table.arrays_size(), 2);
  neug::test::AssertStringColumn(q_bob_table, 0, {"Bob"});
  neug::test::AssertInt64Column(q_bob_table, 1, {30});

  conn.reset();
  db.Close();
  std::filesystem::remove_all(db_path);
}

TEST_F(TestJsonExtension, VPersonJsonl) {
  ASSERT_TRUE(std::filesystem::exists(vperson_jsonl))
      << "Test file not found: " << vperson_jsonl;

  std::string db_path = db_path_base + "_vperson_jsonl";
  SetupVPersonDatabase(db_path);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(db_path));
  auto conn = db.Connect();
  ASSERT_TRUE(conn != nullptr);

  auto load_res = conn->Query("LOAD json");
  ASSERT_TRUE(load_res.has_value())
      << "LOAD json failed: " << load_res.error().ToString();

  std::string import_query = "COPY person FROM (LOAD FROM \"" + vperson_jsonl +
                             "\" RETURN ID, fName, "
                             "gender, age, eyesight, height);";
  auto import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import vPerson.jsonl failed: " << import_res.error().ToString();

  auto count_res = conn->Query("MATCH (p:person) RETURN count(p);");
  EXPECT_TRUE(count_res);
  auto count_table = count_res.value().response();
  EXPECT_EQ(count_table.row_count(), 1);
  EXPECT_EQ(count_table.arrays_size(), 1);
  neug::test::AssertInt64Column(count_table, 0, {8});

  auto q_alice = conn->Query("MATCH (p:person) WHERE p.ID = 0 RETURN p.fName;");
  EXPECT_TRUE(q_alice);
  auto q_alice_table = q_alice.value().response();
  EXPECT_EQ(q_alice_table.row_count(), 1);
  EXPECT_EQ(q_alice_table.arrays_size(), 1);
  neug::test::AssertStringColumn(q_alice_table, 0, {"Alice"});

  auto q_carol =
      conn->Query("MATCH (p:person) WHERE p.ID = 3 RETURN p.fName, p.age;");
  EXPECT_TRUE(q_carol);
  auto q_carol_table = q_carol.value().response();
  EXPECT_EQ(q_carol_table.row_count(), 1);
  EXPECT_EQ(q_carol_table.arrays_size(), 2);
  neug::test::AssertStringColumn(q_carol_table, 0, {"Carol"});
  neug::test::AssertInt64Column(q_carol_table, 1, {45});

  conn.reset();
  db.Close();
  std::filesystem::remove_all(db_path);
}
