#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/rt_mutable_graph/file_names.h"
#include "neug/storages/rt_mutable_graph/schema.h"

TEST(StorageDDLTest, CreateAndAlterTables) {
  std::string data_path = "/tmp/test_batch_loading";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);
  gs::NeugDB db(data_path, 1, "w", "gopt", "");
  // Get current directory where the .cc exists
  const char* flex_data_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (flex_data_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  std::string flex_data_dir = flex_data_dir_ptr;
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
  EXPECT_TRUE(conn->query("ALTER TABLE person ADD birthday DATE;").ok());
  EXPECT_TRUE(
      conn->query("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;").ok());
  EXPECT_FALSE(
      conn->query("ALTER TABLE person ADD name STRING;").ok());  // should fail
  EXPECT_TRUE(conn->query("ALTER TABLE knows ADD registion DATE;").ok());
  EXPECT_FALSE(conn->query("ALTER TABLE person DROP non_existing_column;")
                   .ok());  // should fail
  EXPECT_TRUE(conn->query("ALTER TABLE person DROP birthday;").ok());
  EXPECT_TRUE(conn->query("ALTER TABLE person DROP IF EXISTS birthday;").ok());
  EXPECT_TRUE(conn->query("ALTER TABLE person DROP age;").ok());
  EXPECT_TRUE(conn->query("ALTER TABLE person RENAME name TO username;").ok());
  EXPECT_TRUE(conn->query("MATCH (v:person)-[e:created]->(:software) "
                          "DELETE e;")
                  .ok());
  EXPECT_TRUE(conn->query("MATCH (v:person) DELETE v;").ok());
  EXPECT_TRUE(conn->query("DROP TABLE knows;").ok());
  {
    auto res = conn->query("MATCH (v:person) RETURN v;");
    EXPECT_TRUE(res.ok());
    auto res_val = res.value();
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn->query(
        "MATCH (v:software)<-[e:created]-(:person) "
        "RETURN e;");
    EXPECT_TRUE(res.ok());
    auto res_val = res.value();
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn->query("COPY person from \"" + flex_data_dir +
                           "/person_after_alter.csv\";");
    EXPECT_TRUE(res.ok());
  }
  {
    auto res = conn->query("MATCH (v:person) RETURN count(v);");
    EXPECT_TRUE(res.ok());
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 8 } }>");
  }
}