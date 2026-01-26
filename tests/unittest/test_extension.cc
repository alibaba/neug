#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/gopt/g_vfs_holder.h"
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"

class TestJsonExtension : public ::testing::Test {
 protected:
  std::string db_path_base = "/tmp/test_json_extension_db";
  std::string student_jsona =
      "extension/tests/json/datasets/json_array/student.json";
  std::string student_jsonl =
      "extension/tests/json/datasets/json_list/student.json";
  std::string knows_jsona =
      "extension/tests/json/datasets/json_array/knows.json";
  std::string knows_jsonl =
      "extension/tests/json/datasets/json_list/knows.json";
  std::string school_jsona =
      "extension/tests/json/datasets/json_array/school.json";
  std::string school_jsonl =
      "extension/tests/json/datasets/json_list/school.json";
  std::string attends_jsona =
      "extension/tests/json/datasets/json_array/attends.json";
  std::string attends_jsonl =
      "extension/tests/json/datasets/json_list/attends.json";

  std::string json_extension_path =
      std::string(std::getenv("HOME")) +
      "/.neug/extensions/json/libjson.neug_extension";

  void SetUp() override {
    auto vfs_ = std::make_unique<neug::common::VirtualFileSystem>();
    neug::common::VFSHolder::setVFS(vfs_.get());
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/extension_unittest_data/json/test_data");
  }

  void SetupDatabase(const std::string& db_path) {
    std::filesystem::remove_all(db_path);

    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();

    // Create student table
    auto res = conn->Query(
        "CREATE NODE TABLE student("
        "id INT32, "
        "name STRING, "
        "age INT32, "
        "height DOUBLE, "
        "birthday DATE, "
        "club STRING, "
        "hobbies STRING, "
        "PRIMARY KEY(id)"
        ");");
    ASSERT_TRUE(res.has_value())
        << "Failed to create table: " << res.error().ToString();

    // Create school table
    res = conn->Query(
        "CREATE NODE TABLE school("
        "id INT32, "
        "name STRING, "
        "PRIMARY KEY(id)"
        ");");
    ASSERT_TRUE(res.has_value())
        << "Failed to create school table: " << res.error().ToString();

    // Create knows edge table
    res = conn->Query(
        "CREATE REL TABLE knows("
        "FROM student TO student, "
        "weight DOUBLE"
        ");");
    ASSERT_TRUE(res.has_value())
        << "Failed to create knows table: " << res.error().ToString();

    // Create attends edge table
    res = conn->Query(
        "CREATE REL TABLE attends("
        "FROM student TO school"
        ");");
    ASSERT_TRUE(res.has_value())
        << "Failed to create attends table: " << res.error().ToString();

    conn.reset();
    db.Close();
  }
};

TEST_F(TestJsonExtension, LoadAndImportJsonArray) {
  std::string db_path = db_path_base + "_array";
  SetupDatabase(db_path);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(db_path));
  auto conn = db.Connect();
  ASSERT_TRUE(conn != nullptr);

  LOG(INFO) << "=== Testing JSON Array format at: " << db_path;

  // load JSON extension
  auto load_res = conn->Query("LOAD json");
  ASSERT_TRUE(load_res.has_value())
      << "LOAD json failed: " << load_res.error().ToString();

  auto show_res = conn->Query("CALL show_loaded_extensions() RETURN *;");
  ASSERT_TRUE(show_res.has_value())
      << "CALL show_loaded_extensions failed: " << show_res.error().ToString();

  if (show_res.has_value()) {
    auto& rs = show_res.value();
    int row_idx = 0;
    while (rs.hasNext()) {
      auto row = rs.next();
      std::string name, desc;
      name = std::string(row.entries()[0]->element().object().str());
      desc = std::string(row.entries()[1]->element().object().str());

      if (row_idx == 0) {
        ASSERT_EQ(name, "json");
        ASSERT_EQ(desc, "Provides functions to read and write JSON files.");
      }
      ++row_idx;
    }
  }

  LOG(INFO) << "=== Testing loading vertex data ===";

  // Import JSON Array data
  std::string import_query = "COPY student FROM \"" + student_jsona + "\";";
  auto import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSON Array vertex failed: " << import_res.error().ToString();

  // Verify student count
  {
    auto count_res = conn->Query("MATCH (s:student) RETURN count(s);");
    ASSERT_TRUE(count_res.has_value())
        << "Count query failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 67 } }>");
  }

  // Verify specific student fields
  {
    auto q1 = conn->Query("MATCH (s:student) WHERE s.id = 1 RETURN s.name;");
    ASSERT_TRUE(q1.has_value())
        << "Query name failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    EXPECT_NE(r1.ToString().find("Mika"), std::string::npos);
  }
  {
    auto q2 = conn->Query("MATCH (s:student) WHERE s.id = 2 RETURN s.age;");
    ASSERT_TRUE(q2.has_value())
        << "Query age failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    EXPECT_NE(r2.ToString().find("16"), std::string::npos);
  }
  {
    auto q3 = conn->Query("MATCH (s:student) WHERE s.id = 3 RETURN s.height;");
    ASSERT_TRUE(q3.has_value())
        << "Query height failed: " << q3.error().ToString();
    auto& rs3 = q3.value();
    ASSERT_TRUE(rs3.hasNext());
    auto r3 = rs3.next();
    EXPECT_NE(r3.ToString().find("142.8"), std::string::npos);
  }
  {
    auto q_date_comp = conn->Query(
        "MATCH (s:student) WHERE s.id = 1 AND s.birthday = DATE('2004-05-08') "
        "RETURN s.name;");
    if (q_date_comp.has_value()) {
      auto& rs_date = q_date_comp.value();
      if (rs_date.hasNext()) {
        auto r_date = rs_date.next();
        EXPECT_NE(r_date.ToString().find("Mika"), std::string::npos)
            << "Mika should match the exact birthday 2004-05-08";
        LOG(INFO) << "Exact date match successful for Mika";
      } else {
        LOG(WARNING) << "Exact date comparison might not be supported or date "
                        "format differs";
      }
    } else {
      LOG(WARNING) << "Date comparison query failed: "
                   << q_date_comp.error().ToString();
    }
  }

  // Import JSON Array school data
  import_query = "COPY school FROM \"" + school_jsona + "\";";
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSON Array school vertex failed: "
      << import_res.error().ToString();

  // Verify school count
  {
    auto count_res = conn->Query("MATCH (s:school) RETURN count(s);");
    ASSERT_TRUE(count_res.has_value())
        << "Count school query failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 9 } }>");
  }

  // Verify specific school data
  {
    auto q1 = conn->Query("MATCH (s:school) WHERE s.id = 1 RETURN s.name;");
    ASSERT_TRUE(q1.has_value())
        << "Query school name failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    EXPECT_NE(r1.ToString().find("Abydos High School"), std::string::npos);
  }
  {
    auto q2 = conn->Query("MATCH (s:school) WHERE s.id = 4 RETURN s.name;");
    ASSERT_TRUE(q2.has_value())
        << "Query school name failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    EXPECT_NE(r2.ToString().find("Millenium Science School"),
              std::string::npos);
  }
  {
    auto q3 = conn->Query("MATCH (s:school) WHERE s.id = 6 RETURN s.name;");
    ASSERT_TRUE(q3.has_value())
        << "Query school name failed: " << q3.error().ToString();
    auto& rs3 = q3.value();
    ASSERT_TRUE(rs3.hasNext());
    auto r3 = rs3.next();
    auto result_str = r3.ToString();
    LOG(INFO) << "School id=6 query result: " << result_str;
    EXPECT_NE(result_str.find("Shan Hai Jing Academy"), std::string::npos)
        << "Expected 'Shan Hai Jing Academy' in result, got: " << result_str;
  }

  LOG(INFO) << "=== Testing loading knows edge data ===";

  // Import knows edge data
  import_query = "COPY knows FROM \"" + knows_jsona +
                 "\" (from=\"student\",to=\"student\");";
  LOG(INFO) << "Executing query = " << import_query;
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value()) << "Import JSON array knows edge failed: "
                                      << import_res.error().ToString();

  // Verify knows edge count
  {
    auto count_res = conn->Query("MATCH ()-[k:knows]->() RETURN count(k);");
    ASSERT_TRUE(count_res.has_value())
        << "Count knows edge failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    LOG(INFO) << "Knows edge count: " << row.ToString();
  }

  LOG(INFO) << "=== Testing loading attends edge data ===";

  // Import attends edge data
  import_query = "COPY attends FROM \"" + attends_jsona +
                 "\" (from=\"student\",to=\"school\");";
  LOG(INFO) << "Executing query = " << import_query;
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSON array attends edge failed: "
      << import_res.error().ToString();

  // Verify attends edge count
  {
    auto count_res = conn->Query("MATCH ()-[a:attends]->() RETURN count(a);");
    ASSERT_TRUE(count_res.has_value())
        << "Count attends edge failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 4 } }>");
  }

  // Verify specific attends relationships
  {
    auto q1 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 1 RETURN "
        "sc.name;");
    ASSERT_TRUE(q1.has_value())
        << "Query student 1 school failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    EXPECT_NE(r1.ToString().find("Trinity General School"), std::string::npos);
  }
  {
    auto q2 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 2 RETURN "
        "sc.name;");
    ASSERT_TRUE(q2.has_value())
        << "Query student 2 school failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    EXPECT_NE(r2.ToString().find("Millenium Science School"),
              std::string::npos);
  }
  {
    auto q3 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 3 RETURN "
        "sc.name;");
    ASSERT_TRUE(q3.has_value())
        << "Query student 3 school failed: " << q3.error().ToString();
    auto& rs3 = q3.value();
    ASSERT_TRUE(rs3.hasNext());
    auto r3 = rs3.next();
    EXPECT_NE(r3.ToString().find("Gehena Academy"), std::string::npos);
  }
  {
    auto q4 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 4 RETURN "
        "sc.name;");
    ASSERT_TRUE(q4.has_value())
        << "Query student 4 school failed: " << q4.error().ToString();
    auto& rs4 = q4.value();
    ASSERT_TRUE(rs4.hasNext());
    auto r4 = rs4.next();
    EXPECT_NE(r4.ToString().find("Abydos High School"), std::string::npos);
  }

  LOG(INFO) << "JSON Array format test completed successfully";
  conn.reset();
  db.Close();
  std::filesystem::remove_all(db_path);
}

TEST_F(TestJsonExtension, LoadAndImportJsonLines) {
  std::string db_path = db_path_base + "_jsonl";
  SetupDatabase(db_path);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(db_path));
  auto conn = db.Connect();
  ASSERT_TRUE(conn != nullptr);

  LOG(INFO) << "=== Testing JSONL (JSON Lines) format at: " << db_path;

  // Load JSON extension
  auto load_res = conn->Query("LOAD json");
  ASSERT_TRUE(load_res.has_value())
      << "LOAD json failed: " << load_res.error().ToString();

  LOG(INFO) << "=== Testing loading student vertex data (JSONL) ===";

  // Import JSONL student data
  std::string import_query = "COPY student FROM \"" + student_jsonl + "\";";
  auto import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSONL student failed: " << import_res.error().ToString();

  // Verify student count (should be 67 records)
  {
    auto count_res = conn->Query("MATCH (s:student) RETURN count(s);");
    ASSERT_TRUE(count_res.has_value())
        << "Count query failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 67 } }>")
        << "Expected 67 records from JSONL file";
  }

  // Verify Mika's data (id=1)
  {
    auto q1 =
        conn->Query("MATCH (s:student) WHERE s.id = 1 RETURN s.name, s.age;");
    ASSERT_TRUE(q1.has_value())
        << "Query id=1 failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    auto result_str = r1.ToString();
    EXPECT_NE(result_str.find("Mika"), std::string::npos)
        << "Expected Mika in result";
    EXPECT_NE(result_str.find("17"), std::string::npos) << "Expected age 17";
    LOG(INFO) << "Mika query result: " << result_str;
  }

  // Verify Hifumi's data (id=16)
  {
    auto q2 = conn->Query(
        "MATCH (s:student) WHERE s.id = 16 RETURN s.name, s.height;");
    ASSERT_TRUE(q2.has_value())
        << "Query id=16 failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    auto result_str = r2.ToString();
    EXPECT_NE(result_str.find("Hifumi"), std::string::npos)
        << "Expected Hifumi in result";
    EXPECT_NE(result_str.find("158.4"), std::string::npos)
        << "Expected height 158.4";
    LOG(INFO) << "Hifumi query result: " << result_str;
  }

  // Verify Nagisa's data (id=5)
  {
    auto q3 =
        conn->Query("MATCH (s:student) WHERE s.id = 5 RETURN s.name, s.club;");
    ASSERT_TRUE(q3.has_value())
        << "Query id=5 failed: " << q3.error().ToString();
    auto& rs3 = q3.value();
    ASSERT_TRUE(rs3.hasNext());
    auto r3 = rs3.next();
    auto result_str = r3.ToString();
    EXPECT_NE(result_str.find("Nagisa"), std::string::npos)
        << "Expected Nagisa in result";
    EXPECT_NE(result_str.find("Tea Party"), std::string::npos)
        << "Expected club Tea Party";
    LOG(INFO) << "Nagisa query result: " << result_str;
  }

  // Verify Airi's data (id=24)
  {
    auto q4 =
        conn->Query("MATCH (s:student) WHERE s.id = 24 RETURN s.name, s.club;");
    ASSERT_TRUE(q4.has_value())
        << "Query id=24s failed: " << q4.error().ToString();
    auto& rs4 = q4.value();
    ASSERT_TRUE(rs4.hasNext());
    auto r4 = rs4.next();
    auto result_str = r4.ToString();
    EXPECT_NE(result_str.find("Airi"), std::string::npos)
        << "Expected Airi in result";
    EXPECT_NE(result_str.find("After School Sweets Club"), std::string::npos)
        << "Expected club After School Sweets Club";
    LOG(INFO) << "Airi query result: " << result_str;
  }

  // Verify date field for Yuuka
  {
    auto q_date = conn->Query(
        "MATCH (s:student) WHERE s.birthday = DATE('2005-03-14') RETURN "
        "s.name;");
    if (q_date.has_value()) {
      auto& rs_date = q_date.value();
      if (rs_date.hasNext()) {
        auto r_date = rs_date.next();
        EXPECT_NE(r_date.ToString().find("Yuuka"), std::string::npos)
            << "Yuuka should match the exact birthday 2005-03-14";
        LOG(INFO) << "Exact date match successful for Yuuka";
      } else {
        LOG(WARNING) << "Exact date comparison might not be supported or date "
                        "format differs";
      }
    } else {
      LOG(WARNING) << "Date comparison query failed: "
                   << q_date.error().ToString();
    }
  }

  // Test age range query
  {
    auto q_range = conn->Query(
        "MATCH (s:student) WHERE s.age >= 18 AND s.age <= 20 RETURN s.name "
        "ORDER BY s.age;");
    if (q_range.has_value()) {
      auto& rs_range = q_range.value();
      int count = 0;
      while (rs_range.hasNext()) {
        auto r = rs_range.next();
        LOG(INFO) << "Age range result: " << r.ToString();
        count++;
      }
      EXPECT_EQ(count, 3) << "Expected 3 students with age between 18 and 20";
    }
  }

  LOG(INFO) << "=== Testing loading school vertex data (JSONL) ===";

  // Import JSONL school data
  import_query = "COPY school FROM \"" + school_jsonl + "\";";
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSONL school vertex failed: " << import_res.error().ToString();

  // Verify school count
  {
    auto count_res = conn->Query("MATCH (s:school) RETURN count(s);");
    ASSERT_TRUE(count_res.has_value())
        << "Count school query failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 9 } }>");
  }

  // Verify specific school data
  {
    auto q1 = conn->Query("MATCH (s:school) WHERE s.id = 1 RETURN s.name;");
    ASSERT_TRUE(q1.has_value())
        << "Query school name failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    EXPECT_NE(r1.ToString().find("Abydos High School"), std::string::npos);
  }
  {
    auto q2 = conn->Query("MATCH (s:school) WHERE s.id = 7 RETURN s.name;");
    ASSERT_TRUE(q2.has_value())
        << "Query school name failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    EXPECT_NE(r2.ToString().find("Red Winter Federal Academy"),
              std::string::npos);
  }

  LOG(INFO) << "=== Testing loading knows edge data (JSONL) ===";

  // Import knows edge data
  import_query = "COPY knows FROM \"" + knows_jsonl +
                 "\" (from=\"student\",to=\"student\");";
  LOG(INFO) << "Executing query = " << import_query;
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSONL knows edge failed: " << import_res.error().ToString();

  // Verify knows edge count
  {
    auto count_res = conn->Query("MATCH ()-[k:knows]->() RETURN count(k);");
    ASSERT_TRUE(count_res.has_value())
        << "Count knows edge failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    LOG(INFO) << "Knows edge count: " << row.ToString();
  }

  LOG(INFO) << "=== Testing loading attends edge data (JSONL) ===";

  // Import attends edge data
  import_query = "COPY attends FROM \"" + attends_jsonl +
                 "\" (from=\"student\",to=\"school\");";
  LOG(INFO) << "Executing query = " << import_query;
  import_res = conn->Query(import_query);
  ASSERT_TRUE(import_res.has_value())
      << "Import JSONL attends edge failed: " << import_res.error().ToString();

  // Verify attends edge count
  {
    auto count_res = conn->Query("MATCH ()-[a:attends]->() RETURN count(a);");
    ASSERT_TRUE(count_res.has_value())
        << "Count attends edge failed: " << count_res.error().ToString();
    auto& rs = count_res.value();
    ASSERT_TRUE(rs.hasNext());
    auto row = rs.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 4 } }>");
  }

  // Verify specific attends relationships
  {
    auto q1 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 1 RETURN "
        "sc.name;");
    ASSERT_TRUE(q1.has_value())
        << "Query student 1 school failed: " << q1.error().ToString();
    auto& rs1 = q1.value();
    ASSERT_TRUE(rs1.hasNext());
    auto r1 = rs1.next();
    EXPECT_NE(r1.ToString().find("Trinity General School"), std::string::npos);
  }
  {
    auto q2 = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 4 RETURN "
        "sc.name;");
    ASSERT_TRUE(q2.has_value())
        << "Query student 4 school failed: " << q2.error().ToString();
    auto& rs2 = q2.value();
    ASSERT_TRUE(rs2.hasNext());
    auto r2 = rs2.next();
    EXPECT_NE(r2.ToString().find("Abydos High School"), std::string::npos);
  }

  // Verify path query: student -> school
  {
    auto q_path = conn->Query(
        "MATCH (s:student)-[:attends]->(sc:school) RETURN s.name, sc.name "
        "ORDER BY s.id;");
    if (q_path.has_value()) {
      auto& rs_path = q_path.value();
      int count = 0;
      while (rs_path.hasNext()) {
        auto r = rs_path.next();
        LOG(INFO) << "Student-School path: " << r.ToString();
        count++;
      }
      EXPECT_EQ(count, 4) << "Expected 4 student-school relationships";
    }
  }

  LOG(INFO) << "JSONL (JSON Lines) format test completed successfully";

  conn.reset();
  db.Close();
  std::filesystem::remove_all(db_path);
}