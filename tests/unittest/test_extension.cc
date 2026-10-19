#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include "column_assertions.h"
#include "neug/main/connection.h"
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

constexpr const char* kExtensionHome = "NEUG_EXTENSION_HOME_PYENV";
static int SetExtensionHome(const char* value) {
#ifdef _WIN32
  return _putenv_s(kExtensionHome, value ? value : "");
#else
  return value ? setenv(kExtensionHome, value, 1) : unsetenv(kExtensionHome);
#endif
}

class ExtensionConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (const char* value = std::getenv(kExtensionHome)) {
      had_extension_home_ = true;
      previous_extension_home_ = value;
    }
    root_ = std::filesystem::temp_directory_path() /
            ("neug_install_config_" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root_);
    extension_home_ = root_ / "extensions_home";
    ASSERT_EQ(SetExtensionHome(extension_home_.string().c_str()), 0);
    neug::NeugDBConfig config((root_ / "db").string(), 1);
    config.checkpoint_on_close = false;
    ASSERT_TRUE(db_.Open(config));
    connection_ = db_.Connect();
  }

  void TearDown() override {
    connection_.reset();
    db_.Close();
    EXPECT_EQ(
        SetExtensionHome(had_extension_home_ ? previous_extension_home_.c_str()
                                             : nullptr),
        0);
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  std::filesystem::path root_;
  std::filesystem::path extension_home_;
  neug::NeugDB db_;
  std::shared_ptr<neug::Connection> connection_;

 private:
  bool had_extension_home_ = false;
  std::string previous_extension_home_;
};

TEST_F(ExtensionConfigTest, BuildSettingControlsInstaller) {
  // A file in place of the extension home stops the enabled installer before
  // any network request. The disabled installer must not inspect that path.
  std::ofstream(extension_home_) << "not a directory";
  for (const auto* query :
       {"INSTALL gds", "INSTALL gds FROM 'https://example.invalid/repo'"}) {
    SCOPED_TRACE(query);
    auto result = connection_->Query(query);
    ASSERT_FALSE(result);
    if (!NEUG_LITE) {
      EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_IO_ERROR);
      EXPECT_NE(result.error().error_message().find(
                    "Failed to create extension directory"),
                std::string::npos);
    } else {
      EXPECT_EQ(result.error().error_code(),
                neug::StatusCode::ERR_NOT_SUPPORTED);
    }
  }
}

TEST_F(ExtensionConfigTest, LocalLoadDoesNotAttemptInstallation) {
  auto result = connection_->Query("LOAD gds");
  ASSERT_FALSE(result);
  if (!NEUG_LITE) {
    EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_IO_ERROR);
    EXPECT_NE(result.error().error_message().find("not found"),
              std::string::npos);
  } else {
    EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_NE(result.error().error_message().find("LOAD is unavailable"),
              std::string::npos);
  }
  EXPECT_FALSE(std::filesystem::exists(extension_home_));
}

TEST_F(ExtensionConfigTest, BuildSettingControlsUninstall) {
  const auto extension_dir = extension_home_ / "extension" / "gds";
  std::filesystem::create_directories(extension_dir);
  std::ofstream(extension_dir / "libgds.neug_extension") << "local contents";
  auto result = connection_->Query("UNINSTALL gds");
  if (!NEUG_LITE) {
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_FALSE(std::filesystem::exists(extension_dir));
  } else {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_NE(result.error().error_message().find("UNINSTALL is unavailable"),
              std::string::npos);
    std::ifstream input(extension_dir / "libgds.neug_extension");
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}),
              "local contents");
  }
}
