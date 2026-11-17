/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "neug/version.h"
#include "test_helpers.h"

#include <fstream>

TEST_F(CApiTest, ConfigurationAndCloseReopen) {
  Contacts();
  EXPECT_EQ(options.read_only, 0);
  EXPECT_STREQ(neug_version(), NEUG_VERSION_STRING);
  EXPECT_EQ(neug_db_destroy(&db, &error), NEUG_ERROR_ILLEGAL_STATE);
  EXPECT_NE(db, nullptr);
  neug_error_destroy(&error);
  C_OK(neug_db_close(db, &error));
  uint8_t closed = 0;
  C_OK(neug_connection_is_closed(connection, &closed, &error));
  EXPECT_EQ(closed, 1);
  neug_result* failed = nullptr;
  EXPECT_EQ(neug_query(connection, S("RETURN 1"), {}, &failed, &error),
            NEUG_ERROR_CONNECTION_CLOSED);
  EXPECT_EQ(failed, nullptr);
  neug_error_destroy(&error);
  C_OK(neug_db_destroy(&db, &error));
  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_open(&options, &db, &error));
  C_OK(neug_db_connect(db, &connection, &error));
  Query("MATCH (p:Person) RETURN count(p);");
  EXPECT_EQ(Integer(Cell()), 2);
}

TEST(CApiValidation, NullArgumentsAndUninitializedOptions) {
  neug_error* error = nullptr;
  neug_database* db = nullptr;
  neug_db_options options{};
  EXPECT_EQ(neug_db_open(&options, &db, &error), NEUG_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(db, nullptr);
  EXPECT_GT(neug_error_message(error).size, 0u);
  neug_error_destroy(&error);
  uint64_t count = 123;
  EXPECT_EQ(neug_result_row_count(nullptr, &count, &error),
            NEUG_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(count, 0u);
  neug_error_destroy(&error);
  neug_db_options_init(&options);
  options.read_only = 2;
  EXPECT_EQ(neug_db_open(&options, &db, &error), NEUG_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(db, nullptr);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_connection_commit(nullptr, &error),
            NEUG_ERROR_INVALID_ARGUMENT);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_db_destroy(&db, &error), NEUG_OK);
  neug_error_destroy(&error);
}

TEST_F(CApiTest, ReopenedDatabaseAcceptsInsertsWithListProperties) {
  Query("CREATE NODE TABLE Item(id INT64, values INT64[], PRIMARY KEY(id));");
  Query("CREATE (:Item {id:1, values:CAST([10,20],'INT64[]')});");
  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_close(db, &error));
  C_OK(neug_db_destroy(&db, &error));
  C_OK(neug_db_open(&options, &db, &error));
  C_OK(neug_db_connect(db, &connection, &error));

  // Reloaded list storage must grow when a new nonempty value is inserted.
  Query("CREATE (:Item {id:$id, values:CAST($values,'INT64[]')});",
        R"({"id":2,"values":[30,40,50]})");
  Query("MATCH (i:Item) RETURN i.id,i.values ORDER BY i.id;");
  EXPECT_EQ(Integer(Cell(0, 0)), 1);
  EXPECT_EQ(Integer(Child(Cell(0, 1), 1)), 20);
  EXPECT_EQ(Integer(Cell(1, 0)), 2);
  EXPECT_EQ(Integer(Child(Cell(1, 1), 2)), 50);

  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_close(db, &error));
  C_OK(neug_db_destroy(&db, &error));
  C_OK(neug_db_open(&options, &db, &error));
  C_OK(neug_db_connect(db, &connection, &error));
  Query("MATCH (i:Item {id:2}) RETURN i.values;");
  EXPECT_EQ(Integer(Child(Cell(), 2)), 50);
}

TEST_F(CApiTest, ReadOnlyDatabaseRejectsWrites) {
  Contacts();
  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_close(db, &error));
  C_OK(neug_db_destroy(&db, &error));
  options.read_only = 1;
  C_OK(neug_db_open(&options, &db, &error));
  C_OK(neug_db_connect(db, &connection, &error));
  Query("MATCH (p:Person) RETURN p.name ORDER BY p.id;");
  EXPECT_EQ(String(Cell()), "Alice");
  neug_result* failed = nullptr;
  EXPECT_NE(
      neug_query(connection, S("CREATE (:Person {id:3, name:'X', age:1});"), {},
                 &failed, &error),
      NEUG_OK);
  EXPECT_EQ(failed, nullptr);
  neug_error_destroy(&error);
}

TEST_F(CApiTest, EmptyPathOpensAnIndependentTemporaryDatabase) {
  Contacts();
  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_close(db, &error));
  C_OK(neug_db_destroy(&db, &error));
  options.path = {};
  C_OK(neug_db_open(&options, &db, &error));
  C_OK(neug_db_connect(db, &connection, &error));
  Query("CALL SHOW_NODE_TABLES() RETURN *;");
  uint64_t count = 1;
  C_OK(neug_result_row_count(result, &count, &error));
  EXPECT_EQ(count, 0u);
  Contacts();
  Query("MATCH (p:Person) RETURN count(p);");
  EXPECT_EQ(Integer(Cell()), 2);
}

namespace {
class DirectoryBlock {
 public:
  explicit DirectoryBlock(std::filesystem::path directory,
                          bool as_directory = false)
      : directory_(std::move(directory)),
        backup_(directory_.string() + ".saved") {
    if (std::filesystem::exists(directory_)) {
      std::filesystem::rename(directory_, backup_);
    }
    if (as_directory)
      std::filesystem::create_directory(directory_);
    std::ofstream block(as_directory ? directory_ / "block" : directory_);
    block << "not a directory";
  }
  ~DirectoryBlock() {
    std::error_code error;
    std::filesystem::remove_all(directory_, error);
    if (std::filesystem::exists(backup_)) {
      std::filesystem::rename(backup_, directory_, error);
    }
  }

 private:
  std::filesystem::path directory_;
  std::filesystem::path backup_;
};
}  // namespace

TEST_F(CApiTest, ClosePreparationFailureKeepsHandleAndAllowsRetry) {
  Contacts();
  {
    // Preserve the live files while blocking creation of staging directories.
    // This works even when running as root, unlike permission-only injection.
    DirectoryBlock fault(dir / "runtime");
    EXPECT_NE(neug_db_close(db, &error), NEUG_OK);
    neug_error_destroy(&error);
    uint8_t closed = 1;
    C_OK(neug_db_is_closed(db, &closed, &error));
    EXPECT_EQ(closed, 0);
    EXPECT_EQ(neug_db_destroy(&db, &error), NEUG_ERROR_ILLEGAL_STATE);
    neug_error_destroy(&error);
    EXPECT_NE(db, nullptr);
    C_OK(neug_connection_is_closed(connection, &closed, &error));
    EXPECT_EQ(closed, 1);
  }
  C_OK(neug_db_close(db, &error));
  uint8_t closed = 0;
  C_OK(neug_db_is_closed(db, &closed, &error));
  EXPECT_EQ(closed, 1);
}

TEST_F(CApiTest, ClosePublicationFailureReportsClosedState) {
  Contacts();
  {
    // Staging and graph consumption can begin; the final selector rename
    // fails because CURRENT is a nonempty directory instead of a file.
    DirectoryBlock fault(dir / "checkpoint" / "CURRENT", true);
    EXPECT_NE(neug_db_close(db, &error), NEUG_OK);
    neug_error_destroy(&error);
    uint8_t closed = 0;
    C_OK(neug_db_is_closed(db, &closed, &error));
    EXPECT_EQ(closed, 1);
  }
  C_OK(neug_db_destroy(&db, &error));
  EXPECT_EQ(db, nullptr);
}
