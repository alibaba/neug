/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/storages/module/module_factory.h"
#include "test_index_common.h"

namespace neug {
namespace {

class E2EIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ModuleFactory::instance().Register(
        kVecIndexType, [] { return std::make_unique<VecIndex>(); });
  }

  void SetUp() override {
    workDir_ = std::string("/tmp/test_e2e_index_") +
               ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(workDir_);

    config_.data_dir = workDir_;
    config_.mode = DBMode::READ_WRITE;
    config_.checkpoint_on_close = false;
  }

  void TearDown() override { std::filesystem::remove_all(workDir_); }

  static void AssertNoIndexes(const QueryResult& result) {
    const auto& response = result.response();
    EXPECT_EQ(response.row_count(), 0);
    ASSERT_EQ(response.arrays_size(), 5);
    for (const auto& array : response.arrays()) {
      ASSERT_TRUE(array.has_string_array());
      EXPECT_EQ(array.string_array().values_size(), 0);
    }
  }

  static void AssertExpectedIndex(const QueryResult& result) {
    const auto& response = result.response();
    ASSERT_EQ(response.row_count(), 1);
    ASSERT_EQ(response.arrays_size(), 5);
    EXPECT_EQ(response.arrays(0).string_array().values(0),
              "entity_embedding_hnsw");
    EXPECT_EQ(response.arrays(1).string_array().values(0), "hnsw");
    EXPECT_EQ(response.arrays(2).string_array().values(0), "Entity");
    EXPECT_EQ(response.arrays(3).string_array().values(0), "embedding");
    EXPECT_EQ(
        response.arrays(4).string_array().values(0),
        R"({"description":"escapedtvalue","ef_construction":"200","m":"16","metric":"ip"})");
  }

  std::string workDir_;
  NeugDBConfig config_;
};

TEST_F(E2EIndexTest, CreateShowDropAndPersistDropAcrossReopen) {
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(config_));
    auto connection = db.Connect();
    ASSERT_NE(connection, nullptr);

    auto createTable = connection->Query(
        "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(createTable) << createTable.error().ToString();

    auto createEntity =
        connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
    ASSERT_TRUE(createEntity) << createEntity.error().ToString();

    auto createIndex = connection->Query(R"(
      CREATE INDEX entity_embedding_hnsw
      ON Entity USING HNSW (embedding)
      WITH (
        metric = 'ip',
        m = 16,
        ef_construction = 200,
        description = 'escaped\tvalue'
      );
    )");
    ASSERT_TRUE(createIndex) << createIndex.error().ToString();

    auto showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertExpectedIndex(showIndexes.value());

    auto showSelectedColumns =
        connection->Query("CALL SHOW_INDEXES() RETURN name, label;");
    ASSERT_TRUE(showSelectedColumns) << showSelectedColumns.error().ToString();
    const auto& selectedResponse = showSelectedColumns.value().response();
    ASSERT_EQ(selectedResponse.row_count(), 1);
    ASSERT_EQ(selectedResponse.arrays_size(), 2);
    EXPECT_EQ(selectedResponse.arrays(0).string_array().values(0),
              "entity_embedding_hnsw");
    EXPECT_EQ(selectedResponse.arrays(1).string_array().values(0), "Entity");

    auto showUnknownColumn =
        connection->Query("CALL SHOW_INDEXES() RETURN unknown_column;");
    EXPECT_FALSE(showUnknownColumn);

    auto createIfNotExists = connection->Query(
        "CREATE INDEX entity_embedding_hnsw IF NOT EXISTS "
        "ON Entity USING HNSW (embedding);");
    ASSERT_TRUE(createIfNotExists) << createIfNotExists.error().ToString();

    auto createExisting = connection->Query(
        "CREATE INDEX entity_embedding_hnsw "
        "ON Entity USING HNSW (embedding);");
    ASSERT_FALSE(createExisting);
    EXPECT_EQ(createExisting.error().error_code(),
              StatusCode::ERR_ILLEGAL_OPERATION);

    auto dropIndex = connection->Query("DROP INDEX entity_embedding_hnsw;");
    ASSERT_TRUE(dropIndex) << dropIndex.error().ToString();

    showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertNoIndexes(showIndexes.value());

    auto dropIfExists =
        connection->Query("DROP INDEX entity_embedding_hnsw IF EXISTS;");
    ASSERT_TRUE(dropIfExists) << dropIfExists.error().ToString();

    auto dropMissing = connection->Query("DROP INDEX entity_embedding_hnsw;");
    ASSERT_FALSE(dropMissing);
    EXPECT_EQ(dropMissing.error().error_code(), StatusCode::ERR_NOT_FOUND);

    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    db.Close();
  }

  {
    NeugDB reopened;
    ASSERT_TRUE(reopened.Open(config_));
    auto connection = reopened.Connect();
    ASSERT_NE(connection, nullptr);

    auto showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertNoIndexes(showIndexes.value());

    connection->Close();
    reopened.Close();
  }
}

TEST_F(E2EIndexTest, PersistCreatedIndexAcrossReopen) {
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(config_));
    auto connection = db.Connect();
    ASSERT_NE(connection, nullptr);

    auto createTable = connection->Query(
        "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(createTable) << createTable.error().ToString();

    auto createEntity =
        connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
    ASSERT_TRUE(createEntity) << createEntity.error().ToString();

    auto createIndex = connection->Query(R"(
      CREATE INDEX entity_embedding_hnsw
      ON Entity USING HNSW (embedding)
      WITH (
        metric = 'ip',
        m = 16,
        ef_construction = 200,
        description = 'escaped\tvalue'
      );
    )");
    ASSERT_TRUE(createIndex) << createIndex.error().ToString();

    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    db.Close();
  }

  {
    NeugDB reopened;
    ASSERT_TRUE(reopened.Open(config_));
    auto connection = reopened.Connect();
    ASSERT_NE(connection, nullptr);

    auto showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertExpectedIndex(showIndexes.value());

    connection->Close();
    reopened.Close();
  }
}

}  // namespace
}  // namespace neug
