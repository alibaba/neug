/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gopt_test.h"
#include "neug/compiler/main/option_config.h"
#include "neug/storages/graph/graph_stats.h"

namespace neug {
namespace gopt {

class MetaDataTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/ldbc_schema.yaml");
  std::string statsData = getGOptResource("stats/ldbc_0.1_statistics.json");

  common::cardinality_t getTableCard(main::ClientContext* ctx,
                                     const std::string& tableName) {
    auto catalog = ctx->getCatalog();
    auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
    auto tableEntry = catalog->getTableCatalogEntry(&transaction, tableName);
    return ctx->getGraphStats()->getTableCardinality(tableEntry);
  }
};

TEST_F(MetaDataTest, GCataLog) {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  auto schemaResult = Schema::LoadFromYamlNode(YAML::Load(schemaData));
  ASSERT_TRUE(schemaResult) << schemaResult.error().ToString();
  auto schema = std::move(schemaResult).value();
  neug::catalog::GCatalog catalog;
  auto clonedCatalog = catalog.clone(&schema);
  auto entry = clonedCatalog->getTableCatalogEntry(&transaction, "KNOWS");
  auto knowsEntry = static_cast<EdgeSchema*>(entry);
  ASSERT_EQ("KNOWS", knowsEntry->edge_label_name);
  ASSERT_EQ(8, knowsEntry->getLabelId());
  ASSERT_EQ(1, knowsEntry->getSrcTableID());
  ASSERT_EQ(1, knowsEntry->getDstTableID());
  auto groupEntry = clonedCatalog->getRelGroupEntry(&transaction, "HASCREATOR");
  ASSERT_EQ(2, groupEntry.size());
  std::vector<
      std::tuple<common::table_id_t, common::table_id_t, common::table_id_t>>
      expected = {
          {2, 0, 1},  // COMMENT -> PERSON
          {3, 0, 1}   // POST -> PERSON
      };
  for (auto hasCreatorEntry : groupEntry) {
    auto triplet = std::make_tuple(hasCreatorEntry->getSrcTableID(),
                                   hasCreatorEntry->getLabelId(),
                                   hasCreatorEntry->getDstTableID());
    auto it = std::find(expected.begin(), expected.end(), triplet);
    ASSERT_TRUE(it != expected.end())
        << "Unexpected triplet: {" << std::get<0>(triplet) << ", "
        << std::get<1>(triplet) << ", " << std::get<2>(triplet) << "}";
  }
}

TEST_F(MetaDataTest, GStorageManager) {
  auto database = std::make_unique<main::MetadataManager>();
  auto ctx = std::make_unique<main::ClientContext>(database.get());
  auto schemaResult = Schema::LoadFromYamlNode(YAML::Load(schemaData));
  ASSERT_TRUE(schemaResult) << schemaResult.error().ToString();
  auto schema = std::move(schemaResult).value();
  GraphStats stats;
  database = database->clone(&schema, stats);
  ctx = std::make_unique<main::ClientContext>(database.get());
  auto& catalog = *ctx->getCatalog();
  auto storageManager = ctx->getGraphStats();
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  auto entry = catalog.getTableCatalogEntry(&transaction, "KNOWS");
  ASSERT_EQ(storageManager->getTableCardinality(entry), 1);
  auto entry2 = catalog.getTableCatalogEntry(&transaction, "COMMENT");
  ASSERT_EQ(storageManager->getTableCardinality(entry2), 1);
  auto entry3 =
      catalog.getTableCatalogEntry(&transaction, "HASCREATOR_COMMENT_PERSON");
  ASSERT_EQ(storageManager->getTableCardinality(entry3), 1);
}

// update schema but stats unchanged, check if the stats returned as expected
// there are 3 ways to update schema:
// 1. add new node type
// 2. remove existing node type
// 3. replace existing node type with a new one, i.e. the same label id but type
// name is changed. check if the stats returned as expected for all 3 ways
TEST_F(MetaDataTest, CheckStats) {
  std::string beforeSchema = getGOptResource("schema/modern_schema.yaml");
  std::string beforeStats = getGOptResource("stats/modern_stats.json");
  std::string afterSchema = getGOptResource("schema/modern_schema_v2.yaml");
  std::string afterStats = getGOptResource("stats/modern_stats_v2.json");
  auto database = std::make_unique<main::MetadataManager>();
  auto ctx = std::make_unique<main::ClientContext>(database.get());
  auto beforeSchemaResult = Schema::LoadFromYamlNode(YAML::Load(beforeSchema));
  ASSERT_TRUE(beforeSchemaResult) << beforeSchemaResult.error().ToString();
  auto beforeSchemaObj = std::move(beforeSchemaResult).value();
  GraphStats beforeGraphStats;
  database = database->clone(&beforeSchemaObj, beforeGraphStats);
  ctx = std::make_unique<main::ClientContext>(database.get());
  ASSERT_EQ(getTableCard(ctx.get(), "person"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "software"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "created"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "knows"), 1);

  // check the statistics after schema update
  auto afterSchemaResult = Schema::LoadFromYamlNode(YAML::Load(afterSchema));
  ASSERT_TRUE(afterSchemaResult) << afterSchemaResult.error().ToString();
  auto afterSchemaObj = std::move(afterSchemaResult).value();
  database = database->clone(&afterSchemaObj, beforeGraphStats);
  ctx = std::make_unique<main::ClientContext>(database.get());
  // person is not updated
  ASSERT_EQ(getTableCard(ctx.get(), "person"), 1);
  // add a new label 'person_v2'
  ASSERT_EQ(getTableCard(ctx.get(), "person_v2"), 1);
  // knows is not updated
  ASSERT_EQ(getTableCard(ctx.get(), "knows"), 1);
  // add a new label 'knows_v2', it has two kinds of <src, dst> pairs
  ASSERT_EQ(getTableCard(ctx.get(), "knows_v2_person_person"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "knows_v2_person_person_v2"), 1);

  // check the statistics after schema and stats are updated
  GraphStats afterGraphStats;
  database = database->clone(&afterSchemaObj, afterGraphStats);
  ctx = std::make_unique<main::ClientContext>(database.get());
  ASSERT_EQ(getTableCard(ctx.get(), "person"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "person_v2"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "knows"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "knows_v2_person_person"), 1);
  ASSERT_EQ(getTableCard(ctx.get(), "knows_v2_person_person_v2"), 1);
}

}  // namespace gopt
}  // namespace neug
