#include "gopt_test.h"

namespace gs {
namespace gopt {

class MetaDataTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/ldbc_schema.yaml");
  std::string statsData = getGOptResource("stats/ldbc_0.1_statistics.json");
};

TEST_F(MetaDataTest, GCataLog) {
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  gs::catalog::GCatalog catalog(schemaData);
  auto entry = catalog.getTableCatalogEntry(&transaction, "KNOWS");
  auto knowsEntry = entry->constPtrCast<catalog::GRelTableCatalogEntry>();
  ASSERT_EQ("KNOWS", knowsEntry->getName());
  ASSERT_EQ(8, knowsEntry->getLabelId());
  ASSERT_EQ(1, knowsEntry->getSrcTableID());
  ASSERT_EQ(1, knowsEntry->getDstTableID());
  auto groupEntry = catalog.getRelGroupEntry(&transaction, "HASCREATOR");
  ASSERT_EQ(2, groupEntry->getRelTableIDs().size());
  std::vector<
      std::tuple<common::table_id_t, common::table_id_t, common::table_id_t>>
      expected = {
          {2, 0, 1},  // COMMENT -> PERSON
          {3, 0, 1}   // POST -> PERSON
      };
  for (auto relTableID : groupEntry->getRelTableIDs()) {
    auto entry = catalog.getTableCatalogEntry(&transaction, relTableID);
    auto hasCreatorEntry =
        entry->constPtrCast<catalog::GRelTableCatalogEntry>();
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
  gs::catalog::GCatalog catalog(schemaData);
  storage::MemoryManager memoryManager;
  storage::WAL wal;
  gs::storage::GStorageManager storageManager(statsData, catalog, memoryManager,
                                              wal);
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  auto entry = catalog.getTableCatalogEntry(&transaction, "KNOWS");
  auto knowsTable = storageManager.getTable(entry->getTableID());
  ASSERT_EQ(knowsTable->getNumTotalRows(&transaction), 14073);
  auto entry2 = catalog.getTableCatalogEntry(&transaction, "COMMENT");
  auto commentTable = storageManager.getTable(entry2->getTableID())
                          ->ptrCast<storage::GNodeTable>();
  ASSERT_EQ(commentTable->getStats(&transaction).getTableCard(), 151043);
  auto entry3 =
      catalog.getTableCatalogEntry(&transaction, "HASCREATOR_COMMENT_PERSON");
  auto hasCreatorTable = storageManager.getTable(entry3->getTableID());
  ASSERT_EQ(hasCreatorTable->getNumTotalRows(&transaction), 151043);
}

}  // namespace gopt
}  // namespace gs