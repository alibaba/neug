#include "main/attached_database.h"

#include "common/exception/runtime.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace main {

void AttachedDatabase::invalidateCache() {
  if (dbType != common::ATTACHED_KUZU_DB_TYPE) {
    auto catalogExtension = catalog->ptrCast<extension::CatalogExtension>();
    catalogExtension->invalidateCache();
  }
}

void AttachedKuzuDatabase::initCatalog(const std::string& path,
                                       ClientContext* context) {}

static void validateEmptyWAL(const std::string& path, ClientContext* context) {}

AttachedKuzuDatabase::AttachedKuzuDatabase(std::string dbPath,
                                           std::string dbName,
                                           std::string dbType,
                                           ClientContext* clientContext)
    : AttachedDatabase{std::move(dbName), std::move(dbType),
                       nullptr /* catalog */} {}

}  // namespace main
}  // namespace kuzu
