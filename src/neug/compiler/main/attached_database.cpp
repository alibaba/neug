#include "neug/compiler/main/attached_database.h"

#include "neug/compiler/common/exception/runtime.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/db_config.h"
#include "neug/compiler/storage/storage_manager.h"
#include "neug/compiler/transaction/transaction_manager.h"

namespace gs {
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
}  // namespace gs
