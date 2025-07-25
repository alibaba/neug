#include "neug/compiler/extension/catalog_extension.h"

namespace gs {
namespace extension {

void CatalogExtension::invalidateCache() {
  tables = std::make_unique<catalog::CatalogSet>();
  init();
}

}  // namespace extension
}  // namespace gs
