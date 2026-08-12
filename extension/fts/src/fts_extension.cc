#include "fts_extension.h"

#include "fts_function.h"
#include "fts_index_scan.h"
#include "neug/compiler/extension/extension_api.h"

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerFunction<
      neug::fts_ext::FTSBM25Function>(
      neug::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerFunction<
      neug::fts_ext::FTSIndexScanFunction>(
      neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerRule<
      neug::fts_ext::FTSIndexScanOptimizer>(
      neug::catalog::CatalogEntryType::RULE_ENTRY);
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          neug::fts_ext::kExtensionCatalogName,
          "Provides FTS indexes and full-text search."});
}

const char* Name() { return neug::fts_ext::kExtensionName; }

}  // extern "C"
