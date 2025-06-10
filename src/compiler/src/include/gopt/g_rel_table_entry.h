#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"

namespace gs {
namespace catalog {
class GRelTableCatalogEntry : public RelTableCatalogEntry {
 public:
  GRelTableCatalogEntry() = default;

  GRelTableCatalogEntry(std::string name,
                        common::RelMultiplicity srcMultiplicity,
                        common::RelMultiplicity dstMultiplicity,
                        common::table_id_t tableId, common::table_id_t labelId,
                        common::table_id_t srcTableID,
                        common::table_id_t dstTableID,
                        common::ExtendDirection storageDirection)
      : RelTableCatalogEntry(std::move(name), srcMultiplicity, dstMultiplicity,
                             srcTableID, dstTableID, storageDirection),
        labelId(labelId) {
    this->setOID(tableId);
  }

  common::table_id_t getLabelId() const { return this->labelId; }

 private:
  common::table_id_t labelId;
};
}  // namespace catalog
}  // namespace gs