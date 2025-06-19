#pragma once

#include "src/include/catalog/catalog_entry/catalog_entry.h"

namespace gs {
namespace catalog {

class TypeCatalogEntry : public CatalogEntry {
 public:
  //===--------------------------------------------------------------------===//
  // constructors
  //===--------------------------------------------------------------------===//
  TypeCatalogEntry() = default;
  TypeCatalogEntry(std::string name, common::LogicalType type)
      : CatalogEntry{CatalogEntryType::TYPE_ENTRY, std::move(name)},
        type{std::move(type)} {}

  //===--------------------------------------------------------------------===//
  // getter & setter
  //===--------------------------------------------------------------------===//
  const common::LogicalType& getLogicalType() const { return type; }

  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<TypeCatalogEntry> deserialize(
      common::Deserializer& deserializer);

 private:
  common::LogicalType type;
};

}  // namespace catalog
}  // namespace gs
