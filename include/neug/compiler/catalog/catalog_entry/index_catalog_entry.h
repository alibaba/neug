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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/catalog/catalog_entry/catalog_entry.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/common/serializer/buffered_reader.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "table_catalog_entry.h"

namespace neug::common {
struct BufferReader;
}
namespace neug::common {
class BufferedSerializer;
}
namespace neug {
namespace catalog {

struct NEUG_API IndexToCypherInfo : ToCypherInfo {
  const main::ClientContext* context;
  const common::FileScanInfo& exportFileInfo;

  IndexToCypherInfo(const main::ClientContext* context,
                    const common::FileScanInfo& exportFileInfo)
      : context{context}, exportFileInfo{exportFileInfo} {}
};

class IndexCatalogEntry;
struct NEUG_API IndexAuxInfo {
  virtual ~IndexAuxInfo() = default;
  virtual std::shared_ptr<common::BufferedSerializer> serialize() const;

  virtual std::unique_ptr<IndexAuxInfo> copy() = 0;

  template <typename TARGET>
  TARGET& cast() {
    return dynamic_cast<TARGET&>(*this);
  }
  template <typename TARGET>
  const TARGET& cast() const {
    return dynamic_cast<const TARGET&>(*this);
  }

  virtual std::string toCypher(const IndexCatalogEntry& indexEntry,
                               const ToCypherInfo& info) const = 0;

  virtual TableCatalogEntry* getTableEntryToExport(
      const main::ClientContext* /*context*/) const {
    return nullptr;
  }
};

class NEUG_API IndexCatalogEntry final : public CatalogEntry {
 public:
  static std::string getInternalIndexName(common::table_id_t tableID,
                                          std::string indexName) {
    return common::stringFormat("{}_{}", tableID, std::move(indexName));
  }

  IndexCatalogEntry(std::string type, common::table_id_t tableID,
                    std::string indexName,
                    std::vector<common::property_id_t> properties,
                    std::unique_ptr<IndexAuxInfo> auxInfo)
      : CatalogEntry{CatalogEntryType::INDEX_ENTRY,
                     common::stringFormat("{}_{}", tableID, indexName)},
        type{std::move(type)},
        tableID{tableID},
        indexName{std::move(indexName)},
        propertyIDs{std::move(properties)},
        auxInfo{std::move(auxInfo)} {}

  std::string getIndexType() const { return type; }

  common::table_id_t getTableID() const { return tableID; }

  std::string getIndexName() const { return indexName; }

  std::vector<common::property_id_t> getPropertyIDs() const {
    return propertyIDs;
  }

  // When serializing index entries to disk, we first write the fields of the
  // base class, followed by the size (in bytes) of the auxiliary data and its
  // content.
  void serialize(common::Serializer& serializer) const override;
  // During deserialization of index entries from disk, we first read the base
  // class (IndexCatalogEntry). The auxiliary data is stored in auxBuffer, with
  // its size in auxBufferSize. Once the extension is loaded, the corresponding
  // indexes are reconstructed using the auxBuffer.
  static std::unique_ptr<IndexCatalogEntry> deserialize(
      common::Deserializer& deserializer);

  std::string toCypher(const ToCypherInfo& info) const override {
    return isLoaded() ? auxInfo->toCypher(*this, info) : "";
  }
  std::unique_ptr<IndexCatalogEntry> copy() const {
    return std::make_unique<IndexCatalogEntry>(type, tableID, indexName,
                                               propertyIDs, auxInfo->copy());
  }

  void copyFrom(const CatalogEntry& other) override;

  std::unique_ptr<common::BufferReader> getAuxBufferReader() const;

  void setAuxInfo(std::unique_ptr<IndexAuxInfo> auxInfo_);
  const IndexAuxInfo& getAuxInfo() const { return *auxInfo; }

  bool isLoaded() const { return auxBuffer == nullptr; }

  TableCatalogEntry* getTableEntryToExport(main::ClientContext* context) {
    return isLoaded() ? auxInfo->getTableEntryToExport(context) : nullptr;
  }

 protected:
  std::string type;
  common::table_id_t tableID = common::INVALID_TABLE_ID;
  std::string indexName;
  std::vector<common::property_id_t> propertyIDs;
  std::unique_ptr<uint8_t[]> auxBuffer = nullptr;
  std::unique_ptr<IndexAuxInfo> auxInfo;
  uint64_t auxBufferSize = 0;
};

}  // namespace catalog
}  // namespace neug
