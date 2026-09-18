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

#include "neug/compiler/catalog/catalog_set.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/string_format.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace catalog {

CatalogSet::CatalogSet(bool isInternal) {
  if (isInternal) {
    nextOID = INTERNAL_CATALOG_SET_START_OID;
  }
}

bool CatalogSet::containsEntry(const std::string& name) {
  std::shared_lock lck{mtx};
  return containsEntryNoLock(name);
}

bool CatalogSet::containsEntryNoLock(const std::string& name) const {
  return entries.contains(name);
}

CatalogEntry* CatalogSet::getEntry(const std::string& name) {
  std::shared_lock lck{mtx};
  return getEntryNoLock(name);
}

CatalogEntry* CatalogSet::getEntryNoLock(const std::string& name) const {
  // LCOV_EXCL_START
  validateExistNoLock(name);
  // LCOV_EXCL_STOP
  const auto entry = entries.at(name).get();
  NEUG_ASSERT(entry != nullptr);
  return entry;
}

oid_t CatalogSet::createEntry(std::unique_ptr<CatalogEntry> entry) {
  CatalogEntry* entryPtr = nullptr;
  oid_t oid = INVALID_OID;
  {
    std::unique_lock lck{mtx};
    oid = nextOID++;
    entry->setOID(oid);
    entryPtr = createEntryNoLock(std::move(entry));
  }
  NEUG_ASSERT(entryPtr);
  return oid;
}

CatalogEntry* CatalogSet::createEntryNoLock(
    std::unique_ptr<CatalogEntry> entry) {
  // LCOV_EXCL_START
  validateNotExistNoLock(entry->getName());
  // LCOV_EXCL_STOP
  auto* entryPtr = entry.get();
  emplaceNoLock(std::move(entry));
  return entryPtr;
}

void CatalogSet::emplaceNoLock(std::unique_ptr<CatalogEntry> entry) {
  if (entries.contains(entry->getName())) {
    entries.erase(entry->getName());
  }
  entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::eraseNoLock(const std::string& name) { entries.erase(name); }

void CatalogSet::dropEntry(const std::string& name, oid_t oid) {
  std::unique_lock lck{mtx};
  dropEntryNoLock(name, oid);
}

void CatalogSet::dropEntryNoLock(const std::string& name, oid_t oid) {
  // LCOV_EXCL_START
  validateExistNoLock(name);
  // LCOV_EXCL_STOP
  auto* entry = entries.at(name).get();
  NEUG_ASSERT(entry->getOID() == oid);
  eraseNoLock(name);
}

CatalogEntrySet CatalogSet::getEntries() {
  CatalogEntrySet result;
  std::shared_lock lck{mtx};
  for (auto& [name, entry] : entries) {
    result.emplace(name, entry.get());
  }
  return result;
}

CatalogEntry* CatalogSet::getEntryOfOID(oid_t oid) {
  for (auto& [_, entry] : entries) {
    if (entry->getOID() != oid) {
      continue;
    }
    return entry.get();
  }
  return nullptr;
}

void CatalogSet::serialize(Serializer serializer) const {
  std::vector<CatalogEntry*> entriesToSerialize;
  for (auto& [_, entry] : entries) {
    switch (entry->getType()) {
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
    case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
    case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
    case CatalogEntryType::COPY_FUNCTION_ENTRY:
    case CatalogEntryType::TABLE_FUNCTION_ENTRY:
    case CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY:
    case CatalogEntryType::RULE_ENTRY:
      continue;
    default:
      entriesToSerialize.push_back(entry.get());
    }
  }
  serializer.writeDebuggingInfo("nextOID");
  serializer.serializeValue<oid_t>(nextOID);
  serializer.writeDebuggingInfo("numEntries");
  const uint64_t numEntriesToSerialize = entriesToSerialize.size();
  serializer.serializeValue<uint64_t>(numEntriesToSerialize);
  for (const auto entry : entriesToSerialize) {
    entry->serialize(serializer);
  }
}

std::unique_ptr<CatalogSet> CatalogSet::deserialize(
    Deserializer& deserializer) {
  std::string debuggingInfo;
  auto catalogSet = std::make_unique<CatalogSet>();
  deserializer.validateDebuggingInfo(debuggingInfo, "nextOID");
  deserializer.deserializeValue<oid_t>(catalogSet->nextOID);
  uint64_t numEntries = 0;
  deserializer.validateDebuggingInfo(debuggingInfo, "numEntries");
  deserializer.deserializeValue<uint64_t>(numEntries);
  for (uint64_t i = 0; i < numEntries; i++) {
    auto entry = CatalogEntry::deserialize(deserializer);
    if (entry != nullptr) {
      catalogSet->emplaceNoLock(std::move(entry));
    }
  }
  return catalogSet;
}

// Ideally we should not trigger the following check. Instead, we should throw
// more informative error message at catalog level.
void CatalogSet::validateExistNoLock(const std::string& name) const {
  if (!containsEntryNoLock(name)) {
    THROW_SCHEMA_MISMATCH(stringFormat("{} does not exist in catalog.", name));
  }
}

void CatalogSet::validateNotExistNoLock(const std::string& name) const {
  if (containsEntryNoLock(name)) {
    THROW_SCHEMA_MISMATCH(stringFormat("{} already exists in catalog.", name));
  }
}

}  // namespace catalog
}  // namespace neug
