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

#include <mutex>

#include "neug/compiler/binder/ddl/bound_create_sequence_info.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {
class ValueVector;
}

namespace binder {
struct BoundExtraCreateCatalogEntryInfo;
struct BoundAlterInfo;
}  // namespace binder

namespace transaction {
class Transaction;
}  // namespace transaction

namespace catalog {

struct SequenceRollbackData {
  uint64_t usageCount;
  int64_t currVal;
};

struct SequenceData {
  SequenceData() = default;
  explicit SequenceData(const binder::BoundCreateSequenceInfo& info)
      : usageCount{0},
        currVal{info.startWith},
        increment{info.increment},
        startValue{info.startWith},
        minValue{info.minValue},
        maxValue{info.maxValue},
        cycle{info.cycle} {}

  uint64_t usageCount;
  int64_t currVal;
  int64_t increment;
  int64_t startValue;
  int64_t minValue;
  int64_t maxValue;
  bool cycle;
};

class CatalogSet;
class NEUG_API SequenceCatalogEntry final : public CatalogEntry {
 public:
  //===--------------------------------------------------------------------===//
  // constructors
  //===--------------------------------------------------------------------===//
  SequenceCatalogEntry() : sequenceData{} {}
  explicit SequenceCatalogEntry(
      const binder::BoundCreateSequenceInfo& sequenceInfo)
      : CatalogEntry{CatalogEntryType::SEQUENCE_ENTRY,
                     sequenceInfo.sequenceName},
        sequenceData{SequenceData(sequenceInfo)} {}

  //===--------------------------------------------------------------------===//
  // getter & setter
  //===--------------------------------------------------------------------===//
  SequenceData getSequenceData();

  //===--------------------------------------------------------------------===//
  // sequence functions
  //===--------------------------------------------------------------------===//
  int64_t currVal();
  void nextKVal(transaction::Transaction* transaction, const uint64_t& count);
  void nextKVal(transaction::Transaction* transaction, const uint64_t& count,
                common::ValueVector& resultVector);
  void rollbackVal(const uint64_t& usageCount, const int64_t& currVal);

  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<SequenceCatalogEntry> deserialize(
      common::Deserializer& deserializer);

  std::string toCypher(const ToCypherInfo& info) const override;

  binder::BoundCreateSequenceInfo getBoundCreateSequenceInfo(
      bool isInternal) const;

  static std::string getSerialName(const std::string& tableName,
                                   const std::string& propertyName) {
    return std::string(tableName)
        .append("_")
        .append(propertyName)
        .append("_")
        .append("serial");
  }

 private:
  void nextValNoLock();

 private:
  std::mutex mtx;
  SequenceData sequenceData;
};

}  // namespace catalog
}  // namespace neug
