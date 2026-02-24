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

#include "neug/compiler/catalog/catalog_entry/sequence_catalog_entry.h"

#include "neug/compiler/binder/ddl/bound_create_sequence_info.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/arithmetic/add.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace catalog {

SequenceData SequenceCatalogEntry::getSequenceData() {
  std::lock_guard lck(mtx);
  return sequenceData;
}

int64_t SequenceCatalogEntry::currVal() {
  std::lock_guard lck(mtx);
  if (sequenceData.usageCount == 0) {
    THROW_CATALOG_EXCEPTION(
        "currval: sequence \"" + name +
        "\" is not yet defined. To define the sequence, call nextval first.");
  }
  return sequenceData.currVal;
}

void SequenceCatalogEntry::nextValNoLock() {}

// referenced from DuckDB
void SequenceCatalogEntry::nextKVal(transaction::Transaction* transaction,
                                    const uint64_t& count) {
  NEUG_ASSERT(count > 0);
  SequenceRollbackData rollbackData{};
  {
    std::lock_guard lck(mtx);
    rollbackData =
        SequenceRollbackData{sequenceData.usageCount, sequenceData.currVal};
    for (auto i = 0ul; i < count; i++) {
      nextValNoLock();
    }
  }
  transaction->pushSequenceChange(this, count, rollbackData);
}

void SequenceCatalogEntry::nextKVal(transaction::Transaction* transaction,
                                    const uint64_t& count,
                                    ValueVector& resultVector) {
  NEUG_ASSERT(count > 0);
  SequenceRollbackData rollbackData{};
  {
    std::lock_guard lck(mtx);
    rollbackData =
        SequenceRollbackData{sequenceData.usageCount, sequenceData.currVal};
    for (auto i = 0ul; i < count; i++) {
      nextValNoLock();
      resultVector.setValue(i, sequenceData.currVal);
    }
  }
  transaction->pushSequenceChange(this, count, rollbackData);
}

void SequenceCatalogEntry::rollbackVal(const uint64_t& usageCount,
                                       const int64_t& currVal) {
  std::lock_guard lck(mtx);
  sequenceData.usageCount = usageCount;
  sequenceData.currVal = currVal;
}

void SequenceCatalogEntry::serialize(Serializer& serializer) const {
  CatalogEntry::serialize(serializer);
  serializer.writeDebuggingInfo("usageCount");
  serializer.write(sequenceData.usageCount);
  serializer.writeDebuggingInfo("currVal");
  serializer.write(sequenceData.currVal);
  serializer.writeDebuggingInfo("increment");
  serializer.write(sequenceData.increment);
  serializer.writeDebuggingInfo("startValue");
  serializer.write(sequenceData.startValue);
  serializer.writeDebuggingInfo("minValue");
  serializer.write(sequenceData.minValue);
  serializer.writeDebuggingInfo("maxValue");
  serializer.write(sequenceData.maxValue);
  serializer.writeDebuggingInfo("cycle");
  serializer.write(sequenceData.cycle);
}

std::unique_ptr<SequenceCatalogEntry> SequenceCatalogEntry::deserialize(
    Deserializer& deserializer) {
  std::string debuggingInfo;
  uint64_t usageCount = 0;
  int64_t currVal = 0;
  int64_t increment = 0;
  int64_t startValue = 0;
  int64_t minValue = 0;
  int64_t maxValue = 0;
  bool cycle = false;
  deserializer.validateDebuggingInfo(debuggingInfo, "usageCount");
  deserializer.deserializeValue(usageCount);
  deserializer.validateDebuggingInfo(debuggingInfo, "currVal");
  deserializer.deserializeValue(currVal);
  deserializer.validateDebuggingInfo(debuggingInfo, "increment");
  deserializer.deserializeValue(increment);
  deserializer.validateDebuggingInfo(debuggingInfo, "startValue");
  deserializer.deserializeValue(startValue);
  deserializer.validateDebuggingInfo(debuggingInfo, "minValue");
  deserializer.deserializeValue(minValue);
  deserializer.validateDebuggingInfo(debuggingInfo, "maxValue");
  deserializer.deserializeValue(maxValue);
  deserializer.validateDebuggingInfo(debuggingInfo, "cycle");
  deserializer.deserializeValue(cycle);
  auto result = std::make_unique<SequenceCatalogEntry>();
  result->sequenceData.usageCount = usageCount;
  result->sequenceData.currVal = currVal;
  result->sequenceData.increment = increment;
  result->sequenceData.startValue = startValue;
  result->sequenceData.minValue = minValue;
  result->sequenceData.maxValue = maxValue;
  result->sequenceData.cycle = cycle;
  return result;
}

std::string SequenceCatalogEntry::toCypher(
    const ToCypherInfo& /* info */) const {
  return stringFormat(
      "DROP SEQUENCE IF EXISTS `{}`;\n"
      "CREATE SEQUENCE IF NOT EXISTS `{}` START {} INCREMENT {} MINVALUE {} "
      "MAXVALUE {} {} CYCLE;\n"
      "RETURN nextval('{}');",
      getName(), getName(), sequenceData.currVal, sequenceData.increment,
      sequenceData.minValue, sequenceData.maxValue,
      sequenceData.cycle ? "" : "NO", getName());
}

BoundCreateSequenceInfo SequenceCatalogEntry::getBoundCreateSequenceInfo(
    bool isInternal) const {
  return BoundCreateSequenceInfo(name, sequenceData.startValue,
                                 sequenceData.increment, sequenceData.minValue,
                                 sequenceData.maxValue, sequenceData.cycle,
                                 ConflictAction::ON_CONFLICT_THROW, isInternal);
}

}  // namespace catalog
}  // namespace neug
