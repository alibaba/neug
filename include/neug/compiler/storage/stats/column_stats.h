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

#include <optional>

#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/storage/stats/hyperloglog.h"

namespace neug {
namespace storage {

class ColumnStats {
 public:
  ColumnStats() = default;
  explicit ColumnStats(const common::LogicalType& dataType);
  EXPLICIT_COPY_DEFAULT_MOVE(ColumnStats);

  common::cardinality_t getNumDistinctValues() const {
    return hll ? hll->count() : 0;
  }

  void update(const common::ValueVector* vector);

  void merge(const ColumnStats& other) {
    if (hll) {
      NEUG_ASSERT(other.hll);
      hll->merge(*other.hll);
    };
  }

  void serialize(common::Serializer& serializer) const {
    serializer.writeDebuggingInfo("has_hll");
    serializer.serializeValue(hll.has_value());
    if (hll) {
      serializer.writeDebuggingInfo("hll");
      hll->serialize(serializer);
    }
  }

  static ColumnStats deserialize(common::Deserializer& deserializer) {
    ColumnStats columnStats;
    std::string info;
    deserializer.validateDebuggingInfo(info, "has_hll");
    bool hasHll = false;
    deserializer.deserializeValue(hasHll);
    if (hasHll) {
      deserializer.validateDebuggingInfo(info, "hll");
      columnStats.hll = HyperLogLog::deserialize(deserializer);
    }
    return columnStats;
  }

 private:
  ColumnStats(const ColumnStats& other) : hll{other.hll}, hashes{nullptr} {}

 private:
  std::optional<HyperLogLog> hll;
  // Preallocated vector for hash values.
  std::unique_ptr<common::ValueVector> hashes;
};

}  // namespace storage
}  // namespace neug
