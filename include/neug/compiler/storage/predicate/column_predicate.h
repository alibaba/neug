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

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/zone_map_check_result.h"

namespace neug {
namespace storage {

struct MergedColumnChunkStats;

class ColumnPredicate;
class NEUG_API ColumnPredicateSet {
 public:
  ColumnPredicateSet() = default;
  EXPLICIT_COPY_DEFAULT_MOVE(ColumnPredicateSet);

  void addPredicate(std::unique_ptr<ColumnPredicate> predicate) {
    predicates.push_back(std::move(predicate));
  }
  void tryAddPredicate(const binder::Expression& column,
                       const binder::Expression& predicate);
  bool isEmpty() const { return predicates.empty(); }

  common::ZoneMapCheckResult checkZoneMap(
      const MergedColumnChunkStats& stats) const;

  std::string toString() const;

 private:
  ColumnPredicateSet(const ColumnPredicateSet& other)
      : predicates{copyVector(other.predicates)} {}

 private:
  std::vector<std::unique_ptr<ColumnPredicate>> predicates;
};

class NEUG_API ColumnPredicate {
 public:
  ColumnPredicate(std::string columnName, common::ExpressionType expressionType)
      : columnName{std::move(columnName)}, expressionType(expressionType) {}

  virtual ~ColumnPredicate() = default;

  virtual common::ZoneMapCheckResult checkZoneMap(
      const MergedColumnChunkStats& stats) const = 0;

  virtual std::string toString();

  virtual std::unique_ptr<ColumnPredicate> copy() const = 0;

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

 protected:
  std::string columnName;
  common::ExpressionType expressionType;
};

struct ColumnPredicateUtil {
  static std::unique_ptr<ColumnPredicate> tryConvert(
      const binder::Expression& column, const binder::Expression& predicate);
};

}  // namespace storage
}  // namespace neug
