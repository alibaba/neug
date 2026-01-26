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

#include "bound_reading_clause.h"
#include "neug/compiler/binder/bound_table_scan_info.h"

namespace neug {
namespace binder {

class BoundLoadFrom final : public BoundReadingClause {
  static constexpr common::ClauseType clauseType_ =
      common::ClauseType::LOAD_FROM;

 public:
  explicit BoundLoadFrom(BoundTableScanInfo info)
      : BoundReadingClause{clauseType_}, info{std::move(info)} {}

  const BoundTableScanInfo* getInfo() const { return &info; }

 private:
  BoundTableScanInfo info;
};

}  // namespace binder
}  // namespace neug
