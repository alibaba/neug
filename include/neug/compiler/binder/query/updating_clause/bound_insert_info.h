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
#include "neug/compiler/common/enums/conflict_action.h"
#include "neug/compiler/common/enums/table_type.h"

namespace neug {
namespace binder {

struct BoundInsertInfo {
  common::TableType tableType;
  std::shared_ptr<Expression> pattern;
  expression_vector columnExprs;
  expression_vector columnDataExprs;
  common::ConflictAction conflictAction;

  BoundInsertInfo(common::TableType tableType,
                  std::shared_ptr<Expression> pattern)
      : tableType{tableType},
        pattern{std::move(pattern)},
        conflictAction{common::ConflictAction::ON_CONFLICT_THROW} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundInsertInfo);

 private:
  BoundInsertInfo(const BoundInsertInfo& other)
      : tableType{other.tableType},
        pattern{other.pattern},
        columnExprs{other.columnExprs},
        columnDataExprs{other.columnDataExprs},
        conflictAction{other.conflictAction} {}
};

}  // namespace binder
}  // namespace neug
