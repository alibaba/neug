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
#include "neug/compiler/common/enums/table_type.h"

namespace neug {
namespace binder {

struct BoundSetPropertyInfo {
  common::TableType tableType;
  std::shared_ptr<Expression> pattern;
  std::shared_ptr<Expression> column;
  std::shared_ptr<Expression> columnData;
  bool updatePk = false;

  BoundSetPropertyInfo(common::TableType tableType,
                       std::shared_ptr<Expression> pattern,
                       std::shared_ptr<Expression> column,
                       std::shared_ptr<Expression> columnData)
      : tableType{tableType},
        pattern{std::move(pattern)},
        column{std::move(column)},
        columnData{std::move(columnData)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundSetPropertyInfo);

 private:
  BoundSetPropertyInfo(const BoundSetPropertyInfo& other)
      : tableType{other.tableType},
        pattern{other.pattern},
        column{other.column},
        columnData{other.columnData},
        updatePk{other.updatePk} {}
};

}  // namespace binder
}  // namespace neug
