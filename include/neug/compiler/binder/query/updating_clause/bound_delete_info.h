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
#include "neug/compiler/common/enums/delete_type.h"
#include "neug/compiler/common/enums/table_type.h"

namespace neug {
namespace binder {

struct BoundDeleteInfo {
  common::DeleteNodeType deleteType;
  common::TableType tableType;
  std::shared_ptr<Expression> pattern;

  BoundDeleteInfo(common::DeleteNodeType deleteType,
                  common::TableType tableType,
                  std::shared_ptr<Expression> pattern)
      : deleteType{deleteType},
        tableType{tableType},
        pattern{std::move(pattern)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundDeleteInfo);

  std::string toString() const { return "Delete " + pattern->toString(); }

 private:
  BoundDeleteInfo(const BoundDeleteInfo& other)
      : deleteType{other.deleteType},
        tableType{other.tableType},
        pattern{other.pattern} {}
};

}  // namespace binder
}  // namespace neug
