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

#include <memory>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace common {
class FileSystem;
}

namespace function {

struct NEUG_API TableFuncBindData {
  // output columns
  binder::expression_vector columns;
  common::row_idx_t numRows;
  // input params
  binder::expression_vector params;

  TableFuncBindData() : numRows{0} {}
  explicit TableFuncBindData(common::row_idx_t numRows) : numRows{numRows} {}
  explicit TableFuncBindData(binder::expression_vector columns)
      : columns{std::move(columns)}, numRows{0} {}
  TableFuncBindData(binder::expression_vector columns,
                    common::row_idx_t numRows)
      : columns{std::move(columns)}, numRows{numRows} {}
  TableFuncBindData(binder::expression_vector columns,
                    common::row_idx_t numRows, binder::expression_vector params)
      : columns{std::move(columns)},
        numRows{numRows},
        params{std::move(params)} {}
  TableFuncBindData(const TableFuncBindData& other)
      : columns{other.columns},
        numRows{other.numRows},
        params(other.params),
        columnSkips{other.columnSkips} {}
  TableFuncBindData& operator=(const TableFuncBindData& other) = delete;
  virtual ~TableFuncBindData() = default;

  common::idx_t getNumColumns() const { return columns.size(); }
  void setColumnSkips(std::vector<bool> skips) {
    columnSkips = std::move(skips);
  }
  std::vector<bool> getColumnSkips() const;

  std::shared_ptr<binder::Expression> getRowSkips() const { return rowSkips; }

  void setRowSkips(std::shared_ptr<binder::Expression> skips) {
    rowSkips = std::move(skips);
  }

  virtual std::shared_ptr<binder::Expression> getNodeOutput() const {
    return nullptr;
  }

  virtual bool getIgnoreErrorsOption() const;

  virtual std::unique_ptr<TableFuncBindData> copy() const;

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

  template <class TARGET>
  TARGET& cast() {
    return *common::neug_dynamic_cast<TARGET*>(this);
  }

 protected:
  std::vector<bool> columnSkips;
  std::shared_ptr<binder::Expression> rowSkips;
};

}  // namespace function
}  // namespace neug
