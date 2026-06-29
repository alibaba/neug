/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/common/cast.h"
#include "neug/utils/io/read/common/schema.h"

namespace common {
class Expression;
}  // namespace common

namespace neug {
namespace reader {

struct ReadLocalState {
  virtual ~ReadLocalState() = default;

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
};

struct ReadSharedState {
  ExternalSchema schema;
  std::vector<std::string> projectColumns;
  std::shared_ptr<::common::Expression> skipRows;

  int columnNum() {
    if (!schema.entry) {
      return 0;
    }
    const auto& allColumns = schema.entry->columnNames;
    return projectColumns.empty() ? allColumns.size() : projectColumns.size();
  }
};

}  // namespace reader
}  // namespace neug
