/** Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace binder {

struct BoundCreateIndexInfo {
  std::string indexName;
  std::shared_ptr<NodeOrRelExpression> pattern;
  std::vector<std::string> propertyNames;
  std::vector<common::DataType> propertyTypes;
  std::string indexType;
  std::unordered_map<std::string, std::string> options;
  bool ifNotExists = false;

  BoundCreateIndexInfo copy() const {
    BoundCreateIndexInfo result;
    result.indexName = indexName;
    result.pattern = pattern;
    result.propertyNames = propertyNames;
    result.propertyTypes = copyVector(propertyTypes);
    result.indexType = indexType;
    result.options = options;
    result.ifNotExists = ifNotExists;
    return result;
  }
};

class BoundCreateIndex final : public BoundStatement {
 public:
  explicit BoundCreateIndex(BoundCreateIndexInfo info)
      : BoundStatement{common::StatementType::CREATE_INDEX,
                       BoundStatementResult::createSingleStringColumnResult()},
        info{std::move(info)} {}

  const BoundCreateIndexInfo& getInfo() const { return info; }
  BoundCreateIndexInfo moveInfo() { return std::move(info); }

 private:
  BoundCreateIndexInfo info;
};

}  // namespace binder
}  // namespace neug
