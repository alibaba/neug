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

#include <string>

#include "neug/compiler/planner/operator/logical_operator.h"

namespace neug::planner {

class LogicalDropIndex final : public LogicalOperator {
 public:
  LogicalDropIndex(std::string indexName, bool ifExists)
      : LogicalOperator{LogicalOperatorType::DROP_INDEX},
        indexName{std::move(indexName)},
        ifExists{ifExists} {}

  void computeFactorizedSchema() override { createEmptySchema(); }
  void computeFlatSchema() override { createEmptySchema(); }
  std::string getExpressionsForPrinting() const override { return indexName; }
  const std::string& getIndexName() const { return indexName; }
  bool getIfExists() const { return ifExists; }
  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalDropIndex>(indexName, ifExists);
  }

 private:
  std::string indexName;
  bool ifExists;
};

}  // namespace neug::planner
