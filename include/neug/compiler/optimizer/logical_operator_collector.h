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

#include "logical_operator_visitor.h"

namespace neug {
namespace optimizer {

class LogicalOperatorCollector : public LogicalOperatorVisitor {
 public:
  ~LogicalOperatorCollector() override = default;

  void collect(planner::LogicalOperator* op);

  bool hasOperators() const { return !ops.empty(); }
  std::vector<planner::LogicalOperator*> getOperators() const { return ops; }

 protected:
  std::vector<planner::LogicalOperator*> ops;
};

class LogicalFlattenCollector final : public LogicalOperatorCollector {
 protected:
  void visitFlatten(planner::LogicalOperator* op) override {
    ops.push_back(op);
  }
};

class LogicalFilterCollector final : public LogicalOperatorCollector {
 protected:
  void visitFilter(planner::LogicalOperator* op) override { ops.push_back(op); }
};

class LogicalScanNodeTableCollector final : public LogicalOperatorCollector {
 protected:
  void visitScanNodeTable(planner::LogicalOperator* op) override {
    ops.push_back(op);
  }
};

// TODO(Xiyang): Rename me.
class LogicalIndexScanNodeCollector final : public LogicalOperatorCollector {
 protected:
  void visitScanNodeTable(planner::LogicalOperator* op) override;
};

class LogicalRecursiveExtendCollector final : public LogicalOperatorCollector {
 protected:
  void visitRecursiveExtend(planner::LogicalOperator* op) override {
    ops.push_back(op);
  }
};

}  // namespace optimizer
}  // namespace neug
