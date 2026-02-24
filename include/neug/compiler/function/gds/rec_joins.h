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
#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/enums/path_semantic.h"
#include "neug/compiler/graph/graph_entry.h"

namespace neug {
namespace function {

struct RJBindData {
  graph::GraphEntry graphEntry;

  std::shared_ptr<binder::Expression> nodeInput = nullptr;
  std::shared_ptr<binder::Expression> nodeOutput = nullptr;
  uint16_t lowerBound = 0;
  uint16_t upperBound = 0;
  common::PathSemantic semantic = common::PathSemantic::WALK;

  common::ExtendDirection extendDirection = common::ExtendDirection::FWD;

  bool flipPath = false;
  bool writePath = true;

  std::shared_ptr<binder::Expression> directionExpr = nullptr;
  std::shared_ptr<binder::Expression> lengthExpr = nullptr;
  std::shared_ptr<binder::Expression> pathNodeIDsExpr = nullptr;
  std::shared_ptr<binder::Expression> pathEdgeIDsExpr = nullptr;

  std::shared_ptr<binder::Expression> weightPropertyExpr = nullptr;
  std::shared_ptr<binder::Expression> weightOutputExpr = nullptr;

  explicit RJBindData(graph::GraphEntry graphEntry)
      : graphEntry{std::move(graphEntry)} {}
  RJBindData(const RJBindData& other);

  std::string toString() const {
    return "RJBindData{"
           "graphEntry=" +
           graphEntry.toString() +
           ", nodeInput=" + (nodeInput ? nodeInput->toString() : "null") +
           ", nodeOutput=" + (nodeOutput ? nodeOutput->toString() : "null") +
           ", lowerBound=" + std::to_string(lowerBound) +
           ", upperBound=" + std::to_string(upperBound) + ", extendDirection=" +
           std::to_string(static_cast<uint8_t>(extendDirection)) +
           ", relPredicate=" + pathEdgeIDsExpr->toString() +
           ", nodePredicate=" + pathNodeIDsExpr->toString() + "}";
  }
};

class RJAlgorithm {
 public:
  virtual ~RJAlgorithm() = default;

  virtual std::string getFunctionName() const = 0;
  virtual binder::expression_vector getResultColumns(
      const RJBindData& bindData) const = 0;

  virtual std::unique_ptr<RJAlgorithm> copy() const = 0;
};

class DefaultRJAlgorithm : public RJAlgorithm {
 public:
  std::string getFunctionName() const override { return "DefaultRJ"; }

  binder::expression_vector getResultColumns(
      const RJBindData& bindData) const override {
    return {};
  }

  std::unique_ptr<RJAlgorithm> copy() const override {
    return std::make_unique<DefaultRJAlgorithm>(*this);
  }
};

}  // namespace function
}  // namespace neug
