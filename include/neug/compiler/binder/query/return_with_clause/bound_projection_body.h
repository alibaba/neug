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

namespace neug {
namespace binder {

class BoundProjectionBody {
  static constexpr uint64_t INVALID_NUMBER = UINT64_MAX;

 public:
  explicit BoundProjectionBody(bool distinct)
      : distinct{distinct}, skipNumber{nullptr}, limitNumber{nullptr} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundProjectionBody);

  bool isDistinct() const { return distinct; }

  void setProjectionExpressions(expression_vector expressions) {
    projectionExpressions = std::move(expressions);
  }
  expression_vector getProjectionExpressions() const {
    return projectionExpressions;
  }

  void setGroupByExpressions(expression_vector expressions) {
    groupByExpressions = std::move(expressions);
  }
  expression_vector getGroupByExpressions() const { return groupByExpressions; }

  void setAggregateExpressions(expression_vector expressions) {
    aggregateExpressions = std::move(expressions);
  }
  bool hasAggregateExpressions() const { return !aggregateExpressions.empty(); }
  expression_vector getAggregateExpressions() const {
    return aggregateExpressions;
  }

  void setOrderByExpressions(expression_vector expressions,
                             std::vector<bool> sortOrders) {
    orderByExpressions = std::move(expressions);
    isAscOrders = std::move(sortOrders);
  }
  bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
  const expression_vector& getOrderByExpressions() const {
    return orderByExpressions;
  }
  const std::vector<bool>& getSortingOrders() const { return isAscOrders; }

  void setSkipNumber(std::shared_ptr<Expression> number) {
    skipNumber = std::move(number);
  }
  bool hasSkip() const { return skipNumber != nullptr; }
  std::shared_ptr<Expression> getSkipNumber() const { return skipNumber; }

  void setLimitNumber(std::shared_ptr<Expression> number) {
    limitNumber = std::move(number);
  }
  bool hasLimit() const { return limitNumber != nullptr; }
  std::shared_ptr<Expression> getLimitNumber() const { return limitNumber; }

  bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

 private:
  BoundProjectionBody(const BoundProjectionBody& other)
      : distinct{other.distinct},
        projectionExpressions{other.projectionExpressions},
        groupByExpressions{other.groupByExpressions},
        aggregateExpressions{other.aggregateExpressions},
        orderByExpressions{other.orderByExpressions},
        isAscOrders{other.isAscOrders},
        skipNumber{other.skipNumber},
        limitNumber{other.limitNumber} {}

 private:
  bool distinct;
  expression_vector projectionExpressions;
  expression_vector groupByExpressions;
  expression_vector aggregateExpressions;
  expression_vector orderByExpressions;
  std::vector<bool> isAscOrders;
  std::shared_ptr<Expression> skipNumber;
  std::shared_ptr<Expression> limitNumber;
};

}  // namespace binder
}  // namespace neug
