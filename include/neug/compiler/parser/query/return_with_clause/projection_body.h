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

#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace parser {

class ProjectionBody {
 public:
  ProjectionBody(
      bool isDistinct,
      std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions)
      : isDistinct{isDistinct},
        projectionExpressions{std::move(projectionExpressions)} {}
  DELETE_COPY_DEFAULT_MOVE(ProjectionBody);

  inline bool getIsDistinct() const { return isDistinct; }

  inline const std::vector<std::unique_ptr<ParsedExpression>>&
  getProjectionExpressions() const {
    return projectionExpressions;
  }

  inline void setOrderByExpressions(
      std::vector<std::unique_ptr<ParsedExpression>> expressions,
      std::vector<bool> sortOrders) {
    orderByExpressions = std::move(expressions);
    isAscOrders = std::move(sortOrders);
  }
  inline bool hasOrderByExpressions() const {
    return !orderByExpressions.empty();
  }
  inline const std::vector<std::unique_ptr<ParsedExpression>>&
  getOrderByExpressions() const {
    return orderByExpressions;
  }

  inline std::vector<bool> getSortOrders() const { return isAscOrders; }

  inline void setSkipExpression(std::unique_ptr<ParsedExpression> expression) {
    skipExpression = std::move(expression);
  }
  inline bool hasSkipExpression() const { return skipExpression != nullptr; }
  inline ParsedExpression* getSkipExpression() const {
    return skipExpression.get();
  }

  inline void setLimitExpression(std::unique_ptr<ParsedExpression> expression) {
    limitExpression = std::move(expression);
  }
  inline bool hasLimitExpression() const { return limitExpression != nullptr; }
  inline ParsedExpression* getLimitExpression() const {
    return limitExpression.get();
  }

 private:
  bool isDistinct;
  std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions;
  std::vector<std::unique_ptr<ParsedExpression>> orderByExpressions;
  std::vector<bool> isAscOrders;
  std::unique_ptr<ParsedExpression> skipExpression;
  std::unique_ptr<ParsedExpression> limitExpression;
};

}  // namespace parser
}  // namespace neug
