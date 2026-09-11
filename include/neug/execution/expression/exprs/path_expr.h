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
#include "neug/execution/expression/expr.h"

namespace neug {
namespace execution {
class PathNodesExpr : public ExprBase {
 public:
  PathNodesExpr(std::unique_ptr<ExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {
    type_ = DataType::List(DataType::VERTEX);
  }

  ~PathNodesExpr() override = default;

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  DataType type_;
  std::unique_ptr<ExprBase> path_expr_;
};

class PathRelationsExpr : public ExprBase {
 public:
  PathRelationsExpr(std::unique_ptr<ExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {
    type_ = DataType::List(DataType::EDGE);
  }

  ~PathRelationsExpr() override = default;

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  DataType type_;
  std::unique_ptr<ExprBase> path_expr_;
};

class SingleRelationshipPathExpr : public ExprBase {
 public:
  SingleRelationshipPathExpr(std::unique_ptr<ExprBase>&& start_expr,
                             std::unique_ptr<ExprBase>&& rel_expr,
                             std::unique_ptr<ExprBase>&& end_expr)
      : start_expr_(std::move(start_expr)),
        rel_expr_(std::move(rel_expr)),
        end_expr_(std::move(end_expr)),
        type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::unique_ptr<ExprBase> start_expr_;
  std::unique_ptr<ExprBase> rel_expr_;
  std::unique_ptr<ExprBase> end_expr_;
  DataType type_;
};

class PathConcatExpr : public ExprBase {
 public:
  explicit PathConcatExpr(std::vector<std::unique_ptr<ExprBase>> path_exprs)
      : path_exprs_(std::move(path_exprs)), type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> path_exprs_;
  DataType type_;
};

class PathPropsExpr : public ExprBase {
 public:
  PathPropsExpr(std::unique_ptr<ExprBase>&& path_expr, const std::string& prop,
                const DataType& type, bool extract_vertex_prop)
      : prop_(prop),
        type_(DataType::List(type)),
        extract_vertex_prop_(extract_vertex_prop),
        path_expr_(std::move(path_expr)) {}
  ~PathPropsExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::string prop_;
  DataType type_;
  bool extract_vertex_prop_;
  std::unique_ptr<ExprBase> path_expr_;
};

class StartEndNodeExpr : public ExprBase {
 public:
  StartEndNodeExpr(std::unique_ptr<ExprBase>&& path_expr, bool is_start)
      : path_expr_(std::move(path_expr)), is_start_(is_start) {
    type_ = DataType::VERTEX;
  }
  ~StartEndNodeExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  DataType type_;
  std::unique_ptr<ExprBase> path_expr_;
  bool is_start_;
};
}  // namespace execution
}  // namespace neug
