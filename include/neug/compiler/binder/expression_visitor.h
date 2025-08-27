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

namespace gs {
namespace binder {

class ExpressionChildrenCollector {
 public:
  static expression_vector collectChildren(const Expression& expression);

 private:
  static expression_vector collectCaseChildren(const Expression& expression);

  static expression_vector collectSubqueryChildren(
      const Expression& expression);

  static expression_vector collectNodeChildren(const Expression& expression);

  static expression_vector collectRelChildren(const Expression& expression);
};

class ExpressionVisitor {
 public:
  virtual ~ExpressionVisitor() = default;

  void visit(std::shared_ptr<Expression> expr);

  static bool isRandom(const Expression& expression);

 protected:
  void visitSwitch(std::shared_ptr<Expression> expr);
  virtual void visitFunctionExpr(std::shared_ptr<Expression>) {}
  virtual void visitAggFunctionExpr(std::shared_ptr<Expression>) {}
  virtual void visitPropertyExpr(std::shared_ptr<Expression>) {}
  virtual void visitLiteralExpr(std::shared_ptr<Expression>) {}
  virtual void visitVariableExpr(std::shared_ptr<Expression>) {}
  virtual void visitPathExpr(std::shared_ptr<Expression>) {}
  virtual void visitNodeRelExpr(std::shared_ptr<Expression>) {}
  virtual void visitParamExpr(std::shared_ptr<Expression>) {}
  virtual void visitSubqueryExpr(std::shared_ptr<Expression>) {}
  virtual void visitCaseExpr(std::shared_ptr<Expression>) {}
  virtual void visitGraphExpr(std::shared_ptr<Expression>) {}
  virtual void visitLambdaExpr(std::shared_ptr<Expression>) {}

  virtual void visitChildren(const Expression& expr);
  void visitCaseExprChildren(const Expression& expr);
};

// Do not collect subquery expression recursively. Caller should handle
// recursive subquery instead.
class SubqueryExprCollector final : public ExpressionVisitor {
 public:
  bool hasSubquery() const { return !exprs.empty(); }
  expression_vector getSubqueryExprs() const { return exprs; }

 protected:
  void visitSubqueryExpr(std::shared_ptr<Expression> expr) override {
    exprs.push_back(expr);
  }

 private:
  expression_vector exprs;
};

class DependentVarNameCollector final : public ExpressionVisitor {
 public:
  std::unordered_set<std::string> getVarNames() const { return varNames; }

 protected:
  void visitSubqueryExpr(std::shared_ptr<Expression> expr) override;
  void visitPropertyExpr(std::shared_ptr<Expression> expr) override;
  void visitNodeRelExpr(std::shared_ptr<Expression> expr) override;
  void visitVariableExpr(std::shared_ptr<Expression> expr) override;

 private:
  std::unordered_set<std::string> varNames;
};

class PropertyExprCollector final : public ExpressionVisitor {
 public:
  expression_vector getPropertyExprs() const { return expressions; }

 protected:
  void visitSubqueryExpr(std::shared_ptr<Expression> expr) override;
  void visitPropertyExpr(std::shared_ptr<Expression> expr) override;
  void visitNodeRelExpr(std::shared_ptr<Expression> expr) override;

 private:
  expression_vector expressions;
};

class RenameDependentVar final : public ExpressionVisitor {
 public:
  RenameDependentVar(const std::string& newVarName) : newVarName{newVarName} {}

 protected:
  void visitPropertyExpr(std::shared_ptr<Expression> expr) override;

 private:
  std::string newVarName;
};

class ConstantExpressionVisitor {
 public:
  static bool needFold(const Expression& expr);
  static bool isConstant(const Expression& expr);

 private:
  static bool visitFunction(const Expression& expr);
  static bool visitCase(const Expression& expr);
  static bool visitChildren(const Expression& expr);
};

}  // namespace binder
}  // namespace gs
