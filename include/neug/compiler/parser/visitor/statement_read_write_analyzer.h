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

#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/parsed_statement_visitor.h"

namespace neug {
namespace parser {

class StatementReadWriteAnalyzer final : public StatementVisitor {
 public:
  explicit StatementReadWriteAnalyzer(main::ClientContext* context)
      : StatementVisitor{}, readOnly{true}, context{context} {}

  bool isReadOnly() const { return readOnly; }

 private:
  void visitCreateSequence(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitDrop(const Statement& /*statement*/) override { readOnly = false; }
  void visitCreateTable(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitCreateType(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitAlter(const Statement& /*statement*/) override { readOnly = false; }
  void visitCopyFrom(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitStandaloneCall(const Statement& /*statement*/) override {
    readOnly = true;
  }
  void visitStandaloneCallFunction(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitCreateMacro(const Statement& /*statement*/) override {
    readOnly = false;
  }
  void visitExtension(const Statement& /*statement*/) override {
    readOnly = true;
  }

  void visitReadingClause(const ReadingClause* readingClause) override;
  void visitWithClause(const WithClause* withClause) override;
  void visitReturnClause(const ReturnClause* returnClause) override;

  void visitUpdatingClause(const UpdatingClause* /*updatingClause*/) override {
    readOnly = false;
  }

  bool isExprReadOnly(const ParsedExpression* expr);

 private:
  bool readOnly;
  main::ClientContext* context;
};

}  // namespace parser
}  // namespace neug
