/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <utility>

#include "neug/execution/expression/exprs/logical_expr.h"

namespace neug::execution {
namespace {

class BoundProbe final : public RecordExprBase,
                         public VertexExprBase,
                         public EdgeExprBase {
 public:
  BoundProbe(Value value, int& calls)
      : value_(std::move(value)), calls_(calls) {}
  const DataType& type() const override { return value_.type(); }
  Value eval_record(const DataChunk&, size_t) const override { return eval(); }
  Value eval_vertex(label_t, vid_t) const override { return eval(); }
  Value eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const void*) const override {
    return eval();
  }

 private:
  Value eval() const {
    ++calls_;
    return value_;
  }
  Value value_;
  int& calls_;
};

class ProbeExpr final : public ExprBase {
 public:
  ProbeExpr(std::optional<bool> value, int& calls)
      : value_(value ? Value::BOOLEAN(*value) : Value(DataType::BOOLEAN)),
        calls_(calls) {}
  const DataType& type() const override { return value_.type(); }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface*,
                                       const ParamsMap&) const override {
    return std::make_unique<BoundProbe>(value_, calls_);
  }

 private:
  Value value_;
  int& calls_;
};

Value evaluate(const BindedExprBase& expression, VarType mode) {
  switch (mode) {
  case VarType::kRecord:
    return expression.Cast<RecordExprBase>().eval_record(DataChunk(), 0);
  case VarType::kVertex:
    return expression.Cast<VertexExprBase>().eval_vertex(0, 0);
  case VarType::kEdge:
    return expression.Cast<EdgeExprBase>().eval_edge({0, 0, 0}, 0, 0, nullptr);
  }
  return Value(DataType::BOOLEAN);
}

TEST(LogicalExprTest, PreservesThreeValuedLogicInEveryEvaluationMode) {
  struct TruthRow {
    std::optional<bool> left;
    std::optional<bool> right;
    std::optional<bool> conjunction;
    std::optional<bool> disjunction;
  };
  const TruthRow rows[] = {
      {true, true, true, true},
      {true, false, false, true},
      {true, std::nullopt, std::nullopt, true},
      {false, true, false, true},
      {false, false, false, false},
      {false, std::nullopt, false, std::nullopt},
      {std::nullopt, true, std::nullopt, true},
      {std::nullopt, false, false, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
  };
  for (const auto mode : {VarType::kRecord, VarType::kVertex, VarType::kEdge}) {
    SCOPED_TRACE(static_cast<int>(mode));
    for (const auto& row : rows) {
      SCOPED_TRACE(row.left ? (*row.left ? "true" : "false") : "null");
      SCOPED_TRACE(row.right ? (*row.right ? "true" : "false") : "null");
      for (const auto op : {::common::Logical::AND, ::common::Logical::OR}) {
        SCOPED_TRACE(static_cast<int>(op));
        int leftCalls = 0;
        int rightCalls = 0;
        BinaryLogicalExpr expression(
            std::make_unique<ProbeExpr>(row.left, leftCalls),
            std::make_unique<ProbeExpr>(row.right, rightCalls), op);
        auto bound = expression.bind(nullptr, {});
        const auto actual = evaluate(*bound, mode);
        const auto expected =
            op == ::common::Logical::AND ? row.conjunction : row.disjunction;
        ASSERT_EQ(actual.IsNull(), !expected.has_value());
        if (expected) {
          EXPECT_EQ(actual.GetValue<bool>(), *expected);
        }
        EXPECT_EQ(leftCalls, 1);
        const bool shortCircuit =
            row.left && (op == ::common::Logical::AND ? !*row.left : *row.left);
        EXPECT_EQ(rightCalls, shortCircuit ? 0 : 1);
      }
    }
  }
}

}  // namespace
}  // namespace neug::execution
