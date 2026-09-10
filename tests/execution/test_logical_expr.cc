/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include <iterator>
#include <memory>
#include <optional>
#include <utility>

#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/execution/expression/exprs/logical_expr.h"
#include "neug/utils/exception/exception.h"

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

TEST(LogicalExprTest, WithinSupportsListsAndArraysInEveryEvaluationMode) {
  for (const bool as_array : {false, true}) {
    SCOPED_TRACE(as_array ? "array" : "list");
    const auto type = as_array ? DataType::Array(DataType::INT64, 2)
                               : DataType::List(DataType::INT64);
    const auto values =
        as_array
            ? Value::ARRAY(type, {Value::INT64(1), Value::INT64(3)})
            : Value::LIST(DataType::INT64, {Value::INT64(1), Value::INT64(3)});
    struct TestCase {
      Value needle;
      Value haystack;
      std::optional<bool> expected;
    };
    const TestCase cases[] = {
        {Value::INT64(1), values, true},
        {Value::INT64(2), values, false},
        {Value::INT64(3), values, true},
        {Value(DataType::INT64), values, std::nullopt},
        {Value::INT64(1), Value(type), std::nullopt},
    };
    for (size_t i = 0; i < std::size(cases); ++i) {
      SCOPED_TRACE(i);
      const auto& test = cases[i];
      WithInExpr expression(std::make_unique<ConstExpr>(test.needle),
                            std::make_unique<ConstExpr>(test.haystack));
      auto bound = expression.bind(nullptr, {});
      for (const auto mode :
           {VarType::kRecord, VarType::kVertex, VarType::kEdge}) {
        SCOPED_TRACE(static_cast<int>(mode));
        const auto actual = evaluate(*bound, mode);
        ASSERT_EQ(actual.IsNull(), !test.expected.has_value());
        if (test.expected) {
          EXPECT_EQ(actual.GetValue<bool>(), *test.expected);
        }
      }
    }
  }
}

TEST(LogicalExprTest, WithinHandlesEmptyListsAndRejectsScalarContainers) {
  WithInExpr empty(
      std::make_unique<ConstExpr>(Value::INT64(1)),
      std::make_unique<ConstExpr>(Value::LIST(DataType::INT64, {})));
  WithInExpr invalid(std::make_unique<ConstExpr>(Value::INT64(1)),
                     std::make_unique<ConstExpr>(Value::INT64(1)));
  auto bound_empty = empty.bind(nullptr, {});
  auto bound_invalid = invalid.bind(nullptr, {});
  for (const auto mode : {VarType::kRecord, VarType::kVertex, VarType::kEdge}) {
    SCOPED_TRACE(static_cast<int>(mode));
    const auto actual = evaluate(*bound_empty, mode);
    ASSERT_FALSE(actual.IsNull());
    EXPECT_FALSE(actual.GetValue<bool>());
    EXPECT_THROW(evaluate(*bound_invalid, mode),
                 exception::InvalidArgumentException);
  }
}

}  // namespace
}  // namespace neug::execution
