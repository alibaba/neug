/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/execution/expression/exprs/udfs.h"

namespace neug::execution {
namespace {

class ScalarTestStorage : public IStorageInterface {
 public:
  explicit ScalarTestStorage(bool readable = true) : readable_(readable) {}
  bool readable() const override {
    ++read_calls;
    return readable_;
  }
  bool writable() const override { return false; }
  const Schema& schema() const override { return schema_; }
  bool GetVertexIndex(label_t, const Value&, vid_t&) const override {
    return false;
  }
  mutable int read_calls = 0;

 private:
  bool readable_;
  Schema schema_;
};

Value twice(const std::vector<Value>& args) {
  return Value::INT64(2 * args.at(0).GetValue<int64_t>());
}

Value with_graph(const std::vector<Value>& args,
                 const IStorageInterface* storage) {
  if (!storage) {
    THROW_RUNTIME_ERROR("Test function requires storage");
  }
  return Value::INT64(args.at(0).GetValue<int64_t>() +
                      (storage->readable() ? 1 : 0));
}

std::vector<std::unique_ptr<ExprBase>> arguments() {
  std::vector<std::unique_ptr<ExprBase>> args;
  args.emplace_back(std::make_unique<ConstExpr>(Value::INT64(7)));
  return args;
}

void expect_all_modes(const BindedExprBase& expression, int64_t expected) {
  EXPECT_EQ(expression.Cast<RecordExprBase>()
                .eval_record(DataChunk(), 0)
                .GetValue<int64_t>(),
            expected);
  EXPECT_EQ(
      expression.Cast<VertexExprBase>().eval_vertex(0, 0).GetValue<int64_t>(),
      expected);
  EXPECT_EQ(expression.Cast<EdgeExprBase>()
                .eval_edge({0, 0, 0}, 0, 0, nullptr)
                .GetValue<int64_t>(),
            expected);
}

TEST(ScalarFunctionExprTest, LegacyCallbackWorksWithoutStorage) {
  ScalarFunctionExpr expression(twice, "twice", DataType::INT64, arguments());
  auto bound = expression.bind(nullptr, {});
  expect_all_modes(*bound, 14);
}

TEST(ScalarFunctionExprTest, EachBindingUsesItsOwnStorage) {
  ScalarTestStorage first_storage;
  ScalarTestStorage second_storage(false);
  ScalarFunctionExpr expression(nullptr, "with_graph", DataType::INT64,
                                arguments(), with_graph);
  auto first = expression.bind(&first_storage, {});
  auto second = expression.bind(&second_storage, {});
  expect_all_modes(*first, 8);
  expect_all_modes(*second, 7);
  expect_all_modes(*first, 8);
  EXPECT_EQ(first_storage.read_calls, 6);
  EXPECT_EQ(second_storage.read_calls, 3);
}

TEST(ScalarFunctionExprTest, LegacyParentPropagatesContextToChild) {
  ScalarTestStorage storage;
  std::vector<std::unique_ptr<ExprBase>> children;
  children.emplace_back(std::make_unique<ScalarFunctionExpr>(
      nullptr, "with_graph", DataType::INT64, arguments(), with_graph));
  ScalarFunctionExpr parent(twice, "twice", DataType::INT64,
                            std::move(children));
  auto bound = parent.bind(&storage, {});
  expect_all_modes(*bound, 16);
  EXPECT_EQ(storage.read_calls, 3);
}

TEST(ScalarFunctionExprTest, CatalogCopyPreservesContextCallback) {
  function::NeugContextScalarFunction definition(
      "with_graph", std::vector<common::DataTypeId>{common::DataTypeId::kInt64},
      common::DataTypeId::kInt64, with_graph);
  auto copy = definition.copy();
  auto* scalar = dynamic_cast<function::NeugContextScalarFunction*>(copy.get());
  ASSERT_NE(scalar, nullptr);
  EXPECT_EQ(scalar->neugExecFunc, nullptr);
  EXPECT_EQ(scalar->neugContextExecFunc, with_graph);
}

TEST(ScalarFunctionExprTest, LegacyCatalogCopyPreservesCallback) {
  function::NeugScalarFunction definition(
      "twice", std::vector<common::DataTypeId>{common::DataTypeId::kInt64},
      common::DataTypeId::kInt64, twice);
  auto copy = definition.copy();
  auto* scalar = dynamic_cast<function::NeugScalarFunction*>(copy.get());
  ASSERT_NE(scalar, nullptr);
  EXPECT_EQ(scalar->neugExecFunc, twice);
  EXPECT_EQ(dynamic_cast<function::NeugContextScalarFunction*>(scalar),
            nullptr);
}

}  // namespace
}  // namespace neug::execution
