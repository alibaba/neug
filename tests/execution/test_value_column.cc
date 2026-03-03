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
#include <gtest/gtest.h>
#include <filesystem>

#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"

namespace neug {
namespace execution {
namespace test {

class ValueColumnTest : public ::testing::Test {};

TEST_F(ValueColumnTest, BoolValueColumnBasic) {
  ValueColumnBuilder<bool> builder;
  builder.push_back_opt(true);
  builder.push_back_opt(false);
  builder.push_back_elem(Value::BOOLEAN(true));
  auto col = std::dynamic_pointer_cast<ValueColumn<bool>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), true);
  EXPECT_EQ(col->get_value(1), false);
  EXPECT_EQ(col->get_value(2), true);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<bool>(), true);

  EXPECT_EQ(col->column_info(), "ValueColumn<bool>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kBoolean);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<bool>(), true);
    EXPECT_EQ(shuffled->get_elem(1).GetValue<bool>(), true);
    EXPECT_EQ(shuffled->get_elem(2).GetValue<bool>(), false);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<bool>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), false);
    EXPECT_EQ(opt_col->get_value(2), true);
  }

  // union
  {
    ValueColumnBuilder<bool> builder2;
    builder2.push_back_opt(true);
    builder2.push_back_opt(false);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<bool>(), true);
    EXPECT_EQ(unioned->get_elem(3).GetValue<bool>(), true);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(col->get_value(offsets[0]), true);
    EXPECT_EQ(col->get_value(offsets[1]), true);
    EXPECT_EQ(col->get_value(offsets[2]), false);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), true);
    EXPECT_EQ(col->get_value(offsets[1]), true);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 2);
  }
}

TEST_F(ValueColumnTest, I32ValueColumnBasic) {
  ValueColumnBuilder<int32_t> builder;
  builder.push_back_opt(10);
  builder.push_back_opt(20);
  builder.push_back_elem(Value::INT32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<int32_t>(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<int32>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt32);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<int32_t>(), 30);
    EXPECT_EQ(shuffled->get_elem(1).GetValue<int32_t>(), 10);
    EXPECT_EQ(shuffled->get_elem(2).GetValue<int32_t>(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<int32_t>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), 20);
    EXPECT_EQ(opt_col->get_value(2), 10);
  }

  // union
  {
    ValueColumnBuilder<int32_t> builder2;
    builder2.push_back_opt(40);
    builder2.push_back_opt(50);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<int32_t>(), 10);
    EXPECT_EQ(unioned->get_elem(3).GetValue<int32_t>(), 40);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 10);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 30);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, I64ValueColumnBasic) {
  ValueColumnBuilder<int64_t> builder;
  builder.push_back_opt(10);
  builder.push_back_opt(20);
  builder.push_back_elem(Value::INT64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<int64_t>(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<int64>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt64);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<int64_t>(), 30);
    EXPECT_EQ(shuffled->get_elem(1).GetValue<int64_t>(), 10);
    EXPECT_EQ(shuffled->get_elem(2).GetValue<int64_t>(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<int64_t>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), 20);
    EXPECT_EQ(opt_col->get_value(2), 10);
  }

  // union
  {
    ValueColumnBuilder<int64_t> builder2;
    builder2.push_back_opt(40);
    builder2.push_back_opt(50);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<int64_t>(), 10);
    EXPECT_EQ(unioned->get_elem(3).GetValue<int64_t>(), 40);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 10);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 30);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, U32ValueColumnBasic) {
  ValueColumnBuilder<uint32_t> builder;
  builder.push_back_opt(10);
  builder.push_back_opt(20);
  builder.push_back_elem(Value::UINT32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<uint32_t>(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<uint32>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kUInt32);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<uint32_t>(), 30);
    EXPECT_EQ(shuffled->get_elem(1).GetValue<uint32_t>(), 10);
    EXPECT_EQ(shuffled->get_elem(2).GetValue<uint32_t>(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), 20);
    EXPECT_EQ(opt_col->get_value(2), 10);
  }

  // union
  {
    ValueColumnBuilder<uint32_t> builder2;
    builder2.push_back_opt(40);
    builder2.push_back_opt(50);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<uint32_t>(), 10);
    EXPECT_EQ(unioned->get_elem(3).GetValue<uint32_t>(), 40);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 10);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 30);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, U64ValueColumnBasic) {
  ValueColumnBuilder<uint64_t> builder;
  builder.push_back_opt(10);
  builder.push_back_opt(20);
  builder.push_back_elem(Value::UINT64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<uint64_t>(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<uint64>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kUInt64);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<uint64_t>(), 30);
    EXPECT_EQ(shuffled->get_elem(1).GetValue<uint64_t>(), 10);
    EXPECT_EQ(shuffled->get_elem(2).GetValue<uint64_t>(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), 20);
    EXPECT_EQ(opt_col->get_value(2), 10);
  }

  // union
  {
    ValueColumnBuilder<uint64_t> builder2;
    builder2.push_back_opt(40);
    builder2.push_back_opt(50);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<uint64_t>(), 10);
    EXPECT_EQ(unioned->get_elem(3).GetValue<uint64_t>(), 40);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 10);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), 20);
    EXPECT_EQ(col->get_value(offsets[1]), 30);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, F32ValueColumnBasic) {
  ValueColumnBuilder<float> builder;
  builder.push_back_opt(10.42);
  builder.push_back_opt(20.43);
  builder.push_back_elem(Value::FLOAT(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<float>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_FLOAT_EQ(col->get_value(0), 10.42);
  EXPECT_FLOAT_EQ(col->get_value(1), 20.43);
  EXPECT_FLOAT_EQ(col->get_value(2), 30.44);

  Value elem0 = col->get_elem(0);
  EXPECT_FLOAT_EQ(elem0.GetValue<float>(), 10.42);

  EXPECT_EQ(col->column_info(), "ValueColumn<float>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kFloat);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_FLOAT_EQ(shuffled->get_elem(0).GetValue<float>(), 30.44);
    EXPECT_FLOAT_EQ(shuffled->get_elem(1).GetValue<float>(), 10.42);
    EXPECT_FLOAT_EQ(shuffled->get_elem(2).GetValue<float>(), 20.43);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<float>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_FLOAT_EQ(opt_col->get_value(0), 20.43);
    EXPECT_FLOAT_EQ(opt_col->get_value(2), 10.42);
  }

  // union
  {
    ValueColumnBuilder<float> builder2;
    builder2.push_back_opt(40.44);
    builder2.push_back_opt(50.45);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_FLOAT_EQ(unioned->get_elem(0).GetValue<float>(), 10.42);
    EXPECT_FLOAT_EQ(unioned->get_elem(3).GetValue<float>(), 40.44);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_FLOAT_EQ(col->get_value(offsets[0]), 20.43);
    EXPECT_FLOAT_EQ(col->get_value(offsets[1]), 10.42);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_FLOAT_EQ(col->get_value(offsets[0]), 20.43);
    EXPECT_FLOAT_EQ(col->get_value(offsets[1]), 30.44);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, F64ValueColumnBasic) {
  ValueColumnBuilder<double> builder;
  builder.push_back_opt(10.42);
  builder.push_back_opt(20.43);
  builder.push_back_elem(Value::DOUBLE(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<double>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_DOUBLE_EQ(col->get_value(0), 10.42);
  EXPECT_DOUBLE_EQ(col->get_value(1), 20.43);
  EXPECT_DOUBLE_EQ(col->get_value(2), 30.44);

  Value elem0 = col->get_elem(0);
  EXPECT_DOUBLE_EQ(elem0.GetValue<double>(), 10.42);

  EXPECT_EQ(col->column_info(), "ValueColumn<double>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kDouble);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_DOUBLE_EQ(shuffled->get_elem(0).GetValue<double>(), 30.44);
    EXPECT_DOUBLE_EQ(shuffled->get_elem(1).GetValue<double>(), 10.42);
    EXPECT_DOUBLE_EQ(shuffled->get_elem(2).GetValue<double>(), 20.43);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<double>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_DOUBLE_EQ(opt_col->get_value(0), 20.43);
    EXPECT_DOUBLE_EQ(opt_col->get_value(2), 10.42);
  }

  // union
  {
    ValueColumnBuilder<double> builder2;
    builder2.push_back_opt(40.44);
    builder2.push_back_opt(50.45);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_DOUBLE_EQ(unioned->get_elem(0).GetValue<double>(), 10.42);
    EXPECT_DOUBLE_EQ(unioned->get_elem(3).GetValue<double>(), 40.44);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_DOUBLE_EQ(col->get_value(offsets[0]), 20.43);
    EXPECT_DOUBLE_EQ(col->get_value(offsets[1]), 10.42);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_DOUBLE_EQ(col->get_value(offsets[0]), 20.43);
    EXPECT_DOUBLE_EQ(col->get_value(offsets[1]), 30.44);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, ValueColumnStringBasic) {
  std::string s1 = "hello";
  std::string s2 = "world";
  std::string s3 = "!!!";

  ValueColumnBuilder<std::string> builder;
  builder.push_back_opt(s1);
  builder.push_back_opt(s2);
  builder.push_back_elem(Value::STRING(s3));
  auto col =
      std::dynamic_pointer_cast<ValueColumn<std::string>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), "hello");
  EXPECT_EQ(col->get_value(1), "world");
  EXPECT_EQ(col->get_value(2), "!!!");

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(StringValue::Get(elem0), "hello");
  EXPECT_EQ(col->column_info(), "ValueColumn<string>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kVarchar);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(StringValue::Get(shuffled->get_elem(0)), "!!!");
    EXPECT_EQ(StringValue::Get(shuffled->get_elem(1)), "hello");
    EXPECT_EQ(StringValue::Get(shuffled->get_elem(2)), "world");
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<ValueColumn<std::string>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), "world");
    EXPECT_EQ(opt_col->get_value(2), "hello");
  }

  // union
  {
    ValueColumnBuilder<std::string> builder2;
    builder2.push_back_opt("union");
    builder2.push_back_opt("test");
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(StringValue::Get(unioned->get_elem(0)), "hello");
    EXPECT_EQ(StringValue::Get(unioned->get_elem(3)), "union");
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), "hello");
    EXPECT_EQ(col->get_value(offsets[1]), "!!!");

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), "hello");
    EXPECT_EQ(col->get_value(offsets[1]), "world");
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, DateValueColumnBasic) {
  ValueColumnBuilder<Date> builder;
  builder.push_back_opt(Date(10));
  builder.push_back_opt(Date(20));
  builder.push_back_elem(Value::DATE(Date(30)));
  auto col = std::dynamic_pointer_cast<ValueColumn<Date>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), Date(10));
  EXPECT_EQ(col->get_value(1), Date(20));
  EXPECT_EQ(col->get_value(2), Date(30));

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<Date>(), Date(10));

  EXPECT_EQ(col->column_info(), "ValueColumn<date>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kDate);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<Date>(), Date(30));
    EXPECT_EQ(shuffled->get_elem(1).GetValue<Date>(), Date(10));
    EXPECT_EQ(shuffled->get_elem(2).GetValue<Date>(), Date(20));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<Date>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), Date(20));
    EXPECT_EQ(opt_col->get_value(2), Date(10));
  }

  // union
  {
    ValueColumnBuilder<Date> builder2;
    builder2.push_back_opt(Date(40));
    builder2.push_back_opt(Date(50));
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<Date>(), Date(10));
    EXPECT_EQ(unioned->get_elem(3).GetValue<Date>(), Date(40));
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), Date(20));
    EXPECT_EQ(col->get_value(offsets[1]), Date(10));

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), Date(20));
    EXPECT_EQ(col->get_value(offsets[1]), Date(30));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, DateTimeValueColumnBasic) {
  ValueColumnBuilder<DateTime> builder;
  builder.push_back_opt(DateTime(1766386400000));
  builder.push_back_opt(DateTime(1766388400000));
  builder.push_back_elem(Value::TIMESTAMPMS(DateTime(1766389400000)));
  auto col = std::dynamic_pointer_cast<ValueColumn<DateTime>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), DateTime(1766386400000));
  EXPECT_EQ(col->get_value(1), DateTime(1766388400000));
  EXPECT_EQ(col->get_value(2), DateTime(1766389400000));

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<DateTime>(), DateTime(1766386400000));

  EXPECT_EQ(col->column_info(), "ValueColumn<timestamp_ms>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kTimestampMs);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<DateTime>(),
              DateTime(1766389400000));
    EXPECT_EQ(shuffled->get_elem(1).GetValue<DateTime>(),
              DateTime(1766386400000));
    EXPECT_EQ(shuffled->get_elem(2).GetValue<DateTime>(),
              DateTime(1766388400000));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<DateTime>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), DateTime(1766388400000));
    EXPECT_EQ(opt_col->get_value(2), DateTime(1766386400000));
  }

  // union
  {
    ValueColumnBuilder<DateTime> builder2;
    builder2.push_back_opt(DateTime(1766346400000));
    builder2.push_back_opt(DateTime(1766406400000));
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<DateTime>(),
              DateTime(1766386400000));
    EXPECT_EQ(unioned->get_elem(3).GetValue<DateTime>(),
              DateTime(1766346400000));
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), DateTime(1766388400000));
    EXPECT_EQ(col->get_value(offsets[1]), DateTime(1766386400000));

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), DateTime(1766388400000));
    EXPECT_EQ(col->get_value(offsets[1]), DateTime(1766389400000));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, IntervalValueColumnBasic) {
  ValueColumnBuilder<Interval> builder;
  builder.push_back_opt(Interval(std::string("3years 2months")));
  builder.push_back_opt(Interval(std::string("9hours")));
  builder.push_back_elem(Value::INTERVAL(Interval(std::string("43minutes"))));
  auto col = std::dynamic_pointer_cast<ValueColumn<Interval>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), Interval(std::string("3years 2months")));
  EXPECT_EQ(col->get_value(1), Interval(std::string("9hours")));
  EXPECT_EQ(col->get_value(2), Interval(std::string("43minutes")));

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.GetValue<Interval>(),
            Interval(std::string("3years 2months")));

  EXPECT_EQ(col->column_info(), "ValueColumn<interval>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInterval);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).GetValue<Interval>(),
              Interval(std::string("43minutes")));
    EXPECT_EQ(shuffled->get_elem(1).GetValue<Interval>(),
              Interval(std::string("3years 2months")));
    EXPECT_EQ(shuffled->get_elem(2).GetValue<Interval>(),
              Interval(std::string("9hours")));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<ValueColumn<Interval>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), Interval(std::string("9hours")));
    EXPECT_EQ(opt_col->get_value(2), Interval(std::string("3years 2months")));
  }

  // union
  {
    ValueColumnBuilder<Interval> builder2;
    builder2.push_back_opt(Interval(std::string("58months")));
    builder2.push_back_opt(Interval(std::string("78hours")));
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).GetValue<Interval>(),
              Interval(std::string("3years 2months")));
    EXPECT_EQ(unioned->get_elem(3).GetValue<Interval>(),
              Interval(std::string("58months")));
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), Interval(std::string("9hours")));
    EXPECT_EQ(col->get_value(offsets[1]), Interval(std::string("43minutes")));

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), Interval(std::string("9hours")));
    EXPECT_EQ(col->get_value(offsets[1]),
              Interval(std::string("3years 2months")));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, TupleValueColumnBasic) {
  // Create struct type with child types
  std::vector<DataType> child_types = {DataType(DataTypeId::kInt32),
                                       DataType(DataTypeId::kInt64),
                                       DataType(DataTypeId::kDouble)};
  DataType struct_type = DataType::Struct(child_types);

  StructColumnBuilder builder(struct_type);

  // Add first element
  std::vector<Value> values1;
  values1.emplace_back(Value::INT32(1));
  values1.emplace_back(Value::INT64(2));
  values1.emplace_back(Value::DOUBLE(-3.0));
  builder.push_back_elem(Value::STRUCT(std::move(values1)));

  // Add second element
  std::vector<Value> values2;
  values2.emplace_back(Value::INT32(10));
  values2.emplace_back(Value::INT64(3));
  values2.emplace_back(Value::DOUBLE(2.1));
  builder.push_back_elem(Value::STRUCT(std::move(values2)));

  // Add third element
  std::vector<Value> values3;
  values3.emplace_back(Value::INT32(15));
  values3.emplace_back(Value::INT64(-2));
  values3.emplace_back(Value::DOUBLE(3.6));
  builder.push_back_elem(Value::STRUCT(std::move(values3)));

  auto col = std::dynamic_pointer_cast<StructColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);

  Value elem0 = col->get_elem(0);
  auto children = StructValue::GetChildren(elem0);
  ASSERT_EQ(children.size(), 3);
  EXPECT_EQ(children[0].GetValue<int32_t>(), 1);
  EXPECT_EQ(children[1].GetValue<int64_t>(), 2);
  EXPECT_DOUBLE_EQ(children[2].GetValue<double>(), -3.0);

  EXPECT_EQ(col->column_info(), "StructColumn[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kStruct);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto children0 = StructValue::GetChildren(shuffled->get_elem(0));
    ASSERT_EQ(children0.size(), 3);
    EXPECT_EQ(children0[0].GetValue<int32_t>(), 15);
    EXPECT_EQ(children0[1].GetValue<int64_t>(), -2);
    EXPECT_DOUBLE_EQ(children0[2].GetValue<double>(), 3.6);

    auto children1 = StructValue::GetChildren(shuffled->get_elem(1));
    ASSERT_EQ(children1.size(), 3);
    EXPECT_EQ(children1[0].GetValue<int32_t>(), 1);
    EXPECT_EQ(children1[1].GetValue<int64_t>(), 2);
    EXPECT_DOUBLE_EQ(children1[2].GetValue<double>(), -3.0);

    auto children2 = StructValue::GetChildren(shuffled->get_elem(2));
    ASSERT_EQ(children2.size(), 3);
    EXPECT_EQ(children2[0].GetValue<int32_t>(), 10);
    EXPECT_EQ(children2[1].GetValue<int64_t>(), 3);
    EXPECT_DOUBLE_EQ(children2[2].GetValue<double>(), 2.1);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col = std::dynamic_pointer_cast<StructColumn>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->is_optional());
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    auto children0 = StructValue::GetChildren(opt_col->get_elem(0));
    EXPECT_EQ(children0[0].GetValue<int32_t>(), 10);
    EXPECT_EQ(children0[1].GetValue<int64_t>(), 3);
    EXPECT_DOUBLE_EQ(children0[2].GetValue<double>(), 2.1);

    auto children2 = StructValue::GetChildren(opt_col->get_elem(2));
    EXPECT_EQ(children2[0].GetValue<int32_t>(), 1);
    EXPECT_EQ(children2[1].GetValue<int64_t>(), 2);
    EXPECT_DOUBLE_EQ(children2[2].GetValue<double>(), -3.0);
  }

  // union
  /*{
    StructColumnBuilder builder2(struct_type);
    std::vector<Value> values4;
    values4.emplace_back(Value::INT32(19));
    values4.emplace_back(Value::INT64(-2));
    values4.emplace_back(Value::DOUBLE(3.6));
    builder2.push_back_elem(Value::STRUCT(std::move(values4)));
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 4);

    auto children0 = StructValue::GetChildren(unioned->get_elem(0));
    ASSERT_EQ(children0.size(), 3);
    EXPECT_EQ(children0[0].GetValue<int32_t>(), 1);
    EXPECT_EQ(children0[1].GetValue<int64_t>(), 2);
    EXPECT_DOUBLE_EQ(children0[2].GetValue<double>(), -3.0);

    auto children3 = StructValue::GetChildren(unioned->get_elem(3));
    ASSERT_EQ(children3.size(), 3);
    EXPECT_EQ(children3[0].GetValue<int32_t>(), 19);
    EXPECT_EQ(children3[1].GetValue<int64_t>(), -2);
    EXPECT_DOUBLE_EQ(children3[2].GetValue<double>(), 3.6);
  }*/

  // TODO: order_by_limit and dedup not applicable to StructColumn
  // These methods use get_value() which is specific to ValueColumn<T>
  // order_by_limit
  /*
   {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), tuple2);
    EXPECT_EQ(col->get_value(offsets[1]), tuple1);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), tuple2);
    EXPECT_EQ(col->get_value(offsets[1]), tuple3);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }*/
}

TEST_F(ValueColumnTest, ListColumnBasic) {
  DataType list_type = DataType(DataTypeId::kInt32);

  ListColumnBuilder builder(list_type);

  // Add first list [10, 20]
  std::vector<Value> list_values1;
  list_values1.emplace_back(Value::INT32(10));
  list_values1.emplace_back(Value::INT32(20));
  builder.push_back_elem(Value::LIST(list_type, std::move(list_values1)));

  // Add second list [30]
  std::vector<Value> list_values2;
  list_values2.emplace_back(Value::INT32(30));
  builder.push_back_elem(Value::LIST(list_type, std::move(list_values2)));

  auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());
  // EXPECT_EQ(col->get_value(0), list1);
  // EXPECT_EQ(col->get_value(1), list2);

  // RTAny elem0 = col->get_elem(0);
  // EXPECT_EQ(elem0.as_list(), list1);

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 2);

  EXPECT_EQ(col->column_info(), "ListColumn[2]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kList);

  // // shuffle
  // {
  //   std::vector<size_t> offsets = {1, 0};
  //   auto shuffled = col->shuffle(offsets);
  //   ASSERT_EQ(shuffled->size(), 2);

  //   auto* list_col = dynamic_cast<ListValueColumn*>(shuffled.get());
  //   ASSERT_NE(list_col, nullptr);
  //   EXPECT_EQ(list_col->get_value(0).get(0).as_int32(), 30);
  //   EXPECT_EQ(list_col->get_value(1).get(0).as_int32(), 10);
  // }
}

class OptionalValueColumnTest : public ::testing::Test {};

TEST_F(OptionalValueColumnTest, BoolOptionalValueColumnBasic) {
  ValueColumnBuilder<bool> builder(true);
  builder.push_back_opt(true);
  builder.push_back_null();
  builder.push_back_elem(Value::BOOLEAN(false));
  auto col = std::dynamic_pointer_cast<ValueColumn<bool>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<bool>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kBoolean);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<bool>(), true);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<bool>(), false);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<bool>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, I32OptionalValueColumnBasic) {
  ValueColumnBuilder<int32_t> builder(true);
  builder.push_back_opt(10);
  builder.push_back_null();
  builder.push_back_elem(Value::INT32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<int32>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt32);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<int32_t>(), 10);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<int32_t>(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<int32_t>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, I64OptionalValueColumnBasic) {
  ValueColumnBuilder<int64_t> builder(true);
  builder.push_back_opt(10);
  builder.push_back_null();
  builder.push_back_elem(Value::INT64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<int64>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt64);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<int64_t>(), 10);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<int64_t>(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<int64_t>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, U32OptionalValueColumnBasic) {
  ValueColumnBuilder<uint32_t> builder(true);
  builder.push_back_opt(10);
  builder.push_back_null();
  builder.push_back_elem(Value::UINT32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<uint32>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kUInt32);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<uint32_t>(), 10);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<uint32_t>(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, U64OptionalValueColumnBasic) {
  ValueColumnBuilder<uint64_t> builder(true);
  builder.push_back_opt(10);
  builder.push_back_null();
  builder.push_back_elem(Value::UINT64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<uint64>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kUInt64);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<uint64_t>(), 10);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<uint64_t>(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, F32OptionalValueColumnBasic) {
  ValueColumnBuilder<float> builder(true);
  builder.push_back_opt(10.42);
  builder.push_back_null();
  builder.push_back_elem(Value::FLOAT(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<float>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<float>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kFloat);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_FLOAT_EQ(col->get_elem(0).GetValue<float>(), 10.42);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_FLOAT_EQ(col->get_elem(2).GetValue<float>(), 30.44);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<float>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, F64OptionalValueColumnBasic) {
  ValueColumnBuilder<double> builder(true);
  builder.push_back_opt(10.42);
  builder.push_back_null();
  builder.push_back_elem(Value::DOUBLE(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<double>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<double>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kDouble);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_DOUBLE_EQ(col->get_elem(0).GetValue<double>(), 10.42);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_DOUBLE_EQ(col->get_elem(2).GetValue<double>(), 30.44);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<double>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, StringOptionalValueColumnBasic) {
  std::string s1 = "hello";
  std::string s2 = "world";

  ValueColumnBuilder<std::string> builder(true);
  builder.push_back_opt(s1);
  builder.push_back_null();
  builder.push_back_elem(Value::STRING(s2));
  auto col =
      std::dynamic_pointer_cast<ValueColumn<std::string>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<string>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kVarchar);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(StringValue::Get(col->get_elem(0)), "hello");
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(StringValue::Get(col->get_elem(2)), "world");

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<ValueColumn<std::string>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, DateOptionalValueColumnBasic) {
  ValueColumnBuilder<Date> builder(true);
  builder.push_back_opt(Date(10));
  builder.push_back_null();
  builder.push_back_elem(Value::DATE(Date(30)));
  auto col = std::dynamic_pointer_cast<ValueColumn<Date>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<date>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kDate);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<Date>(), Date(10));
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<Date>(), Date(30));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<Date>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, DateTimeOptionalValueColumnBasic) {
  ValueColumnBuilder<DateTime> builder(true);
  builder.push_back_opt(DateTime(10));
  builder.push_back_null();
  builder.push_back_elem(Value::TIMESTAMPMS(DateTime(30)));
  auto col = std::dynamic_pointer_cast<ValueColumn<DateTime>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<timestamp_ms>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kTimestampMs);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<DateTime>(), DateTime(10));
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<DateTime>(), DateTime(30));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<DateTime>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, IntervalOptionalValueColumnBasic) {
  ValueColumnBuilder<Interval> builder(true);
  builder.push_back_opt(Interval(std::string("3years")));
  builder.push_back_null();
  builder.push_back_elem(Value::INTERVAL(Interval(std::string("24months"))));
  auto col = std::dynamic_pointer_cast<ValueColumn<Interval>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "ValueColumn<interval>[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInterval);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).GetValue<Interval>(),
            Interval(std::string("3years")));
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(col->get_elem(2).GetValue<Interval>(),
            Interval(std::string("24months")));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<ValueColumn<Interval>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(OptionalValueColumnTest, TupleOptionalValueColumnBasic) {
  // Create struct type with child types
  std::vector<DataType> child_types = {DataType(DataTypeId::kInt32),
                                       DataType(DataTypeId::kInt64),
                                       DataType(DataTypeId::kDouble)};
  DataType struct_type = DataType::Struct(child_types);

  StructColumnBuilder builder(struct_type);

  // Add first element
  std::vector<Value> values1;
  values1.emplace_back(Value::INT32(1));
  values1.emplace_back(Value::INT64(2));
  values1.emplace_back(Value::DOUBLE(-3.0));
  builder.push_back_elem(Value::STRUCT(std::move(values1)));

  // Add null
  builder.push_back_null();

  // Add third element
  std::vector<Value> values2;
  values2.emplace_back(Value::INT32(10));
  values2.emplace_back(Value::INT64(3));
  values2.emplace_back(Value::DOUBLE(2.1));
  builder.push_back_elem(Value::STRUCT(std::move(values2)));

  auto col = std::dynamic_pointer_cast<StructColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "StructColumn[3]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kStruct);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  auto children0 = StructValue::GetChildren(col->get_elem(0));
  ASSERT_EQ(children0.size(), 3);
  EXPECT_EQ(children0[0].GetValue<int32_t>(), 1);
  EXPECT_EQ(children0[1].GetValue<int64_t>(), 2);
  EXPECT_DOUBLE_EQ(children0[2].GetValue<double>(), -3.0);

  EXPECT_TRUE(col->get_elem(1).IsNull());

  auto children2 = StructValue::GetChildren(col->get_elem(2));
  ASSERT_EQ(children2.size(), 3);
  EXPECT_EQ(children2[0].GetValue<int32_t>(), 10);
  EXPECT_EQ(children2[1].GetValue<int64_t>(), 3);
  EXPECT_DOUBLE_EQ(children2[2].GetValue<double>(), 2.1);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col = std::dynamic_pointer_cast<StructColumn>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }
}

class ListColumnTest : public ::testing::Test {};

TEST_F(ListColumnTest, ListColumnBasic) {
  ListColumnBuilder builder(DataTypeId::kInt32);

  // Add first list [10, 20]
  std::vector<Value> list_values1;
  list_values1.emplace_back(Value::INT32(10));
  list_values1.emplace_back(Value::INT32(20));
  builder.push_back_elem(
      Value::LIST(DataType(DataTypeId::kInt32), std::move(list_values1)));

  // Add second list [30]
  std::vector<Value> list_values2;
  list_values2.emplace_back(Value::INT32(30));
  builder.push_back_elem(
      Value::LIST(DataType(DataTypeId::kInt32), std::move(list_values2)));

  auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 2);
  EXPECT_EQ(ListValue::GetChildren(col->get_elem(0)).size(), 2);
  EXPECT_EQ(ListValue::GetChildren(col->get_elem(1)).size(), 1);

  EXPECT_EQ(col->column_info(), "ListColumn[2]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kList);

  // Check first list [10, 20]
  auto list0_children = ListValue::GetChildren(col->get_elem(0));
  ASSERT_EQ(list0_children.size(), 2);
  EXPECT_EQ(list0_children[0].GetValue<int32_t>(), 10);
  EXPECT_EQ(list0_children[1].GetValue<int32_t>(), 20);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 2);

    auto* list_col = dynamic_cast<ListColumn*>(shuffled.get());
    ASSERT_NE(list_col, nullptr);

    // Check first list (which should be list2 after shuffle: [30])
    auto list0_children = ListValue::GetChildren(list_col->get_elem(0));
    ASSERT_EQ(list0_children.size(), 1);
    EXPECT_EQ(list0_children[0].GetValue<int32_t>(), 30);

    // Check second list (which should be list1 after shuffle: [10, 20])
    auto list1_children = ListValue::GetChildren(list_col->get_elem(1));
    ASSERT_EQ(list1_children.size(), 2);
    EXPECT_EQ(list1_children[0].GetValue<int32_t>(), 10);
    EXPECT_EQ(list1_children[1].GetValue<int32_t>(), 20);
  }
}

TEST_F(ValueColumnTest, ListColumnUnfold) {
  // unfold i32
  {
    ListColumnBuilder builder(DataTypeId::kInt32);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::INT32(10));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInt32), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::INT32(20));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInt32), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<int32_t>(), 10);
    EXPECT_EQ(unfurled->get_elem(1).GetValue<int32_t>(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold i64
  {
    ListColumnBuilder builder(
        (DataType(DataTypeId::kInt64)));  // -Wvexing-parse

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::INT64(10));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInt64), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::INT64(20));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInt64), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<int64_t>(), 10);
    EXPECT_EQ(unfurled->get_elem(1).GetValue<int64_t>(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold u32
  {
    ListColumnBuilder builder(DataTypeId::kUInt32);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::UINT32(10));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kUInt32), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::UINT32(20));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kUInt32), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<uint32_t>(), 10);
    EXPECT_EQ(unfurled->get_elem(1).GetValue<uint32_t>(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold u64
  {
    ListColumnBuilder builder(DataTypeId::kUInt64);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::UINT64(10));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kUInt64), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::UINT64(20));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kUInt64), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<uint64_t>(), 10);
    EXPECT_EQ(unfurled->get_elem(1).GetValue<uint64_t>(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold f32
  {
    ListColumnBuilder builder(DataTypeId::kFloat);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::FLOAT(10.42));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kFloat), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::FLOAT(20.43));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kFloat), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_FLOAT_EQ(unfurled->get_elem(0).GetValue<float>(), 10.42);
    EXPECT_FLOAT_EQ(unfurled->get_elem(1).GetValue<float>(), 20.43);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold f64
  {
    ListColumnBuilder builder(DataTypeId::kDouble);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::DOUBLE(10.42));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kDouble), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::DOUBLE(20.43));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kDouble), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_DOUBLE_EQ(unfurled->get_elem(0).GetValue<double>(), 10.42);
    EXPECT_DOUBLE_EQ(unfurled->get_elem(1).GetValue<double>(), 20.43);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold string_view
  {
    std::string s1 = "hello";
    std::string s2 = "world";

    ListColumnBuilder builder(
        (DataType(DataTypeId::kVarchar)));  // -Wvexing-parse

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::STRING(s1));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kVarchar), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::STRING(s2));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kVarchar), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(StringValue::Get(unfurled->get_elem(0)), "hello");
    EXPECT_EQ(StringValue::Get(unfurled->get_elem(1)), "world");

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold Date
  {
    ListColumnBuilder builder(DataTypeId::kDate);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::DATE(Date(10)));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kDate), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::DATE(Date(20)));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kDate), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<Date>(), Date(10));
    EXPECT_EQ(unfurled->get_elem(1).GetValue<Date>(), Date(20));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold DateTime
  {
    ListColumnBuilder builder(DataTypeId::kTimestampMs);

    std::vector<Value> list_values1;
    list_values1.emplace_back(Value::TIMESTAMPMS(DateTime(10)));
    builder.push_back_elem(Value::LIST(DataType(DataTypeId::kTimestampMs),
                                       std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(Value::TIMESTAMPMS(DateTime(20)));
    builder.push_back_elem(Value::LIST(DataType(DataTypeId::kTimestampMs),
                                       std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<DateTime>(), DateTime(10));
    EXPECT_EQ(unfurled->get_elem(1).GetValue<DateTime>(), DateTime(20));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold Interval
  {
    ListColumnBuilder builder(DataTypeId::kInterval);

    std::vector<Value> list_values1;
    list_values1.emplace_back(
        Value::INTERVAL(Interval(std::string("10months"))));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInterval), std::move(list_values1)));

    std::vector<Value> list_values2;
    list_values2.emplace_back(
        Value::INTERVAL(Interval(std::string("20months"))));
    builder.push_back_elem(
        Value::LIST(DataType(DataTypeId::kInterval), std::move(list_values2)));

    auto col = std::dynamic_pointer_cast<ListColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).GetValue<Interval>(),
              Interval(std::string("10months")));
    EXPECT_EQ(unfurled->get_elem(1).GetValue<Interval>(),
              Interval(std::string("20months")));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }
}

}  // namespace test
}  // namespace execution
}  // namespace neug