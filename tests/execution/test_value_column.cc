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

#include "neug/execution/common/columns/value_columns.h"

namespace gs {
namespace runtime {
namespace test {

class ValueColumnTest : public ::testing::Test {};

TEST_F(ValueColumnTest, BoolValueColumnBasic) {
  ValueColumnBuilder<bool> builder;
  builder.push_back_opt(true);
  builder.push_back_opt(false);
  builder.push_back_elem(RTAny::from_bool(true));
  auto col = std::dynamic_pointer_cast<ValueColumn<bool>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), true);
  EXPECT_EQ(col->get_value(1), false);
  EXPECT_EQ(col->get_value(2), true);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_bool(), true);

  EXPECT_EQ(col->column_info(), "ValueColumn<bool>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kBoolValue);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_bool(), true);
    EXPECT_EQ(shuffled->get_elem(1).as_bool(), true);
    EXPECT_EQ(shuffled->get_elem(2).as_bool(), false);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<bool>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_bool(), true);
    EXPECT_EQ(unioned->get_elem(3).as_bool(), true);
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
  builder.push_back_elem(RTAny::from_int32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_int32(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<int32>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kI32Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_int32(), 30);
    EXPECT_EQ(shuffled->get_elem(1).as_int32(), 10);
    EXPECT_EQ(shuffled->get_elem(2).as_int32(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<int32_t>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_int32(), 10);
    EXPECT_EQ(unioned->get_elem(3).as_int32(), 40);
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
  builder.push_back_elem(RTAny::from_int64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<int64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_int64(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<int64>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kI64Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_int64(), 30);
    EXPECT_EQ(shuffled->get_elem(1).as_int64(), 10);
    EXPECT_EQ(shuffled->get_elem(2).as_int64(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<int64_t>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_int64(), 10);
    EXPECT_EQ(unioned->get_elem(3).as_int64(), 40);
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
  builder.push_back_elem(RTAny::from_uint32(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_uint32(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<uint>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kU32Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_uint32(), 30);
    EXPECT_EQ(shuffled->get_elem(1).as_uint32(), 10);
    EXPECT_EQ(shuffled->get_elem(2).as_uint32(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<uint32_t>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_uint32(), 10);
    EXPECT_EQ(unioned->get_elem(3).as_uint32(), 40);
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
  builder.push_back_elem(RTAny::from_uint64(30));
  auto col = std::dynamic_pointer_cast<ValueColumn<uint64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), 10);
  EXPECT_EQ(col->get_value(1), 20);
  EXPECT_EQ(col->get_value(2), 30);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_uint64(), 10);

  EXPECT_EQ(col->column_info(), "ValueColumn<uint64>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kU64Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_uint64(), 30);
    EXPECT_EQ(shuffled->get_elem(1).as_uint64(), 10);
    EXPECT_EQ(shuffled->get_elem(2).as_uint64(), 20);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<uint64_t>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_uint64(), 10);
    EXPECT_EQ(unioned->get_elem(3).as_uint64(), 40);
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
  builder.push_back_elem(RTAny::from_float(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<float>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_FLOAT_EQ(col->get_value(0), 10.42);
  EXPECT_FLOAT_EQ(col->get_value(1), 20.43);
  EXPECT_FLOAT_EQ(col->get_value(2), 30.44);

  RTAny elem0 = col->get_elem(0);
  EXPECT_FLOAT_EQ(elem0.as_float(), 10.42);

  EXPECT_EQ(col->column_info(), "ValueColumn<float>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kF32Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_FLOAT_EQ(shuffled->get_elem(0).as_float(), 30.44);
    EXPECT_FLOAT_EQ(shuffled->get_elem(1).as_float(), 10.42);
    EXPECT_FLOAT_EQ(shuffled->get_elem(2).as_float(), 20.43);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<float>>(shuffled);
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

    EXPECT_FLOAT_EQ(unioned->get_elem(0).as_float(), 10.42);
    EXPECT_FLOAT_EQ(unioned->get_elem(3).as_float(), 40.44);
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
  builder.push_back_elem(RTAny::from_double(30.44));
  auto col = std::dynamic_pointer_cast<ValueColumn<double>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_DOUBLE_EQ(col->get_value(0), 10.42);
  EXPECT_DOUBLE_EQ(col->get_value(1), 20.43);
  EXPECT_DOUBLE_EQ(col->get_value(2), 30.44);

  RTAny elem0 = col->get_elem(0);
  EXPECT_DOUBLE_EQ(elem0.as_double(), 10.42);

  EXPECT_EQ(col->column_info(), "ValueColumn<double>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kF64Value);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_DOUBLE_EQ(shuffled->get_elem(0).as_double(), 30.44);
    EXPECT_DOUBLE_EQ(shuffled->get_elem(1).as_double(), 10.42);
    EXPECT_DOUBLE_EQ(shuffled->get_elem(2).as_double(), 20.43);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<double>>(shuffled);
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

    EXPECT_DOUBLE_EQ(unioned->get_elem(0).as_double(), 10.42);
    EXPECT_DOUBLE_EQ(unioned->get_elem(3).as_double(), 40.44);
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

TEST_F(ValueColumnTest, ValueColumnStringViewBasic) {
  std::string s1 = "hello";
  std::string s2 = "world";
  std::string s3 = "!!!";

  ValueColumnBuilder<std::string_view> builder;
  builder.push_back_opt(s1);
  builder.push_back_opt(s2);
  builder.push_back_elem(RTAny::from_string(s3));
  auto col = std::dynamic_pointer_cast<ValueColumn<std::string_view>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), "hello");
  EXPECT_EQ(col->get_value(1), "world");
  EXPECT_EQ(col->get_value(2), "!!!");

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_string(), "hello");
  EXPECT_EQ(col->column_info(), "ValueColumn<string_view>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kStringValue);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_string(), "!!!");
    EXPECT_EQ(shuffled->get_elem(1).as_string(), "hello");
    EXPECT_EQ(shuffled->get_elem(2).as_string(), "world");
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<std::string_view>>(
            shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), "world");
    EXPECT_EQ(opt_col->get_value(2), "hello");
  }

  // union
  {
    ValueColumnBuilder<std::string_view> builder2;
    builder2.push_back_opt("union");
    builder2.push_back_opt("test");
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 5);

    EXPECT_EQ(unioned->get_elem(0).as_string(), "hello");
    EXPECT_EQ(unioned->get_elem(3).as_string(), "union");
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
  builder.push_back_elem(RTAny::from_date(Date(30)));
  auto col = std::dynamic_pointer_cast<ValueColumn<Date>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), Date(10));
  EXPECT_EQ(col->get_value(1), Date(20));
  EXPECT_EQ(col->get_value(2), Date(30));

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_date(), Date(10));

  EXPECT_EQ(col->column_info(), "ValueColumn<date>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kDate);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_date(), Date(30));
    EXPECT_EQ(shuffled->get_elem(1).as_date(), Date(10));
    EXPECT_EQ(shuffled->get_elem(2).as_date(), Date(20));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Date>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_date(), Date(10));
    EXPECT_EQ(unioned->get_elem(3).as_date(), Date(40));
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
  builder.push_back_elem(RTAny::from_datetime(DateTime(1766389400000)));
  auto col = std::dynamic_pointer_cast<ValueColumn<DateTime>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), DateTime(1766386400000));
  EXPECT_EQ(col->get_value(1), DateTime(1766388400000));
  EXPECT_EQ(col->get_value(2), DateTime(1766389400000));

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_datetime(), DateTime(1766386400000));

  EXPECT_EQ(col->column_info(), "ValueColumn<datetime>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kDateTime);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_datetime(), DateTime(1766389400000));
    EXPECT_EQ(shuffled->get_elem(1).as_datetime(), DateTime(1766386400000));
    EXPECT_EQ(shuffled->get_elem(2).as_datetime(), DateTime(1766388400000));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<DateTime>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_datetime(), DateTime(1766386400000));
    EXPECT_EQ(unioned->get_elem(3).as_datetime(), DateTime(1766346400000));
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
  builder.push_back_elem(
      RTAny::from_interval(Interval(std::string("43minutes"))));
  auto col = std::dynamic_pointer_cast<ValueColumn<Interval>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), Interval(std::string("3years 2months")));
  EXPECT_EQ(col->get_value(1), Interval(std::string("9hours")));
  EXPECT_EQ(col->get_value(2), Interval(std::string("43minutes")));

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_interval(), Interval(std::string("3years 2months")));

  EXPECT_EQ(col->column_info(), "ValueColumn<interval>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kInterval);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_interval(),
              Interval(std::string("43minutes")));
    EXPECT_EQ(shuffled->get_elem(1).as_interval(),
              Interval(std::string("3years 2months")));
    EXPECT_EQ(shuffled->get_elem(2).as_interval(),
              Interval(std::string("9hours")));
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Interval>>(shuffled);
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

    EXPECT_EQ(unioned->get_elem(0).as_interval(),
              Interval(std::string("3years 2months")));
    EXPECT_EQ(unioned->get_elem(3).as_interval(),
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

TEST_F(ValueColumnTest, SetValueColumnBasic) {
  std::set<int32_t> values1 = {10, 20, 30};
  std::set<int32_t> values2 = {20, 30, 40};
  std::set<int32_t> values3 = {30, 40, 50};

  auto set_impl1 = SetImpl<int32_t>::make_set_impl(std::move(values1));
  auto set_impl2 = SetImpl<int32_t>::make_set_impl(std::move(values2));
  auto set_impl3 = SetImpl<int32_t>::make_set_impl(std::move(values3));

  Set set1(set_impl1.get()), set2(set_impl2.get()), set3(set_impl3.get());

  ValueColumnBuilder<Set> builder;
  builder.push_back_opt(set1);
  builder.push_back_opt(set2);
  builder.push_back_elem(RTAny::from_set(set3));
  auto col = std::dynamic_pointer_cast<ValueColumn<Set>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), set1);
  EXPECT_EQ(col->get_value(1), set2);
  EXPECT_EQ(col->get_value(2), set3);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_set(), set1);

  EXPECT_EQ(col->column_info(), "ValueColumn<set>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kSet);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_set(), set3);
    EXPECT_EQ(shuffled->get_elem(1).as_set(), set1);
    EXPECT_EQ(shuffled->get_elem(2).as_set(), set2);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Set>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), set2);
    EXPECT_EQ(opt_col->get_value(2), set1);
  }

  // union
  {
    std::set<int32_t> values4 = {30, 40, 50};
    auto set_impl4 = SetImpl<int32_t>::make_set_impl(std::move(values4));
    Set set4(set_impl4.get());
    ValueColumnBuilder<Set> builder2;
    builder2.push_back_opt(set4);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 4);

    EXPECT_EQ(unioned->get_elem(0).as_set(), set1);
    EXPECT_EQ(unioned->get_elem(3).as_set(), set4);
  }

  // order_by_limit
  {
    std::vector<size_t> offsets;
    bool success = col->order_by_limit(true, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), set2);
    EXPECT_EQ(col->get_value(offsets[1]), set1);

    offsets.clear();
    success = col->order_by_limit(false, 2, offsets);
    ASSERT_TRUE(success);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(col->get_value(offsets[0]), set2);
    EXPECT_EQ(col->get_value(offsets[1]), set3);
  }

  // dedup
  {
    std::vector<size_t> offsets;
    col->generate_dedup_offset(offsets);
    EXPECT_EQ(offsets.size(), 3);
  }
}

TEST_F(ValueColumnTest, TupleValueColumnBasic) {
  std::tuple<int32_t, int64_t, double> values1 = std::make_tuple(1, 2, -3.0);
  std::tuple<int32_t, int64_t, double> values2 = std::make_tuple(10, 3, 2.1);
  std::tuple<int32_t, int64_t, double> values3 = std::make_tuple(15, -2, 3.6);
  auto tuple_impl1 = Tuple::make_tuple_impl(std::move(values1));
  auto tuple_impl2 = Tuple::make_tuple_impl(std::move(values2));
  auto tuple_impl3 = Tuple::make_tuple_impl(std::move(values3));
  Tuple tuple1(tuple_impl1.get()), tuple2(tuple_impl2.get()),
      tuple3(tuple_impl3.get());

  ValueColumnBuilder<Tuple> builder;
  builder.push_back_opt(tuple1);
  builder.push_back_opt(tuple2);
  builder.push_back_elem(RTAny::from_tuple(tuple3));
  auto col = std::dynamic_pointer_cast<ValueColumn<Tuple>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->get_value(0), tuple1);
  EXPECT_EQ(col->get_value(1), tuple2);
  EXPECT_EQ(col->get_value(2), tuple3);

  RTAny elem0 = col->get_elem(0);
  EXPECT_EQ(elem0.as_tuple(), tuple1);

  EXPECT_EQ(col->column_info(), "ValueColumn<tuple>[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kTuple);

  // shuffle
  {
    std::vector<size_t> offsets = {2, 0, 1};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    EXPECT_EQ(shuffled->get_elem(0).as_tuple(), tuple3);
    EXPECT_EQ(shuffled->get_elem(1).as_tuple(), tuple1);
    EXPECT_EQ(shuffled->get_elem(2).as_tuple(), tuple2);
  }

  // optional shuffle
  {
    std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
    auto shuffled = col->optional_shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 3);

    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Tuple>>(shuffled);
    ASSERT_NE(opt_col, nullptr);
    EXPECT_TRUE(opt_col->has_value(0));
    EXPECT_FALSE(opt_col->has_value(1));
    EXPECT_TRUE(opt_col->has_value(2));

    EXPECT_EQ(opt_col->get_value(0), tuple2);
    EXPECT_EQ(opt_col->get_value(2), tuple1);
  }

  // union
  {
    std::tuple<int32_t, int64_t, double> values4 = std::make_tuple(19, -2, 3.6);
    auto tuple_impl4 = Tuple::make_tuple_impl(std::move(values4));
    Tuple tuple4(tuple_impl4.get());
    ValueColumnBuilder<Tuple> builder2;
    builder2.push_back_opt(tuple4);
    auto col2 = builder2.finish();

    auto unioned = col->union_col(col2);
    ASSERT_EQ(unioned->size(), 4);

    EXPECT_EQ(unioned->get_elem(0).as_tuple(), tuple1);
    EXPECT_EQ(unioned->get_elem(3).as_tuple(), tuple4);
  }

  // order_by_limit
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
  }
}

TEST_F(ValueColumnTest, ListValueColumnBasic) {
  std::vector<int32_t> list_content1 = {10, 20};
  auto impl1 = ListImpl<int32_t>::make_list_impl(std::move(list_content1));
  List list1(impl1.get());

  std::vector<int32_t> list_content2 = {30};
  auto impl2 = ListImpl<int32_t>::make_list_impl(std::move(list_content2));
  List list2(impl2.get());

  ValueColumnBuilder<List> builder;
  builder.push_back_opt(list1);
  builder.push_back_elem(RTAny::from_list(list2));
  auto col = std::dynamic_pointer_cast<ValueColumn<List>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 2);
  // EXPECT_EQ(col->get_value(0), list1);
  // EXPECT_EQ(col->get_value(1), list2);

  // RTAny elem0 = col->get_elem(0);
  // EXPECT_EQ(elem0.as_list(), list1);

  EXPECT_EQ(col->column_info(), "ValueColumn<list>[2]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kList);

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
  OptionalValueColumnBuilder<bool> builder;
  builder.push_back_opt(true, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_bool(false));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<bool>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<bool>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kBoolValue);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_bool(), true);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_bool(), false);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<bool>>(shuffled);

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
  OptionalValueColumnBuilder<int32_t> builder;
  builder.push_back_opt(10, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_int32(30));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<int32_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<int32>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kI32Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_int32(), 10);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_int32(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<int32_t>>(shuffled);

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
  OptionalValueColumnBuilder<int64_t> builder;
  builder.push_back_opt(10, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_int64(30));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<int64_t>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<int64>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kI64Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_int64(), 10);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_int64(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<int64_t>>(shuffled);

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
  OptionalValueColumnBuilder<uint32_t> builder;
  builder.push_back_opt(10, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_uint32(30));
  auto col = std::dynamic_pointer_cast<OptionalValueColumn<uint32_t>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<uint>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kU32Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_uint32(), 10);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_uint32(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<uint32_t>>(shuffled);

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
  OptionalValueColumnBuilder<uint64_t> builder;
  builder.push_back_opt(10, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_uint64(30));
  auto col = std::dynamic_pointer_cast<OptionalValueColumn<uint64_t>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<uint64>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kU64Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_uint64(), 10);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_uint64(), 30);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<uint64_t>>(shuffled);

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
  OptionalValueColumnBuilder<float> builder;
  builder.push_back_opt(10.42, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_float(30.44));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<float>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<float>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kF32Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_FLOAT_EQ(col->get_elem(0).as_float(), 10.42);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_FLOAT_EQ(col->get_elem(2).as_float(), 30.44);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<float>>(shuffled);

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
  OptionalValueColumnBuilder<double> builder;
  builder.push_back_opt(10.42, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_double(30.44));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<double>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<double>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kF64Value);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_DOUBLE_EQ(col->get_elem(0).as_double(), 10.42);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_DOUBLE_EQ(col->get_elem(2).as_double(), 30.44);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<double>>(shuffled);

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

  OptionalValueColumnBuilder<std::string_view> builder;
  builder.push_back_opt(s1, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_string(s2));
  auto col = std::dynamic_pointer_cast<OptionalValueColumn<std::string_view>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<string_view>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kStringValue);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_string(), "hello");
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_string(), "world");

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<std::string_view>>(
            shuffled);

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
  OptionalValueColumnBuilder<Date> builder;
  builder.push_back_opt(Date(10), true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_date(Date(30)));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<Date>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<date>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kDate);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_date(), Date(10));
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_date(), Date(30));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Date>>(shuffled);

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
  OptionalValueColumnBuilder<DateTime> builder;
  builder.push_back_opt(DateTime(10), true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_datetime(DateTime(30)));
  auto col = std::dynamic_pointer_cast<OptionalValueColumn<DateTime>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<datetime>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kDateTime);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_datetime(), DateTime(10));
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_datetime(), DateTime(30));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<DateTime>>(shuffled);

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
  OptionalValueColumnBuilder<Interval> builder;
  builder.push_back_opt(Interval(std::string("3years")), true);
  builder.push_back_null();
  builder.push_back_elem(
      RTAny::from_interval(Interval(std::string("24months"))));
  auto col = std::dynamic_pointer_cast<OptionalValueColumn<Interval>>(
      builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<interval>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kInterval);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_interval(), Interval(std::string("3years")));
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_interval(), Interval(std::string("24months")));

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Interval>>(shuffled);

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

TEST_F(OptionalValueColumnTest, SetOptionalValueColumnBasic) {
  std::set<int32_t> values1 = {10, 20, 30};
  std::set<int32_t> values2 = {10, 20, 30};

  auto set_impl1 = SetImpl<int32_t>::make_set_impl(std::move(values1));
  auto set_impl2 = SetImpl<int32_t>::make_set_impl(std::move(values2));

  Set set1(set_impl1.get()), set2(set_impl2.get());

  OptionalValueColumnBuilder<Set> builder;
  builder.push_back_opt(set1, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_set(set2));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<Set>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<set>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kSet);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_set(), set1);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_set(), set2);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Set>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }
}

TEST_F(OptionalValueColumnTest, TupleOptionalValueColumnBasic) {
  std::tuple<int32_t, int64_t, double> values1 = std::make_tuple(1, 2, -3.0);
  std::tuple<int32_t, int64_t, double> values2 = std::make_tuple(10, 3, 2.1);
  auto tuple_impl1 = Tuple::make_tuple_impl(std::move(values1));
  auto tuple_impl2 = Tuple::make_tuple_impl(std::move(values2));
  Tuple tuple1(tuple_impl1.get()), tuple2(tuple_impl2.get());

  OptionalValueColumnBuilder<Tuple> builder;
  builder.push_back_opt(tuple1, true);
  builder.push_back_null();
  builder.push_back_elem(RTAny::from_tuple(tuple2));
  auto col =
      std::dynamic_pointer_cast<OptionalValueColumn<Tuple>>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "OptionalValueColumn<tuple>[3]");
  EXPECT_EQ(col->elem_type(), RTAnyType::kTuple);
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_TRUE(col->is_optional());

  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(col->get_elem(0).as_tuple(), tuple1);
  EXPECT_TRUE(col->get_elem(1).is_null());
  EXPECT_EQ(col->get_elem(2).as_tuple(), tuple2);

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    auto opt_col =
        std::dynamic_pointer_cast<OptionalValueColumn<Tuple>>(shuffled);

    ASSERT_NE(opt_col, nullptr);
    EXPECT_FALSE(opt_col->has_value(0));
    EXPECT_TRUE(opt_col->has_value(1));
  }
}

class ListValueColumnTest : public ::testing::Test {};

TEST_F(ListValueColumnTest, ListValueColumnBasic) {
  std::vector<int32_t> list_content1 = {10, 20};
  auto impl1 = ListImpl<int32_t>::make_list_impl(std::move(list_content1));
  List list1(impl1.get());

  std::vector<int32_t> list_content2 = {30};
  auto impl2 = ListImpl<int32_t>::make_list_impl(std::move(list_content2));
  List list2(impl2.get());

  ListValueColumnBuilder builder(RTAnyType::kI32Value);
  builder.push_back_opt(list1);
  builder.push_back_elem(RTAny::from_list(list2));
  auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 2);
  EXPECT_EQ(col->get_value(0).size(), 2);
  EXPECT_EQ(col->get_value(1).size(), 1);

  EXPECT_EQ(col->column_info(), "ListValueColumn[2]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(col->elem_type(), RTAnyType::kList);

  EXPECT_EQ(col->get_elem(0).as_list().size(), 2);
  EXPECT_EQ(col->get_elem(0).as_list().to_string(), "[10, 20]");

  // shuffle
  {
    std::vector<size_t> offsets = {1, 0};
    auto shuffled = col->shuffle(offsets);
    ASSERT_EQ(shuffled->size(), 2);

    auto* list_col = dynamic_cast<ListValueColumn*>(shuffled.get());
    ASSERT_NE(list_col, nullptr);
    EXPECT_EQ(list_col->get_value(0).get(0).as_int32(), 30);
    EXPECT_EQ(list_col->get_value(1).get(0).as_int32(), 10);
  }
}

TEST_F(ValueColumnTest, ListValueColumnUnfold) {
  // unfold i32
  {
    std::vector<int32_t> list_content = {10};
    auto impl1 = ListImpl<int32_t>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<int32_t> list_content2 = {20};
    auto impl2 = ListImpl<int32_t>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kI32Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_int32(), 10);
    EXPECT_EQ(unfurled->get_elem(1).as_int32(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold i64
  {
    std::vector<int64_t> list_content = {10};
    auto impl1 = ListImpl<int64_t>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<int64_t> list_content2 = {20};
    auto impl2 = ListImpl<int64_t>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kI64Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_int64(), 10);
    EXPECT_EQ(unfurled->get_elem(1).as_int64(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold u32
  {
    std::vector<uint32_t> list_content = {10};
    auto impl1 = ListImpl<uint32_t>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<uint32_t> list_content2 = {20};
    auto impl2 = ListImpl<uint32_t>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kU32Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_uint32(), 10);
    EXPECT_EQ(unfurled->get_elem(1).as_uint32(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold u64
  {
    std::vector<uint64_t> list_content = {10};
    auto impl1 = ListImpl<uint64_t>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<uint64_t> list_content2 = {20};
    auto impl2 = ListImpl<uint64_t>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kU64Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_uint64(), 10);
    EXPECT_EQ(unfurled->get_elem(1).as_uint64(), 20);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold f32
  {
    std::vector<float> list_content = {10.42};
    auto impl1 = ListImpl<float>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<float> list_content2 = {20.43};
    auto impl2 = ListImpl<float>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kF32Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_FLOAT_EQ(unfurled->get_elem(0).as_float(), 10.42);
    EXPECT_FLOAT_EQ(unfurled->get_elem(1).as_float(), 20.43);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold f64
  {
    std::vector<double> list_content = {10.42};
    auto impl1 = ListImpl<double>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<double> list_content2 = {20.43};
    auto impl2 = ListImpl<double>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kF64Value);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_DOUBLE_EQ(unfurled->get_elem(0).as_double(), 10.42);
    EXPECT_DOUBLE_EQ(unfurled->get_elem(1).as_double(), 20.43);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold string_view
  {
    std::string s1 = "hello";
    std::string s2 = "world";

    std::vector<std::string_view> list_content = {std::string_view(s1)};
    auto impl1 =
        ListImpl<std::string_view>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<std::string_view> list_content2 = {std::string_view(s2)};
    auto impl2 =
        ListImpl<std::string_view>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kStringValue);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_string(), "hello");
    EXPECT_EQ(unfurled->get_elem(1).as_string(), "world");

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold Date
  {
    std::vector<Date> list_content = {Date(10)};
    auto impl1 = ListImpl<Date>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<Date> list_content2 = {Date(20)};
    auto impl2 = ListImpl<Date>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kDate);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_date(), Date(10));
    EXPECT_EQ(unfurled->get_elem(1).as_date(), Date(20));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold DateTime
  {
    std::vector<DateTime> list_content = {DateTime(10)};
    auto impl1 = ListImpl<DateTime>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<DateTime> list_content2 = {DateTime(20)};
    auto impl2 = ListImpl<DateTime>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kDateTime);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_datetime(), DateTime(10));
    EXPECT_EQ(unfurled->get_elem(1).as_datetime(), DateTime(20));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }

  // unfold Interval
  {
    std::vector<Interval> list_content = {Interval(std::string("10months"))};
    auto impl1 = ListImpl<Interval>::make_list_impl(std::move(list_content));
    List list1(impl1.get());

    std::vector<Interval> list_content2 = {Interval(std::string("20months"))};
    auto impl2 = ListImpl<Interval>::make_list_impl(std::move(list_content2));
    List list2(impl2.get());

    ListValueColumnBuilder builder(RTAnyType::kInterval);
    builder.push_back_opt(list1);
    builder.push_back_opt(list2);
    auto col = std::dynamic_pointer_cast<ListValueColumn>(builder.finish());

    auto [unfurled, offsets] = col->unfold();
    ASSERT_EQ(unfurled->size(), 2);
    EXPECT_EQ(offsets.size(), 2);

    EXPECT_EQ(unfurled->get_elem(0).as_interval(),
              Interval(std::string("10months")));
    EXPECT_EQ(unfurled->get_elem(1).as_interval(),
              Interval(std::string("20months")));

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
  }
}

}  // namespace test
}  // namespace runtime
}  // namespace gs