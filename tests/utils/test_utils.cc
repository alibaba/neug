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

#include <cstdio>
#include <filesystem>

#include <gtest/gtest.h>

#include "neug/execution/common/types/value.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/bitset.h"
#include "neug/utils/encoder.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/string_view_vector.h"
#include "neug/utils/yaml_utils.h"

namespace neug {
namespace test {
class BitsetTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BitsetTest, DefaultConstructor) {
  Bitset bs;
  EXPECT_EQ(bs.size(), 0);
  EXPECT_EQ(bs.count(), 0);
}

TEST_F(BitsetTest, ResizeAndSetGet) {
  Bitset bs;
  bs.resize(100);

  EXPECT_EQ(bs.size(), 100);
  EXPECT_EQ(bs.count(), 0);

  bs.set(0);
  bs.set(63);
  bs.set(64);
  bs.set(99);

  EXPECT_TRUE(bs.get(0));
  EXPECT_TRUE(bs.get(63));
  EXPECT_TRUE(bs.get(64));
  EXPECT_TRUE(bs.get(99));

  EXPECT_FALSE(bs.get(1));
  EXPECT_FALSE(bs.get(50));
  EXPECT_FALSE(bs.get(98));

  EXPECT_EQ(bs.count(), 4);
}

TEST_F(BitsetTest, Reset) {
  Bitset bs;
  bs.resize(200);
  bs.set(10);
  bs.set(150);

  EXPECT_EQ(bs.count(), 2);
  bs.reset(10);
  EXPECT_EQ(bs.count(), 1);
  EXPECT_FALSE(bs.get(10));
  EXPECT_TRUE(bs.get(150));
}

TEST_F(BitsetTest, Clear) {
  Bitset bs;
  bs.resize(100);
  bs.set(50);
  bs.clear();
  EXPECT_EQ(bs.size(), 0);
}

TEST_F(BitsetTest, ResetAll) {
  Bitset bs;
  bs.resize(128);
  bs.set(10);
  bs.set(100);
  EXPECT_EQ(bs.count(), 2);

  bs.reset_all();
  EXPECT_EQ(bs.count(), 0);
  EXPECT_FALSE(bs.get(10));
  EXPECT_FALSE(bs.get(100));
}

TEST_F(BitsetTest, Reserve) {
  Bitset bs;
  bs.reserve(500);
  bs.resize(500);
  bs.set(499);
  EXPECT_TRUE(bs.get(499));
  EXPECT_EQ(bs.size(), 500);
}

TEST_F(BitsetTest, CopyConstructor) {
  Bitset bs1;
  bs1.resize(100);
  bs1.set(25);
  bs1.set(75);

  Bitset bs2(bs1);
  EXPECT_EQ(bs2.size(), 100);
  EXPECT_EQ(bs2.count(), 2);
  EXPECT_TRUE(bs2.get(25));
  EXPECT_TRUE(bs2.get(75));
}

TEST_F(BitsetTest, MoveConstructor) {
  Bitset bs1;
  bs1.resize(100);
  bs1.set(30);

  Bitset bs2(std::move(bs1));
  EXPECT_EQ(bs2.size(), 100);
  EXPECT_TRUE(bs2.get(30));
  EXPECT_EQ(bs1.size(), 0);
}

TEST_F(BitsetTest, CopyAssignment) {
  Bitset bs1, bs2;
  bs1.resize(80);
  bs1.set(10);
  bs1.set(70);

  bs2 = bs1;
  EXPECT_EQ(bs2.size(), 80);
  EXPECT_EQ(bs2.count(), 2);
  EXPECT_TRUE(bs2.get(10));
  EXPECT_TRUE(bs2.get(70));
}

TEST_F(BitsetTest, MoveAssignment) {
  Bitset bs1, bs2;
  bs1.resize(90);
  bs1.set(45);

  bs2 = std::move(bs1);
  EXPECT_EQ(bs2.size(), 90);
  EXPECT_TRUE(bs2.get(45));
  EXPECT_EQ(bs1.size(), 0);
}

TEST_F(BitsetTest, SelfAssignment) {
  Bitset bs;
  bs.resize(50);
  bs.set(20);

  Bitset bs2 = bs;
  EXPECT_EQ(bs2.size(), 50);
  EXPECT_TRUE(bs2.get(20));
}

TEST_F(BitsetTest, AtomicOperations) {
  Bitset bs;
  bs.resize(200);

  bs.atomic_set(100);
  EXPECT_TRUE(bs.get(100));

  bs.atomic_reset(100);
  EXPECT_FALSE(bs.get(100));

  bool was_set = bs.atomic_set_with_ret(150);
  EXPECT_FALSE(was_set);
  was_set = bs.atomic_set_with_ret(150);
  EXPECT_TRUE(was_set);

  bool was_set2 = bs.atomic_reset_with_ret(150);
  EXPECT_TRUE(was_set2);
  was_set2 = bs.atomic_reset_with_ret(150);
  EXPECT_FALSE(was_set2);
}

TEST_F(BitsetTest, Serialization) {
  Bitset original;
  original.resize(200);
  original.set(0);
  original.set(63);
  original.set(64);
  original.set(199);

  std::stringstream ss;
  original.Serialize(ss);

  Bitset restored;
  restored.Deserialize(ss);

  EXPECT_EQ(restored.size(), 200);
  EXPECT_EQ(restored.count(), 4);
  EXPECT_TRUE(restored.get(0));
  EXPECT_TRUE(restored.get(63));
  EXPECT_TRUE(restored.get(64));
  EXPECT_TRUE(restored.get(199));
}

TEST_F(BitsetTest, EmptySerialization) {
  Bitset empty;
  std::stringstream ss;
  empty.Serialize(ss);

  Bitset restored;
  restored.Deserialize(ss);
  EXPECT_EQ(restored.size(), 0);
  EXPECT_EQ(restored.count(), 0);
}

TEST_F(BitsetTest, BoundaryBits) {
  Bitset bs;
  bs.resize(64);

  bs.set(0);
  bs.set(63);
  EXPECT_TRUE(bs.get(0));
  EXPECT_TRUE(bs.get(63));
  EXPECT_EQ(bs.count(), 2);

  bs.resize(65);
  bs.set(64);
  EXPECT_TRUE(bs.get(64));
  EXPECT_EQ(bs.count(), 3);
}

class ArrowUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// =============== LDBCTimeStampParser Tests ===============

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_YYYYMMDD) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(parser("2023-01-01", 10, arrow::TimeUnit::MILLI, &result));

  int64_t expected_ms = 1672531200LL * 1000;
  EXPECT_EQ(result, expected_ms);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_WithTime) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(
      parser("2023-06-15 14:30:45", 19, arrow::TimeUnit::MILLI, &result));

  int64_t expected_ms = 1686839445LL * 1000;
  EXPECT_EQ(result, expected_ms);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_WithTZ_Z) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(
      parser("2023-06-15T14:30:45Z", 20, arrow::TimeUnit::MILLI, &result));

  int64_t expected_ms = 1686839445LL * 1000;
  EXPECT_EQ(result, expected_ms);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_WithTZ_HHMM) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(
      parser("2023-06-15 14:30:45+0800", 24, arrow::TimeUnit::MILLI, &result));

  int64_t expected_utc_seconds = 1686810645LL;
  int64_t expected_ms = expected_utc_seconds * 1000;
  EXPECT_EQ(result, expected_ms);

  EXPECT_TRUE(
      parser("2023-06-15 14:30:45+08:00", 25, arrow::TimeUnit::MILLI, &result));

  expected_utc_seconds = 1686810645LL;
  expected_ms = expected_utc_seconds * 1000;
  EXPECT_EQ(result, expected_ms);

  EXPECT_TRUE(
      parser("2023-06-15 14:30:45+08", 22, arrow::TimeUnit::MILLI, &result));

  expected_utc_seconds = 1686810645LL;
  expected_ms = expected_utc_seconds * 1000;
  EXPECT_EQ(result, expected_ms);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_WithFractionalSeconds) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(
      parser("2023-06-15 14:30:45.123", 23, arrow::TimeUnit::MILLI, &result));

  int64_t base_seconds = 1686839445LL;
  int64_t expected_ms = base_seconds * 1000 + 123;
  EXPECT_EQ(result, expected_ms);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_Microseconds) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_TRUE(parser("2023-06-15 14:30:45.123456", 26, arrow::TimeUnit::MICRO,
                     &result));

  int64_t base_seconds = 1686839445LL;
  int64_t expected_micros = base_seconds * 1000000 + 123456;
  EXPECT_EQ(result, expected_micros);
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_InvalidInput) {
  LDBCTimeStampParser parser;
  int64_t result;

  EXPECT_FALSE(parser("2023", 4, arrow::TimeUnit::MILLI, &result));

  EXPECT_FALSE(parser("2023/01/01", 10, arrow::TimeUnit::MILLI, &result));

  EXPECT_FALSE(
      parser("2023-01-01X12:00:00", 19, arrow::TimeUnit::MILLI, &result));
}

TEST_F(ArrowUtilsTest, LDBCTimeStampParser_function) {
  LDBCTimeStampParser parser;
  EXPECT_STREQ(parser.kind(), "LDBC timestamp parser");
  EXPECT_STREQ(parser.format(), "EmptyFormat");
}

// =============== LDBCLongDateParser Tests ===============

TEST_F(ArrowUtilsTest, LDBCLongDateParser_Valid) {
  LDBCLongDateParser parser;
  int64_t result;

  EXPECT_TRUE(parser("1672531200000", 13, arrow::TimeUnit::MILLI, &result));

  EXPECT_EQ(result, 1672531200000LL);
}

TEST_F(ArrowUtilsTest, LDBCLongDateParser_WithSubseconds) {
  LDBCLongDateParser parser;
  int64_t result;

  EXPECT_TRUE(parser("1672531200123", 13, arrow::TimeUnit::MILLI, &result));

  EXPECT_EQ(result, 1672531200123LL);
}

TEST_F(ArrowUtilsTest, LDBCLongDateParser_Invalid) {
  LDBCLongDateParser parser;
  int64_t result;

  EXPECT_FALSE(parser("abc123def", 9, arrow::TimeUnit::MILLI, &result));
}

TEST_F(ArrowUtilsTest, LDBCLongDateParser_function) {
  LDBCLongDateParser parser;
  EXPECT_STREQ(parser.kind(), "LDBC timestamp parser");
  EXPECT_STREQ(parser.format(), "LongDateFormat");
}

// =============== TypeConverter Tests ===============

TEST_F(ArrowUtilsTest, TypeConverter_Bool) {
  EXPECT_EQ(TypeConverter<bool>::property_type(), DataTypeId::kBoolean);
  auto arrow_type = TypeConverter<bool>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::boolean()));
}

TEST_F(ArrowUtilsTest, TypeConverter_Int32) {
  EXPECT_EQ(TypeConverter<int32_t>::property_type(), DataTypeId::kInt32);
  auto arrow_type = TypeConverter<int32_t>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::int32()));
}

TEST_F(ArrowUtilsTest, TypeConverter_UInt32) {
  EXPECT_EQ(TypeConverter<uint32_t>::property_type(), DataTypeId::kUInt32);
  auto arrow_type = TypeConverter<uint32_t>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::uint32()));
}

TEST_F(ArrowUtilsTest, TypeConverter_Int64) {
  EXPECT_EQ(TypeConverter<int64_t>::property_type(), DataTypeId::kInt64);
  auto arrow_type = TypeConverter<int64_t>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::int64()));
}

TEST_F(ArrowUtilsTest, TypeConverter_UInt64) {
  EXPECT_EQ(TypeConverter<uint64_t>::property_type(), DataTypeId::kUInt64);
  auto arrow_type = TypeConverter<uint64_t>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::uint64()));
}

TEST_F(ArrowUtilsTest, TypeConverter_Double) {
  EXPECT_EQ(TypeConverter<double>::property_type(), DataTypeId::kDouble);
  auto arrow_type = TypeConverter<double>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::float64()));
}

TEST_F(ArrowUtilsTest, TypeConverter_Float) {
  EXPECT_EQ(TypeConverter<float>::property_type(), DataTypeId::kFloat);
  auto arrow_type = TypeConverter<float>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::float32()));
}

TEST_F(ArrowUtilsTest, TypeConverter_String) {
  EXPECT_EQ(TypeConverter<std::string>::property_type(), DataTypeId::kVarchar);
  auto arrow_type = TypeConverter<std::string>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::utf8()));
}

TEST_F(ArrowUtilsTest, TypeConverter_StringView) {
  EXPECT_EQ(TypeConverter<std::string_view>::property_type(),
            DataTypeId::kVarchar);
  auto arrow_type = TypeConverter<std::string_view>::ArrowTypeValue();
  EXPECT_TRUE(arrow_type->Equals(arrow::utf8()));
}

TEST_F(ArrowUtilsTest, TypeConverter_Date) {
  EXPECT_EQ(TypeConverter<Date>::property_type(), DataTypeId::kDate);
  auto arrow_type = TypeConverter<Date>::ArrowTypeValue();
  auto expected = arrow::date64();
  EXPECT_TRUE(arrow_type->Equals(expected));
}

TEST_F(ArrowUtilsTest, TypeConverter_DateTime) {
  EXPECT_EQ(TypeConverter<DateTime>::property_type(), DataTypeId::kTimestampMs);
  auto arrow_type = TypeConverter<DateTime>::ArrowTypeValue();
  auto expected = arrow::timestamp(arrow::TimeUnit::MILLI);
  EXPECT_TRUE(arrow_type->Equals(expected));
}

TEST_F(ArrowUtilsTest, TypeConverter_Interval) {
  EXPECT_EQ(TypeConverter<Interval>::property_type(), DataTypeId::kInterval);
  auto arrow_type = TypeConverter<Interval>::ArrowTypeValue();
  auto expected = arrow::utf8();
  EXPECT_TRUE(arrow_type->Equals(expected));
}

// =============== PropertyTypeToArrowType Function Tests ===============

TEST_F(ArrowUtilsTest, PropertyTypeToArrowType_Function) {
  auto bool_type = PropertyTypeToArrowType(DataTypeId::kBoolean);
  EXPECT_TRUE(bool_type->Equals(arrow::boolean()));

  auto int32_type = PropertyTypeToArrowType(DataTypeId::kInt32);
  EXPECT_TRUE(int32_type->Equals(arrow::int32()));

  auto int64_type = PropertyTypeToArrowType(DataTypeId::kInt64);
  EXPECT_TRUE(int64_type->Equals(arrow::int64()));

  auto uint32_type = PropertyTypeToArrowType(DataTypeId::kUInt32);
  EXPECT_TRUE(uint32_type->Equals(arrow::uint32()));

  auto uint64_type = PropertyTypeToArrowType(DataTypeId::kUInt64);
  EXPECT_TRUE(uint64_type->Equals(arrow::uint64()));

  auto double_type = PropertyTypeToArrowType(DataTypeId::kDouble);
  EXPECT_TRUE(double_type->Equals(arrow::float64()));

  auto float_type = PropertyTypeToArrowType(DataTypeId::kFloat);
  EXPECT_TRUE(float_type->Equals(arrow::float32()));

  auto date_type = PropertyTypeToArrowType(DataTypeId::kDate);
  EXPECT_TRUE(date_type->Equals(arrow::date64()));

  auto string_type = PropertyTypeToArrowType(DataTypeId::kVarchar);
  EXPECT_TRUE(string_type->Equals(arrow::utf8()));
}

// =============== Parser Metadata Tests ===============

TEST_F(ArrowUtilsTest, ParserMetadata) {
  LDBCTimeStampParser ts_parser;
  EXPECT_STREQ(ts_parser.kind(), "LDBC timestamp parser");
  EXPECT_STREQ(ts_parser.format(), "EmptyFormat");

  LDBCLongDateParser long_parser;
  EXPECT_STREQ(long_parser.kind(), "LDBC timestamp parser");
  EXPECT_STREQ(long_parser.format(), "LongDateFormat");
}

class StringViewVectorTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(StringViewVectorTest, DefaultConstructor) {
  StringViewVector vec;
  EXPECT_EQ(vec.size(), 0);
  EXPECT_EQ(vec.content_buffer().size(), 0);
  EXPECT_EQ(vec.offset_buffer().size(), 1);
  EXPECT_EQ(vec.offset_buffer()[0], 0);
}

TEST_F(StringViewVectorTest, PushBackSingleString) {
  StringViewVector vec;
  std::string_view str = "hello";

  vec.push_back(str);

  EXPECT_EQ(vec.size(), 1);
  EXPECT_EQ(vec[0], "hello");
  EXPECT_EQ(vec.content_buffer().size(), 5);
  EXPECT_EQ(vec.offset_buffer().size(), 2);
  EXPECT_EQ(vec.offset_buffer()[0], 0);
  EXPECT_EQ(vec.offset_buffer()[1], 5);
}

TEST_F(StringViewVectorTest, PushBackMultipleStrings) {
  StringViewVector vec;

  vec.push_back("apple");
  vec.push_back("banana");
  vec.push_back("cherry");

  EXPECT_EQ(vec.size(), 3);
  EXPECT_EQ(vec[0], "apple");
  EXPECT_EQ(vec[1], "banana");
  EXPECT_EQ(vec[2], "cherry");

  std::string expected_buffer = "applebananacherry";
  EXPECT_EQ(
      std::string(vec.content_buffer().data(), vec.content_buffer().size()),
      expected_buffer);

  EXPECT_EQ(vec.offset_buffer()[0], 0);
  EXPECT_EQ(vec.offset_buffer()[1], 5);
  EXPECT_EQ(vec.offset_buffer()[2], 11);
  EXPECT_EQ(vec.offset_buffer()[3], 17);
}

TEST_F(StringViewVectorTest, EmplaceBack) {
  StringViewVector vec;

  vec.emplace_back("test1");
  vec.emplace_back("test2");

  EXPECT_EQ(vec.size(), 2);
  EXPECT_EQ(vec[0], "test1");
  EXPECT_EQ(vec[1], "test2");
}

TEST_F(StringViewVectorTest, EmptyString) {
  StringViewVector vec;

  vec.push_back("");
  vec.push_back("non-empty");
  vec.push_back("");

  EXPECT_EQ(vec.size(), 3);
  EXPECT_EQ(vec[0], "");
  EXPECT_EQ(vec[1], "non-empty");
  EXPECT_EQ(vec[2], "");

  EXPECT_EQ(vec.content_buffer().size(), 9);
  EXPECT_EQ(vec.offset_buffer()[0], 0);
  EXPECT_EQ(vec.offset_buffer()[1], 0);
  EXPECT_EQ(vec.offset_buffer()[2], 9);
  EXPECT_EQ(vec.offset_buffer()[3], 9);
}

TEST_F(StringViewVectorTest, VeryLongString) {
  StringViewVector vec;
  std::string long_str(10000, 'x');

  vec.push_back(long_str);

  EXPECT_EQ(vec.size(), 1);
  EXPECT_EQ(vec[0].size(), 10000);
  EXPECT_EQ(std::string(vec[0]), long_str);
}

TEST_F(StringViewVectorTest, OperatorBracketOutOfBounds) {
  StringViewVector vec;
  vec.push_back("test");

  EXPECT_EQ(vec[0], "test");
}

TEST_F(StringViewVectorTest, Clear) {
  StringViewVector vec;
  vec.push_back("first");
  vec.push_back("second");

  EXPECT_EQ(vec.size(), 2);

  vec.clear();

  EXPECT_EQ(vec.size(), 0);
  EXPECT_EQ(vec.content_buffer().size(), 0);
  EXPECT_EQ(vec.offset_buffer().size(), 1);
  EXPECT_EQ(vec.offset_buffer()[0], 0);

  vec.push_back("after_clear");
  EXPECT_EQ(vec.size(), 1);
  EXPECT_EQ(vec[0], "after_clear");
}

TEST_F(StringViewVectorTest, Swap) {
  StringViewVector vec1, vec2;

  vec1.push_back("vec1_item1");
  vec1.push_back("vec1_item2");

  vec2.push_back("vec2_item");

  auto vec1_content = vec1.content_buffer();
  auto vec1_offsets = vec1.offset_buffer();
  auto vec2_content = vec2.content_buffer();
  auto vec2_offsets = vec2.offset_buffer();

  vec1.swap(vec2);

  EXPECT_EQ(vec1.content_buffer(), vec2_content);
  EXPECT_EQ(vec1.offset_buffer(), vec2_offsets);
  EXPECT_EQ(vec2.content_buffer(), vec1_content);
  EXPECT_EQ(vec2.offset_buffer(), vec1_offsets);

  EXPECT_EQ(vec1.size(), 1);
  EXPECT_EQ(vec1[0], "vec2_item");
  EXPECT_EQ(vec2.size(), 2);
  EXPECT_EQ(vec2[0], "vec1_item1");
  EXPECT_EQ(vec2[1], "vec1_item2");
}

TEST_F(StringViewVectorTest, ContentAndOffsetBuffers) {
  StringViewVector vec;
  vec.push_back("buffer");
  vec.push_back("test");

  const auto& const_vec = vec;
  EXPECT_EQ(const_vec.content_buffer().size(), 10);
  EXPECT_EQ(const_vec.offset_buffer().size(), 3);

  auto& content_buf = vec.content_buffer();
  auto& offset_buf = vec.offset_buffer();

  content_buf.push_back('X');
  offset_buf.push_back(11);

  EXPECT_EQ(vec.content_buffer().size(), 11);
  EXPECT_EQ(vec.offset_buffer().size(), 4);
}

TEST_F(StringViewVectorTest, MixedPushAndEmplace) {
  StringViewVector vec;

  vec.push_back("push1");
  vec.emplace_back("emplace1");
  vec.push_back("push2");
  vec.emplace_back("emplace2");

  EXPECT_EQ(vec.size(), 4);
  EXPECT_EQ(vec[0], "push1");
  EXPECT_EQ(vec[1], "emplace1");
  EXPECT_EQ(vec[2], "push2");
  EXPECT_EQ(vec[3], "emplace2");
}

TEST_F(StringViewVectorTest, ZeroSizeAfterConstruction) {
  StringViewVector vec;
  EXPECT_EQ(vec.size(), 0);

  vec.clear();
  vec.clear();
  EXPECT_EQ(vec.size(), 0);
  EXPECT_EQ(vec.offset_buffer().size(), 1);
  EXPECT_EQ(vec.offset_buffer()[0], 0);
}

TEST_F(StringViewVectorTest, DifferentStringTypes) {
  StringViewVector vec;

  vec.push_back("literal");

  std::string str = "from_std_string";
  vec.push_back(str);

  const char* cstr = "from_cstring";
  vec.push_back(std::string_view(cstr));

  EXPECT_EQ(vec.size(), 3);
  EXPECT_EQ(vec[0], "literal");
  EXPECT_EQ(vec[1], "from_std_string");
  EXPECT_EQ(vec[2], "from_cstring");
}

class AppUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// =============== Encoder Tests ===============

TEST_F(AppUtilsTest, EncodeDecode_Long) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  int64_t original = 123456789012345LL;
  encoder.put_long(original);

  Decoder decoder(buffer.data(), buffer.size());
  int64_t decoded = decoder.get_long();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_Int) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  int original = -123456789;
  encoder.put_int(original);

  Decoder decoder(buffer.data(), buffer.size());
  int decoded = decoder.get_int();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_UInt) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  uint32_t original = 4294967295U;
  encoder.put_uint(original);

  Decoder decoder(buffer.data(), buffer.size());
  uint32_t decoded = decoder.get_uint();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_Byte) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  uint8_t original = 255;
  encoder.put_byte(original);

  Decoder decoder(buffer.data(), buffer.size());
  uint8_t decoded = decoder.get_byte();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_Float) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  float original = 3.14159f;
  encoder.put_float(original);

  Decoder decoder(buffer.data(), buffer.size());
  float decoded = decoder.get_float();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_Double) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  double original = 2.718281828459045;
  encoder.put_double(original);

  Decoder decoder(buffer.data(), buffer.size());
  double decoded = decoder.get_double();

  EXPECT_EQ(original, decoded);
}

// =============== String Encoding Tests ===============

TEST_F(AppUtilsTest, EncodeDecode_String) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string original = "Hello, World! This is a test string with unicode: 🚀";
  encoder.put_string(original);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_string();

  EXPECT_EQ(original, decoded);
  EXPECT_EQ(decoded.size(), original.size());
}

TEST_F(AppUtilsTest, EncodeDecode_StringView) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string_view original = "StringView test";
  encoder.put_string_view(original);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_string();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_SmallString) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string original = "small";
  encoder.put_small_string(original);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_small_string();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_SmallStringView) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string_view original = "small view";
  encoder.put_small_string_view(original);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_small_string();

  EXPECT_EQ(original, decoded);
}

TEST_F(AppUtilsTest, EncodeDecode_EmptyString) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string empty = "";
  encoder.put_string(empty);
  encoder.put_small_string(empty);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded1 = decoder.get_string();
  std::string_view decoded2 = decoder.get_small_string();

  EXPECT_TRUE(decoded1.empty());
  EXPECT_TRUE(decoded2.empty());
}

// =============== Bytes Tests ===============

TEST_F(AppUtilsTest, EncodeDecode_Bytes) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  const char* data = "binary\0data\1with\2nulls";
  size_t data_size = 20;

  encoder.put_bytes(data, data_size);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_bytes();

  EXPECT_EQ(decoded.size(), data_size);
  EXPECT_EQ(memcmp(decoded.data(), data, data_size), 0);
}

// =============== Skip and PutAt Tests ===============

TEST_F(AppUtilsTest, SkipAndPutAt_Long) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  size_t pos = encoder.skip_long();

  encoder.put_int(42);

  encoder.put_long_at(pos, 9876543210LL);

  Decoder decoder(buffer.data(), buffer.size());
  int64_t decoded_long = decoder.get_long();
  int decoded_int = decoder.get_int();

  EXPECT_EQ(decoded_long, 9876543210LL);
  EXPECT_EQ(decoded_int, 42);
}

TEST_F(AppUtilsTest, SkipAndPutAt_Int) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  size_t pos = encoder.skip_int();
  encoder.put_byte(255);
  encoder.put_int_at(pos, -12345);

  Decoder decoder(buffer.data(), buffer.size());
  int decoded_int = decoder.get_int();
  uint8_t decoded_byte = decoder.get_byte();

  EXPECT_EQ(decoded_int, -12345);
  EXPECT_EQ(decoded_byte, 255);
}

TEST_F(AppUtilsTest, SkipAndPutAt_Byte) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  size_t pos = encoder.skip_byte();
  encoder.put_long(123456789LL);
  encoder.put_byte_at(pos, 128);

  Decoder decoder(buffer.data(), buffer.size());
  uint8_t decoded_byte = decoder.get_byte();
  int64_t decoded_long = decoder.get_long();

  EXPECT_EQ(decoded_byte, 128);
  EXPECT_EQ(decoded_long, 123456789LL);
}

// =============== Clear Test ===============

TEST_F(AppUtilsTest, EncoderClear) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  encoder.put_int(42);
  encoder.put_string("test");

  EXPECT_FALSE(buffer.empty());

  encoder.clear();

  EXPECT_TRUE(buffer.empty());
}

// =============== Decoder State Tests ===============

TEST_F(AppUtilsTest, DecoderStateQueries) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  encoder.put_int(1);
  encoder.put_int(2);

  Decoder decoder(buffer.data(), buffer.size());

  EXPECT_FALSE(decoder.empty());
  EXPECT_EQ(decoder.size(), 8);

  decoder.get_int();
  EXPECT_FALSE(decoder.empty());
  EXPECT_EQ(decoder.size(), 4);

  decoder.get_int();
  EXPECT_TRUE(decoder.empty());
  EXPECT_EQ(decoder.size(), 0);
}

TEST_F(AppUtilsTest, DecoderReset) {
  std::vector<char> buffer1, buffer2;

  Encoder enc1(buffer1);
  enc1.put_int(100);

  Encoder enc2(buffer2);
  enc2.put_int(200);

  Decoder decoder(buffer1.data(), buffer1.size());
  EXPECT_EQ(decoder.get_int(), 100);

  decoder.reset(buffer2.data(), buffer2.size());
  EXPECT_EQ(decoder.get_int(), 200);
}

// =============== Mixed Data Types ===============

TEST_F(AppUtilsTest, MixedDataTypes) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  encoder.put_byte(255);
  encoder.put_int(-123456);
  encoder.put_long(987654321012345LL);
  encoder.put_float(3.14159f);
  encoder.put_double(2.718281828459045);
  encoder.put_string("mixed string");
  encoder.put_small_string("small");

  Decoder decoder(buffer.data(), buffer.size());

  EXPECT_EQ(decoder.get_byte(), 255);
  EXPECT_EQ(decoder.get_int(), -123456);
  EXPECT_EQ(decoder.get_long(), 987654321012345LL);
  EXPECT_EQ(decoder.get_float(), 3.14159f);
  EXPECT_EQ(decoder.get_double(), 2.718281828459045);
  EXPECT_EQ(decoder.get_string(), "mixed string");
  EXPECT_EQ(decoder.get_small_string(), "small");
}

// =============== Boundary Conditions ===============

TEST_F(AppUtilsTest, LargeSmallString) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  std::string large_small(255, 'x');
  encoder.put_small_string(large_small);

  Decoder decoder(buffer.data(), buffer.size());
  std::string_view decoded = decoder.get_small_string();

  EXPECT_EQ(decoded.size(), 255);
  EXPECT_EQ(std::string(decoded), large_small);
}

TEST_F(AppUtilsTest, ZeroValues) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  encoder.put_byte(0);
  encoder.put_int(0);
  encoder.put_uint(0);
  encoder.put_long(0);
  encoder.put_float(0.0f);
  encoder.put_double(0.0);
  encoder.put_string("");
  encoder.put_small_string("");

  Decoder decoder(buffer.data(), buffer.size());

  EXPECT_EQ(decoder.get_byte(), 0);
  EXPECT_EQ(decoder.get_int(), 0);
  EXPECT_EQ(decoder.get_uint(), 0U);
  EXPECT_EQ(decoder.get_long(), 0LL);
  EXPECT_EQ(decoder.get_float(), 0.0f);
  EXPECT_EQ(decoder.get_double(), 0.0);
  EXPECT_TRUE(decoder.get_string().empty());
  EXPECT_TRUE(decoder.get_small_string().empty());
}

// =============== Endianness Consistency ===============

TEST_F(AppUtilsTest, EndiannessConsistency) {
  std::vector<char> buffer;
  Encoder encoder(buffer);

  uint32_t test_value = 0x12345678;
  encoder.put_uint(test_value);

  ASSERT_EQ(buffer.size(), 4);
  uint32_t reconstructed;
  memcpy(&reconstructed, buffer.data(), 4);

  Decoder decoder(buffer.data(), buffer.size());
  uint32_t decoded = decoder.get_uint();

  EXPECT_EQ(decoded, test_value);
  EXPECT_EQ(reconstructed, test_value);
}

// =============== protobuf utils ===============
class PBUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PBUtilsTest, MultiplicityToStorageStrategy) {
  EdgeStrategy oe, ie;

  EXPECT_TRUE(multiplicity_to_storage_strategy(
      physical::CreateEdgeSchema::ONE_TO_ONE, oe, ie));
  EXPECT_EQ(oe, EdgeStrategy::kSingle);
  EXPECT_EQ(ie, EdgeStrategy::kSingle);

  EXPECT_TRUE(multiplicity_to_storage_strategy(
      physical::CreateEdgeSchema::ONE_TO_MANY, oe, ie));
  EXPECT_EQ(oe, EdgeStrategy::kMultiple);
  EXPECT_EQ(ie, EdgeStrategy::kSingle);

  EXPECT_TRUE(multiplicity_to_storage_strategy(
      physical::CreateEdgeSchema::MANY_TO_ONE, oe, ie));
  EXPECT_EQ(oe, EdgeStrategy::kSingle);
  EXPECT_EQ(ie, EdgeStrategy::kMultiple);

  EXPECT_TRUE(multiplicity_to_storage_strategy(
      physical::CreateEdgeSchema::MANY_TO_MANY, oe, ie));
  EXPECT_EQ(oe, EdgeStrategy::kMultiple);
  EXPECT_EQ(ie, EdgeStrategy::kMultiple);

  EXPECT_FALSE(multiplicity_to_storage_strategy(
      static_cast<physical::CreateEdgeSchema::Multiplicity>(999), oe, ie));
}

TEST_F(PBUtilsTest, ConflictActionToBool) {
  EXPECT_TRUE(
      conflict_action_to_bool(::physical::ConflictAction::ON_CONFLICT_THROW));
  EXPECT_FALSE(conflict_action_to_bool(
      ::physical::ConflictAction::ON_CONFLICT_DO_NOTHING));
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_Valid) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;

  auto* prop1 = props.Add();
  prop1->set_name("age");
  prop1->mutable_type()->set_primitive_type(
      ::common::PrimitiveType::DT_SIGNED_INT32);
  prop1->mutable_default_value()->set_i32(18);

  auto* prop2 = props.Add();
  prop2->set_name("name");
  prop2->mutable_type()->mutable_string()->mutable_var_char();

  auto result = property_defs_to_value(props);
  auto& tuples = result.value();
  ASSERT_EQ(tuples.size(), 2U);

  EXPECT_EQ(tuples[0].second.type().id(), DataTypeId::kInt32);
  EXPECT_EQ(tuples[0].first, "age");
  EXPECT_EQ(tuples[0].second.GetValue<int32_t>(), 18);

  EXPECT_EQ(tuples[1].second.type().id(), DataTypeId::kVarchar);
  EXPECT_EQ(tuples[1].first, "name");
  EXPECT_EQ(tuples[1].second.GetValue<std::string>(), "");
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_NoDefaultValue) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;

  // INT64 with no default value → should use type default (0)
  auto* prop = props.Add();
  prop->set_name("score");
  prop->mutable_type()->set_primitive_type(
      ::common::PrimitiveType::DT_SIGNED_INT64);

  auto result = property_defs_to_value(props);
  ASSERT_TRUE(result.has_value());
  auto& tuples = result.value();
  ASSERT_EQ(tuples.size(), 1U);
  EXPECT_EQ(tuples[0].first, "score");
  EXPECT_EQ(tuples[0].second.type().id(), DataTypeId::kInt64);
  EXPECT_EQ(tuples[0].second.GetValue<int64_t>(), 0LL);
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_AllPrimitiveTypes) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;

  // BOOL
  {
    auto* p = props.Add();
    p->set_name("flag");
    p->mutable_type()->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    p->mutable_default_value()->set_boolean(true);
  }
  // INT64
  {
    auto* p = props.Add();
    p->set_name("big_id");
    p->mutable_type()->set_primitive_type(
        ::common::PrimitiveType::DT_SIGNED_INT64);
    p->mutable_default_value()->set_i64(9876543210LL);
  }
  // UINT32
  {
    auto* p = props.Add();
    p->set_name("count");
    p->mutable_type()->set_primitive_type(
        ::common::PrimitiveType::DT_UNSIGNED_INT32);
    p->mutable_default_value()->set_u32(100U);
  }
  // UINT64
  {
    auto* p = props.Add();
    p->set_name("big_count");
    p->mutable_type()->set_primitive_type(
        ::common::PrimitiveType::DT_UNSIGNED_INT64);
    p->mutable_default_value()->set_u64(123456789012345ULL);
  }
  // FLOAT
  {
    auto* p = props.Add();
    p->set_name("weight");
    p->mutable_type()->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    p->mutable_default_value()->set_f32(1.5f);
  }
  // DOUBLE
  {
    auto* p = props.Add();
    p->set_name("ratio");
    p->mutable_type()->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    p->mutable_default_value()->set_f64(3.14);
  }

  auto result = property_defs_to_value(props);
  ASSERT_TRUE(result.has_value());
  auto& tuples = result.value();
  ASSERT_EQ(tuples.size(), 6U);

  EXPECT_EQ(tuples[0].second.type().id(), DataTypeId::kBoolean);
  EXPECT_EQ(tuples[0].second.GetValue<bool>(), true);

  EXPECT_EQ(tuples[1].second.type().id(), DataTypeId::kInt64);
  EXPECT_EQ(tuples[1].second.GetValue<int64_t>(), 9876543210LL);

  EXPECT_EQ(tuples[2].second.type().id(), DataTypeId::kUInt32);
  EXPECT_EQ(tuples[2].second.GetValue<uint32_t>(), 100U);

  EXPECT_EQ(tuples[3].second.type().id(), DataTypeId::kUInt64);
  EXPECT_EQ(tuples[3].second.GetValue<uint64_t>(), 123456789012345ULL);

  EXPECT_EQ(tuples[4].second.type().id(), DataTypeId::kFloat);
  EXPECT_FLOAT_EQ(tuples[4].second.GetValue<float>(), 1.5f);

  EXPECT_EQ(tuples[5].second.type().id(), DataTypeId::kDouble);
  EXPECT_DOUBLE_EQ(tuples[5].second.GetValue<double>(), 3.14);
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_StringTypes) {
  // VarChar with default max_length
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("tag");
    p->mutable_type()->mutable_string()->mutable_var_char();
    p->mutable_default_value()->set_str("hello");

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    auto& tuples = result.value();
    ASSERT_EQ(tuples.size(), 1U);
    EXPECT_EQ(tuples[0].second.type().id(), DataTypeId::kVarchar);
    EXPECT_EQ(tuples[0].second.GetValue<std::string>(), "hello");
  }

  // VarChar with explicit max_length
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("short_tag");
    p->mutable_type()->mutable_string()->mutable_var_char()->set_max_length(64);

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()[0].second.type().id(), DataTypeId::kVarchar);
  }

  // LongText
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("description");
    p->mutable_type()->mutable_string()->mutable_long_text();

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()[0].second.type().id(), DataTypeId::kVarchar);
  }
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_TemporalTypes) {
  // Date type
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("birthday");
    p->mutable_type()->mutable_temporal()->mutable_date32();

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()[0].second.type().id(), DataTypeId::kDate);
  }

  // DateTime type
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("created_at");
    p->mutable_type()->mutable_temporal()->mutable_date_time();

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()[0].second.type().id(), DataTypeId::kTimestampMs);
  }

  // Interval type
  {
    google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
    auto* p = props.Add();
    p->set_name("duration");
    p->mutable_type()->mutable_temporal()->mutable_interval();

    auto result = property_defs_to_value(props);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()[0].second.type().id(), DataTypeId::kInterval);
  }
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_InvalidType_DT_ANY) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
  auto* p = props.Add();
  p->set_name("bad");
  p->mutable_type()->set_primitive_type(::common::PrimitiveType::DT_ANY);

  auto result = property_defs_to_value(props);
  EXPECT_FALSE(result.has_value());
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_InvalidType_Decimal) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
  auto* p = props.Add();
  p->set_name("price");
  p->mutable_type()->mutable_decimal();

  auto result = property_defs_to_value(props);
  EXPECT_FALSE(result.has_value());
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_InvalidType_Array) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
  auto* p = props.Add();
  p->set_name("arr");
  p->mutable_type()->mutable_array();

  auto result = property_defs_to_value(props);
  EXPECT_FALSE(result.has_value());
}

TEST_F(PBUtilsTest, PropertyDefsToTuple_EmptyProps) {
  google::protobuf::RepeatedPtrField<::physical::PropertyDef> props;
  auto result = property_defs_to_value(props);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 0U);
}

TEST_F(PBUtilsTest, ParseResultSchemaColumnNames_Empty) {
  auto names = parse_result_schema_column_names("");
  EXPECT_TRUE(names.empty());
}

TEST_F(PBUtilsTest, ParseResultSchemaColumnNames_ValidYaml) {
  std::string schema = R"(
returns:
  - name: id
  - name: name
  - name: age
)";
  auto names = parse_result_schema_column_names(schema);
  ASSERT_EQ(names.size(), 3U);
  EXPECT_EQ(names[0], "id");
  EXPECT_EQ(names[1], "name");
  EXPECT_EQ(names[2], "age");
}

TEST_F(PBUtilsTest, ParseResultSchemaColumnNames_NoReturns) {
  std::string schema = R"(
graph: modern
)";
  auto names = parse_result_schema_column_names(schema);
  EXPECT_TRUE(names.empty());
}

TEST_F(PBUtilsTest, ParseResultSchemaColumnNames_InvalidYaml) {
  std::string schema = "{{invalid: yaml: [}}}";
  auto names = parse_result_schema_column_names(schema);
  EXPECT_TRUE(names.empty());
}

TEST_F(PBUtilsTest, ParseResultSchemaColumnNames_SingleColumn) {
  std::string schema = R"(
returns:
  - name: result
)";
  auto names = parse_result_schema_column_names(schema);
  ASSERT_EQ(names.size(), 1U);
  EXPECT_EQ(names[0], "result");
}

TEST_F(PBUtilsTest, ProtoToString_BasicProto) {
  ::physical::CreateEdgeSchema schema;
  // Add a TypeInfo with multiplicity
  auto* type_info = schema.add_type_info();
  type_info->set_multiplicity(::physical::CreateEdgeSchema::MANY_TO_MANY);

  std::string json = proto_to_string(schema);
  EXPECT_FALSE(json.empty());
  // Should contain whitespace (pretty-printed)
  EXPECT_NE(json.find('\n'), std::string::npos);
  // Should contain the multiplicity field
  EXPECT_NE(json.find("MANY_TO_MANY"), std::string::npos);
}

TEST_F(PBUtilsTest, ProtoToString_EmptyProto) {
  ::physical::CreateEdgeSchema schema;
  std::string json = proto_to_string(schema);
  EXPECT_FALSE(json.empty());
  // Empty proto should still produce valid JSON
  EXPECT_NE(json.find('{'), std::string::npos);
  EXPECT_NE(json.find('}'), std::string::npos);
}

// ================================================================
// YamlUtilsTest
// ================================================================
class YamlUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tmp_dir_ = std::filesystem::temp_directory_path() / "yaml_utils_test";
    std::filesystem::create_directories(tmp_dir_);
  }
  void TearDown() override { std::filesystem::remove_all(tmp_dir_); }
  std::filesystem::path tmp_dir_;
};

// ----------------------------------------------------------------
// property_type_to_yaml
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, PropertyTypeToYaml_PrimitiveTypes) {
  struct Case {
    DataType type;
    std::string expected_str;
  };
  std::vector<Case> cases = {
      {DataType(DataType::BOOLEAN), DT_BOOL},
      {DataType(DataType::INT32), DT_SIGNED_INT32},
      {DataType(DataType::UINT32), DT_UNSIGNED_INT32},
      {DataType(DataType::INT64), DT_SIGNED_INT64},
      {DataType(DataType::UINT64), DT_UNSIGNED_INT64},
      {DataType(DataType::FLOAT), DT_FLOAT},
      {DataType(DataType::DOUBLE), DT_DOUBLE},
  };
  for (const auto& c : cases) {
    YAML::Node node = property_type_to_yaml(c.type);
    ASSERT_TRUE(node["primitive_type"])
        << "Missing primitive_type for: " << c.expected_str;
    EXPECT_EQ(node["primitive_type"].as<std::string>(), c.expected_str);
  }
}

TEST_F(YamlUtilsTest, PropertyTypeToYaml_Varchar_DefaultLength) {
  DataType type = DataType::Varchar(STRING_DEFAULT_MAX_LENGTH);
  YAML::Node node = property_type_to_yaml(type);
  ASSERT_TRUE(node["string"]["var_char"]["max_length"]);
  EXPECT_EQ(node["string"]["var_char"]["max_length"].as<int>(),
            STRING_DEFAULT_MAX_LENGTH);
}

TEST_F(YamlUtilsTest, PropertyTypeToYaml_Varchar_CustomLength) {
  DataType type = DataType::Varchar(128);
  YAML::Node node = property_type_to_yaml(type);
  ASSERT_TRUE(node["string"]["var_char"]["max_length"]);
  EXPECT_EQ(node["string"]["var_char"]["max_length"].as<int>(), 128);
}

TEST_F(YamlUtilsTest, PropertyTypeToYaml_TemporalTypes) {
  // Date
  {
    DataType type(DataType::DATE);
    YAML::Node node = property_type_to_yaml(type);
    EXPECT_TRUE(node["temporal"]);
  }
  // TimestampMs
  {
    DataType type(DataType::TIMESTAMP_MS);
    YAML::Node node = property_type_to_yaml(type);
    EXPECT_TRUE(node["temporal"]);
  }
  // Interval
  {
    DataType type(DataType::INTERVAL);
    YAML::Node node = property_type_to_yaml(type);
    EXPECT_TRUE(node["temporal"]);
  }
}

TEST_F(YamlUtilsTest, PropertyTypeToYaml_UnknownType_Throws) {
  DataType type(DataType::SQLNULL);
  EXPECT_THROW(property_type_to_yaml(type), std::exception);
}

// ----------------------------------------------------------------
// get_yaml_files
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, GetYamlFiles_NonExistentDir) {
  auto files = get_yaml_files("/nonexistent/path/abc123");
  EXPECT_TRUE(files.empty());
}

TEST_F(YamlUtilsTest, GetYamlFiles_EmptyDir) {
  auto files = get_yaml_files(tmp_dir_.string());
  EXPECT_TRUE(files.empty());
}

TEST_F(YamlUtilsTest, GetYamlFiles_MixedFiles) {
  // Create some test files using POSIX API to avoid PCH fstream issues
  auto create_file = [](const std::filesystem::path& p) {
    FILE* f = fopen(p.string().c_str(), "w");
    if (f)
      fclose(f);
  };
  create_file(tmp_dir_ / "a.yaml");
  create_file(tmp_dir_ / "b.yml");
  create_file(tmp_dir_ / "c.json");
  create_file(tmp_dir_ / "d.txt");

  auto files = get_yaml_files(tmp_dir_.string());
  EXPECT_EQ(files.size(), 2U);
  // Both .yaml and .yml should be collected
  bool has_yaml = false, has_yml = false;
  for (const auto& f : files) {
    if (f.find(".yaml") != std::string::npos)
      has_yaml = true;
    if (f.find(".yml") != std::string::npos)
      has_yml = true;
  }
  EXPECT_TRUE(has_yaml);
  EXPECT_TRUE(has_yml);
}

// ----------------------------------------------------------------
// get_json_string_from_yaml(YAML::Node)
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, GetJsonFromYaml_NullNode) {
  YAML::Node node;
  auto result = get_json_string_from_yaml(node);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "{}");
}

TEST_F(YamlUtilsTest, GetJsonFromYaml_MapNode) {
  YAML::Node node;
  node["name"] = "Alice";
  node["age"] = 30;
  node["active"] = true;

  auto result = get_json_string_from_yaml(node);
  ASSERT_TRUE(result.has_value());
  const auto& json = result.value();
  EXPECT_NE(json.find("\"name\""), std::string::npos);
  EXPECT_NE(json.find("Alice"), std::string::npos);
  EXPECT_NE(json.find("30"), std::string::npos);
}

TEST_F(YamlUtilsTest, GetJsonFromYaml_SequenceNode) {
  YAML::Node node;
  node.push_back(1);
  node.push_back(2);
  node.push_back(3);

  auto result = get_json_string_from_yaml(node);
  ASSERT_TRUE(result.has_value());
  const auto& json = result.value();
  // Should be a JSON array
  EXPECT_NE(json.find('['), std::string::npos);
  EXPECT_NE(json.find("1"), std::string::npos);
  EXPECT_NE(json.find("3"), std::string::npos);
}

TEST_F(YamlUtilsTest, GetJsonFromYaml_NestedMap) {
  YAML::Node node = YAML::Load(R"(
graph:
  name: modern
  vertices:
    - person
    - software
)");
  auto result = get_json_string_from_yaml(node);
  ASSERT_TRUE(result.has_value());
  const auto& json = result.value();
  EXPECT_NE(json.find("graph"), std::string::npos);
  EXPECT_NE(json.find("modern"), std::string::npos);
  EXPECT_NE(json.find("person"), std::string::npos);
}

// ----------------------------------------------------------------
// get_json_string_from_yaml(file_path)
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, GetJsonFromYaml_FileNotFound) {
  auto result = get_json_string_from_yaml("/nonexistent/file.yaml");
  EXPECT_FALSE(result.has_value());
}

TEST_F(YamlUtilsTest, GetJsonFromYaml_ValidFile) {
  auto yaml_file = tmp_dir_ / "test.yaml";
  // Write using POSIX API
  FILE* f = fopen(yaml_file.string().c_str(), "w");
  ASSERT_NE(f, nullptr);
  fputs("key: value\n", f);
  fclose(f);

  auto result = get_json_string_from_yaml(yaml_file.string());
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value().find("key"), std::string::npos);
  EXPECT_NE(result.value().find("value"), std::string::npos);
}

// ----------------------------------------------------------------
// get_yaml_string_from_yaml_node
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, GetYamlStringFromNode_NullNode) {
  YAML::Node node;  // Null node
  auto result = get_yaml_string_from_yaml_node(node);
  ASSERT_TRUE(result.has_value());
  // Null YAML emits "~" or "null"
  const auto& yaml_str = result.value();
  EXPECT_FALSE(yaml_str.empty());
}

TEST_F(YamlUtilsTest, GetYamlStringFromNode_ScalarNode) {
  YAML::Node node = YAML::Load("hello");
  auto result = get_yaml_string_from_yaml_node(node);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value().find("hello"), std::string::npos);
}

TEST_F(YamlUtilsTest, GetYamlStringFromNode_MapNode) {
  YAML::Node node;
  node["key1"] = "val1";
  node["key2"] = 42;
  auto result = get_yaml_string_from_yaml_node(node);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value().find("key1"), std::string::npos);
  EXPECT_NE(result.value().find("val1"), std::string::npos);
  EXPECT_NE(result.value().find("42"), std::string::npos);
}

TEST_F(YamlUtilsTest, GetYamlStringFromNode_SequenceNode) {
  YAML::Node node;
  node.push_back("a");
  node.push_back("b");
  node.push_back("c");
  auto result = get_yaml_string_from_yaml_node(node);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value().find('a'), std::string::npos);
  EXPECT_NE(result.value().find('c'), std::string::npos);
}

// ----------------------------------------------------------------
// write_yaml_file
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, WriteYamlFile_Success) {
  YAML::Node node;
  node["greeting"] = "hello";
  node["count"] = 5;

  auto out_path = tmp_dir_ / "out.yaml";
  EXPECT_TRUE(write_yaml_file(node, out_path.string()));
  EXPECT_TRUE(std::filesystem::exists(out_path));

  // Read back and verify
  YAML::Node loaded = YAML::LoadFile(out_path.string());
  EXPECT_EQ(loaded["greeting"].as<std::string>(), "hello");
  EXPECT_EQ(loaded["count"].as<int>(), 5);
}

TEST_F(YamlUtilsTest, WriteYamlFile_InvalidPath) {
  YAML::Node node;
  node["x"] = 1;
  EXPECT_FALSE(write_yaml_file(node, "/nonexistent_dir/abc/out.yaml"));
}

// ----------------------------------------------------------------
// config_parsing::get_scalar
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, ConfigParsing_GetScalar_Found) {
  YAML::Node node = YAML::Load("name: Alice\nage: 30");
  std::string name;
  EXPECT_TRUE(config_parsing::get_scalar(node, "name", name));
  EXPECT_EQ(name, "Alice");

  int age = 0;
  EXPECT_TRUE(config_parsing::get_scalar(node, "age", age));
  EXPECT_EQ(age, 30);
}

TEST_F(YamlUtilsTest, ConfigParsing_GetScalar_NotFound) {
  YAML::Node node = YAML::Load("name: Alice");
  std::string val;
  EXPECT_FALSE(config_parsing::get_scalar(node, "missing_key", val));
}

TEST_F(YamlUtilsTest, ConfigParsing_GetScalar_NotScalar) {
  YAML::Node node = YAML::Load("items:\n  - a\n  - b");
  std::string val;
  // "items" exists but is a sequence, not a scalar
  EXPECT_FALSE(config_parsing::get_scalar(node, "items", val));
}

// ----------------------------------------------------------------
// config_parsing::get_sequence
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, ConfigParsing_GetSequence_Found) {
  YAML::Node node = YAML::Load("tags:\n  - cpp\n  - python\n  - rust");
  std::vector<std::string> tags;
  EXPECT_TRUE(config_parsing::get_sequence(node, "tags", tags));
  ASSERT_EQ(tags.size(), 3U);
  EXPECT_EQ(tags[0], "cpp");
  EXPECT_EQ(tags[1], "python");
  EXPECT_EQ(tags[2], "rust");
}

TEST_F(YamlUtilsTest, ConfigParsing_GetSequence_NotFound) {
  YAML::Node node = YAML::Load("name: test");
  std::vector<std::string> seq;
  EXPECT_FALSE(config_parsing::get_sequence(node, "missing", seq));
}

TEST_F(YamlUtilsTest, ConfigParsing_GetSequence_NotSequence) {
  YAML::Node node = YAML::Load("count: 5");
  std::vector<int> seq;
  EXPECT_FALSE(config_parsing::get_sequence(node, "count", seq));
}

TEST_F(YamlUtilsTest, ConfigParsing_GetSequence_IntSequence) {
  YAML::Node node = YAML::Load("ids:\n  - 1\n  - 2\n  - 3");
  std::vector<int> ids;
  EXPECT_TRUE(config_parsing::get_sequence(node, "ids", ids));
  ASSERT_EQ(ids.size(), 3U);
  EXPECT_EQ(ids[0], 1);
  EXPECT_EQ(ids[2], 3);
}

// ----------------------------------------------------------------
// config_parsing::expect_config
// ----------------------------------------------------------------

TEST_F(YamlUtilsTest, ConfigParsing_ExpectConfig_Match) {
  YAML::Node node = YAML::Load("version: 1");
  EXPECT_TRUE(config_parsing::expect_config(node, "version", 1));
}

TEST_F(YamlUtilsTest, ConfigParsing_ExpectConfig_Mismatch) {
  YAML::Node node = YAML::Load("version: 2");
  EXPECT_FALSE(config_parsing::expect_config(node, "version", 1));
}

TEST_F(YamlUtilsTest, ConfigParsing_ExpectConfig_KeyNotFound) {
  YAML::Node node = YAML::Load("name: test");
  EXPECT_FALSE(config_parsing::expect_config(node, "version", 1));
}

TEST_F(YamlUtilsTest, ConfigParsing_ExpectConfig_StringMatch) {
  YAML::Node node = YAML::Load("engine: neug");
  EXPECT_TRUE(
      config_parsing::expect_config(node, "engine", std::string("neug")));
  EXPECT_FALSE(
      config_parsing::expect_config(node, "engine", std::string("other")));
}

}  // namespace test
}  // namespace neug
