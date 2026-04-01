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
#include <string>
#include <string_view>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/list_view.h"
#include "neug/utils/property/property.h"

using namespace neug;
using namespace neug::execution;

// ===========================================================================
// Helpers — reduce repetitive blob-building and directory setup
// ===========================================================================

// Build a POD list blob from an initializer list.
template <typename T>
static std::string BuildPodBlob(std::initializer_list<T> elems) {
  ListViewBuilder b;
  for (auto& e : elems)
    b.append_pod<T>(e);
  return b.finish_pod<T>();
}

// Build a varchar list blob from an initializer list of string_views.
static std::string BuildVarcharBlob(
    std::initializer_list<std::string_view> elems) {
  ListViewBuilder b;
  for (auto& e : elems)
    b.append_blob(std::string(e));
  return b.finish_varlen();
}

// Verify a POD ListView against expected values.
template <typename T>
static void ExpectPodListEq(const ListView& lv,
                            std::initializer_list<T> expected) {
  ASSERT_EQ(lv.size(), expected.size());
  size_t i = 0;
  for (auto& e : expected) {
    EXPECT_EQ(lv.GetElem<T>(i), e) << "index " << i;
    ++i;
  }
}

// Verify a varchar ListView against expected values.
static void ExpectVarcharListEq(
    const ListView& lv, std::initializer_list<std::string_view> expected) {
  ASSERT_EQ(lv.size(), expected.size());
  size_t i = 0;
  for (auto& e : expected) {
    EXPECT_EQ(lv.GetChildStringView(i), e) << "index " << i;
    ++i;
  }
}

// Test fixture for ListColumn tests — manages temporary directories.
class ListColumnFixture : public ::testing::Test {
 protected:
  void SetUpDir(const std::string& name) {
    dir_ = "/tmp/test_list_column_" + name;
    if (std::filesystem::exists(dir_))
      std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
  }

  void SetUpDirs(const std::string& name) {
    work_dir_ = "/tmp/test_list_column_" + name + "_work";
    snap_dir_ = "/tmp/test_list_column_" + name + "_snap";
    for (auto* d : {&work_dir_, &snap_dir_}) {
      if (std::filesystem::exists(*d))
        std::filesystem::remove_all(*d);
      std::filesystem::create_directories(*d);
    }
  }

  std::string dir_;
  std::string work_dir_;
  std::string snap_dir_;
};

// ---------------------------------------------------------------------------
// is_pod_type
// ---------------------------------------------------------------------------
TEST(IsPodTypeTest, PodIds) {
  for (auto id : {DataTypeId::kBoolean, DataTypeId::kInt8, DataTypeId::kInt16,
                  DataTypeId::kInt32, DataTypeId::kInt64, DataTypeId::kUInt8,
                  DataTypeId::kUInt16, DataTypeId::kUInt32, DataTypeId::kUInt64,
                  DataTypeId::kFloat, DataTypeId::kDouble, DataTypeId::kDate,
                  DataTypeId::kTimestampMs, DataTypeId::kInterval}) {
    EXPECT_TRUE(is_pod_type(id)) << "Expected POD: " << static_cast<int>(id);
  }
}

TEST(IsPodTypeTest, NonPodIds) {
  for (auto id :
       {DataTypeId::kVarchar, DataTypeId::kList, DataTypeId::kStruct}) {
    EXPECT_FALSE(is_pod_type(id))
        << "Expected non-POD: " << static_cast<int>(id);
  }
}

// ---------------------------------------------------------------------------
// ListViewBuilder + ListView  — POD element types
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, Int32Roundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  std::string blob = BuildPodBlob<int32_t>({10, 20, 30});

  // Expected size: 4 (count) + 3*4 (ints) = 16
  EXPECT_EQ(blob.size(), 4u + 3u * sizeof(int32_t));

  ListView lv(list_type, blob);
  ExpectPodListEq<int32_t>(lv, {10, 20, 30});
}

TEST(ListViewBuilderTest, DoubleRoundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kDouble));
  std::string blob = BuildPodBlob<double>({1.5, 2.5});

  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 2u);
  EXPECT_DOUBLE_EQ(lv.GetElem<double>(0), 1.5);
  EXPECT_DOUBLE_EQ(lv.GetElem<double>(1), 2.5);
}

TEST(ListViewBuilderTest, BoolRoundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kBoolean));
  std::string blob = BuildPodBlob<bool>({true, false, true});

  ListView lv(list_type, blob);
  ExpectPodListEq<bool>(lv, {true, false, true});
}

TEST(ListViewBuilderTest, EmptyPodList) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt64));
  std::string blob = BuildPodBlob<int64_t>({});

  EXPECT_EQ(blob.size(), 4u);  // 4 bytes for count=0
  ListView lv(list_type, blob);
  EXPECT_EQ(lv.size(), 0u);
}

// ---------------------------------------------------------------------------
// ListViewBuilder + ListView  — non-POD (varchar) elements
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, VarcharRoundtrip) {
  DataType list_type = DataType::List(DataType::Varchar(256));

  ListViewBuilder b;
  b.append_blob("hello");
  b.append_blob("world");
  b.append_blob("foo");
  EXPECT_EQ(b.count(), 3u);

  std::string blob = b.finish_varlen();
  ListView lv(list_type, blob);
  ExpectVarcharListEq(lv, {"hello", "world", "foo"});
}

TEST(ListViewBuilderTest, VarcharEmptyList) {
  DataType list_type = DataType::List(DataType::Varchar(256));
  std::string blob = BuildVarcharBlob({});

  ListView lv(list_type, blob);
  EXPECT_EQ(lv.size(), 0u);
}

TEST(ListViewBuilderTest, VarcharWithEmptyElement) {
  DataType list_type = DataType::List(DataType::Varchar(256));
  std::string blob = BuildVarcharBlob({"", "abc", ""});

  ListView lv(list_type, blob);
  ExpectVarcharListEq(lv, {"", "abc", ""});
}

// ---------------------------------------------------------------------------
// Nested List<List<int32_t>>
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, NestedIntList) {
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);

  std::string inner0 = BuildPodBlob<int32_t>({1, 2});
  std::string inner1 = BuildPodBlob<int32_t>({3, 4, 5});

  ListViewBuilder outer_b;
  outer_b.append_blob(inner0);
  outer_b.append_blob(inner1);
  std::string outer_blob = outer_b.finish_varlen();

  ListView outer(outer_type, outer_blob);
  ASSERT_EQ(outer.size(), 2u);

  ExpectPodListEq<int32_t>(outer.GetChildListView(0), {1, 2});
  ExpectPodListEq<int32_t>(outer.GetChildListView(1), {3, 4, 5});
}

// ---------------------------------------------------------------------------
// ListViewBuilder reset() reuse
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, ResetReuse) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListViewBuilder b;

  b.append_pod<int32_t>(100);
  b.append_pod<int32_t>(200);
  EXPECT_EQ(b.count(), 2u);

  b.reset();
  EXPECT_EQ(b.count(), 0u);

  b.append_pod<int32_t>(42);
  ListView lv(list_type, b.finish_pod<int32_t>());
  ExpectPodListEq<int32_t>(lv, {42});
}

// ---------------------------------------------------------------------------
// ListColumn — set / get
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, SetAndGetIntList) {
  SetUpDir("int");
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.open("col", dir_, dir_);
  col.resize(4);

  col.set_value(0, BuildPodBlob<int32_t>({10, 20, 30}));
  col.set_value(1, BuildPodBlob<int32_t>({}));
  col.set_value(2, BuildPodBlob<int32_t>({99}));
  col.set_value(3, BuildPodBlob<int32_t>({-1, -2}));

  ExpectPodListEq<int32_t>(col.get_view(0), {10, 20, 30});
  EXPECT_EQ(col.get_view(1).size(), 0u);
  ExpectPodListEq<int32_t>(col.get_view(2), {99});
  ExpectPodListEq<int32_t>(col.get_view(3), {-1, -2});
}

TEST_F(ListColumnFixture, SetAndGetVarcharList) {
  SetUpDir("varchar");
  DataType list_type = DataType::List(DataType::Varchar(256));
  ListColumn col(list_type);
  col.open("col", dir_, dir_);
  col.resize(2);

  col.set_value(0, BuildVarcharBlob({"alice", "bob"}));
  col.set_value(1, BuildVarcharBlob({"single"}));

  ExpectVarcharListEq(col.get_view(0), {"alice", "bob"});
  ExpectVarcharListEq(col.get_view(1), {"single"});
}

// ---------------------------------------------------------------------------
// ListColumn — dump and reload from disk
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, DumpAndReload) {
  SetUpDirs("dump");
  DataType list_type = DataType::List(DataType(DataTypeId::kInt64));

  // Write phase
  {
    ListColumn col(list_type);
    col.open("scores", work_dir_, work_dir_);
    col.resize(3);

    col.set_value(0, BuildPodBlob<int64_t>({100L, 200L}));
    col.set_value(1, BuildPodBlob<int64_t>({300L}));
    col.set_value(2, BuildPodBlob<int64_t>({}));

    col.dump(snap_dir_ + "/scores");
  }

  // Reload phase
  {
    ListColumn col(list_type);
    col.open("scores", snap_dir_, work_dir_);

    ExpectPodListEq<int64_t>(col.get_view(0), {100L, 200L});
    ExpectPodListEq<int64_t>(col.get_view(1), {300L});
    EXPECT_EQ(col.get_view(2).size(), 0u);
  }
}

// ---------------------------------------------------------------------------
// ListColumn — resize with non-empty default value
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, ResizeWithDefaultValue) {
  SetUpDir("resize_default");
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.open("col", dir_, dir_);

  // Resize to 3 rows with default [7, 8].
  Property default_prop =
      Property::from_list_data(BuildPodBlob<int32_t>({7, 8}));
  col.resize(3, default_prop);

  for (size_t i = 0; i < 3; ++i) {
    ExpectPodListEq<int32_t>(col.get_view(i), {7, 8});
  }

  // Grow by 2 more rows without a default — they should be empty lists.
  col.resize(5);
  EXPECT_EQ(col.get_view(3).size(), 0u);
  EXPECT_EQ(col.get_view(4).size(), 0u);

  // Shrink to 1 and grow to 4 with a new default: [99]
  Property default2 = Property::from_list_data(BuildPodBlob<int32_t>({99}));
  col.resize(1);
  col.resize(4, default2);

  ExpectPodListEq<int32_t>(col.get_view(0), {7, 8});  // survived shrink
  for (size_t i = 1; i < 4; ++i) {
    ExpectPodListEq<int32_t>(col.get_view(i), {99});
  }

  // Resize with an empty default — new slots should be empty lists.
  Property empty_default = Property::from_list_data("");
  col.resize(6, empty_default);
  for (size_t i = 4; i < 6; ++i) {
    EXPECT_EQ(col.get_view(i).size(), 0u) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// ListView → Value conversion
// ---------------------------------------------------------------------------
TEST(ListViewToValueTest, Int32ToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  std::string blob = BuildPodBlob<int32_t>({5, 6, 7});
  Value val = property_to_value(Property::from_list_data(blob), list_type);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 3u);
  EXPECT_EQ(children[0].GetValue<int32_t>(), 5);
  EXPECT_EQ(children[1].GetValue<int32_t>(), 6);
  EXPECT_EQ(children[2].GetValue<int32_t>(), 7);
}

TEST(ListViewToValueTest, DoubleToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kDouble));
  std::string blob = BuildPodBlob<double>({3.14, 2.72});
  Value val = property_to_value(Property::from_list_data(blob), list_type);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 2u);
  EXPECT_DOUBLE_EQ(children[0].GetValue<double>(), 3.14);
  EXPECT_DOUBLE_EQ(children[1].GetValue<double>(), 2.72);
}

TEST(ListViewToValueTest, VarcharToValue) {
  DataType list_type = DataType::List(DataType::Varchar(256));
  std::string blob = BuildVarcharBlob({"foo", "bar"});
  Value val = property_to_value(Property::from_list_data(blob), list_type);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 2u);
  EXPECT_EQ(StringValue::Get(children[0]), "foo");
  EXPECT_EQ(StringValue::Get(children[1]), "bar");
}

TEST(ListViewToValueTest, EmptyListToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  std::string blob = BuildPodBlob<int32_t>({});
  Value val = property_to_value(Property::from_list_data(blob), list_type);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  EXPECT_EQ(ListValue::GetChildren(val).size(), 0u);
}

TEST(ListViewToValueTest, NestedListToValue) {
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);

  std::string inner_blob = BuildPodBlob<int32_t>({1, 2});

  ListViewBuilder outer_b;
  outer_b.append_blob(inner_blob);
  std::string outer_blob = outer_b.finish_varlen();

  Value outer_val =
      property_to_value(Property::from_list_data(outer_blob), outer_type);

  ASSERT_EQ(outer_val.type().id(), DataTypeId::kList);
  const auto& outer_children = ListValue::GetChildren(outer_val);
  ASSERT_EQ(outer_children.size(), 1u);

  const Value& inner_val = outer_children[0];
  ASSERT_EQ(inner_val.type().id(), DataTypeId::kList);
  const auto& inner_children = ListValue::GetChildren(inner_val);
  ASSERT_EQ(inner_children.size(), 2u);
  EXPECT_EQ(inner_children[0].GetValue<int32_t>(), 1);
  EXPECT_EQ(inner_children[1].GetValue<int32_t>(), 2);
}

// ---------------------------------------------------------------------------
// Property extensions  — from_list_data / as_list_data / set_list_data
// ---------------------------------------------------------------------------
TEST(PropertyListTest, FromAndAsListData) {
  std::string blob = BuildPodBlob<int32_t>({1, 2});
  Property p = Property::from_list_data(blob);
  EXPECT_EQ(p.type(), DataTypeId::kList);
  EXPECT_EQ(p.as_list_data(), blob);
}

TEST(PropertyListTest, SetListData) {
  std::string blob = BuildVarcharBlob({"x", "y"});
  Property p;
  p.set_list_data(blob);
  EXPECT_EQ(p.type(), DataTypeId::kList);
  EXPECT_EQ(p.as_list_data(), blob);
}
