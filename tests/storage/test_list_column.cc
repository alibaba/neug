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

// ---------------------------------------------------------------------------
// is_pod_type
// ---------------------------------------------------------------------------
TEST(IsPodTypeTest, PodIds) {
  EXPECT_TRUE(is_pod_type(DataTypeId::kBoolean));
  EXPECT_TRUE(is_pod_type(DataTypeId::kInt8));
  EXPECT_TRUE(is_pod_type(DataTypeId::kInt16));
  EXPECT_TRUE(is_pod_type(DataTypeId::kInt32));
  EXPECT_TRUE(is_pod_type(DataTypeId::kInt64));
  EXPECT_TRUE(is_pod_type(DataTypeId::kUInt8));
  EXPECT_TRUE(is_pod_type(DataTypeId::kUInt16));
  EXPECT_TRUE(is_pod_type(DataTypeId::kUInt32));
  EXPECT_TRUE(is_pod_type(DataTypeId::kUInt64));
  EXPECT_TRUE(is_pod_type(DataTypeId::kFloat));
  EXPECT_TRUE(is_pod_type(DataTypeId::kDouble));
  EXPECT_TRUE(is_pod_type(DataTypeId::kDate));
  EXPECT_TRUE(is_pod_type(DataTypeId::kTimestampMs));
  EXPECT_TRUE(is_pod_type(DataTypeId::kInterval));
}

TEST(IsPodTypeTest, NonPodIds) {
  EXPECT_FALSE(is_pod_type(DataTypeId::kVarchar));
  EXPECT_FALSE(is_pod_type(DataTypeId::kList));
  EXPECT_FALSE(is_pod_type(DataTypeId::kStruct));
}

// ---------------------------------------------------------------------------
// ListViewBuilder + ListView  — POD element types
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, Int32Roundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  ListViewBuilder b;
  b.append_pod<int32_t>(10);
  b.append_pod<int32_t>(20);
  b.append_pod<int32_t>(30);
  EXPECT_EQ(b.count(), 3u);

  std::string blob = b.finish_pod<int32_t>();

  // Expected size: 4 (count) + 3*4 (ints) = 16
  EXPECT_EQ(blob.size(), 4u + 3u * sizeof(int32_t));

  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 3u);
  EXPECT_EQ(lv.GetElem<int32_t>(0), 10);
  EXPECT_EQ(lv.GetElem<int32_t>(1), 20);
  EXPECT_EQ(lv.GetElem<int32_t>(2), 30);
}

TEST(ListViewBuilderTest, DoubleRoundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kDouble));

  ListViewBuilder b;
  b.append_pod<double>(1.5);
  b.append_pod<double>(2.5);
  std::string blob = b.finish_pod<double>();

  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 2u);
  EXPECT_DOUBLE_EQ(lv.GetElem<double>(0), 1.5);
  EXPECT_DOUBLE_EQ(lv.GetElem<double>(1), 2.5);
}

TEST(ListViewBuilderTest, BoolRoundtrip) {
  DataType list_type = DataType::List(DataType(DataTypeId::kBoolean));

  ListViewBuilder b;
  b.append_pod<bool>(true);
  b.append_pod<bool>(false);
  b.append_pod<bool>(true);
  std::string blob = b.finish_pod<bool>();

  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 3u);
  EXPECT_EQ(lv.GetElem<bool>(0), true);
  EXPECT_EQ(lv.GetElem<bool>(1), false);
  EXPECT_EQ(lv.GetElem<bool>(2), true);
}

TEST(ListViewBuilderTest, EmptyPodList) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt64));

  ListViewBuilder b;
  std::string blob = b.finish_pod<int64_t>();

  // size: 4 (count=0) + 0 elements = 4
  EXPECT_EQ(blob.size(), 4u);

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
  ASSERT_EQ(lv.size(), 3u);
  EXPECT_EQ(lv.GetChildStringView(0), "hello");
  EXPECT_EQ(lv.GetChildStringView(1), "world");
  EXPECT_EQ(lv.GetChildStringView(2), "foo");
}

TEST(ListViewBuilderTest, VarcharEmptyList) {
  DataType list_type = DataType::List(DataType::Varchar(256));

  ListViewBuilder b;
  std::string blob = b.finish_varlen();

  ListView lv(list_type, blob);
  EXPECT_EQ(lv.size(), 0u);
}

TEST(ListViewBuilderTest, VarcharWithEmptyElement) {
  DataType list_type = DataType::List(DataType::Varchar(256));

  ListViewBuilder b;
  b.append_blob("");
  b.append_blob("abc");
  b.append_blob("");
  std::string blob = b.finish_varlen();

  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 3u);
  EXPECT_EQ(lv.GetChildStringView(0), "");
  EXPECT_EQ(lv.GetChildStringView(1), "abc");
  EXPECT_EQ(lv.GetChildStringView(2), "");
}

// ---------------------------------------------------------------------------
// Nested List<List<int32_t>>
// ---------------------------------------------------------------------------
TEST(ListViewBuilderTest, NestedIntList) {
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);

  // Build inner blobs
  ListViewBuilder inner_b0;
  inner_b0.append_pod<int32_t>(1);
  inner_b0.append_pod<int32_t>(2);
  std::string inner0 = inner_b0.finish_pod<int32_t>();

  ListViewBuilder inner_b1;
  inner_b1.append_pod<int32_t>(3);
  inner_b1.append_pod<int32_t>(4);
  inner_b1.append_pod<int32_t>(5);
  std::string inner1 = inner_b1.finish_pod<int32_t>();

  // Build outer blob
  ListViewBuilder outer_b;
  outer_b.append_blob(inner0);
  outer_b.append_blob(inner1);
  std::string outer_blob = outer_b.finish_varlen();

  ListView outer(outer_type, outer_blob);
  ASSERT_EQ(outer.size(), 2u);

  ListView child0 = outer.GetChildListView(0);
  ASSERT_EQ(child0.size(), 2u);
  EXPECT_EQ(child0.GetElem<int32_t>(0), 1);
  EXPECT_EQ(child0.GetElem<int32_t>(1), 2);

  ListView child1 = outer.GetChildListView(1);
  ASSERT_EQ(child1.size(), 3u);
  EXPECT_EQ(child1.GetElem<int32_t>(0), 3);
  EXPECT_EQ(child1.GetElem<int32_t>(1), 4);
  EXPECT_EQ(child1.GetElem<int32_t>(2), 5);
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
  std::string blob = b.finish_pod<int32_t>();
  ListView lv(list_type, blob);
  ASSERT_EQ(lv.size(), 1u);
  EXPECT_EQ(lv.GetElem<int32_t>(0), 42);
}

// ---------------------------------------------------------------------------
// ListColumn — set / get
// ---------------------------------------------------------------------------
TEST(ListColumnTest, SetAndGetIntList) {
  std::string tmp_dir = "/tmp/test_list_column_int";
  if (std::filesystem::exists(tmp_dir)) {
    std::filesystem::remove_all(tmp_dir);
  }
  std::filesystem::create_directories(tmp_dir);

  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.open("col", tmp_dir, tmp_dir);
  col.resize(4);

  // Row 0: [10, 20, 30]
  {
    ListViewBuilder b;
    b.append_pod<int32_t>(10);
    b.append_pod<int32_t>(20);
    b.append_pod<int32_t>(30);
    col.set_value(0, b.finish_pod<int32_t>());
  }
  // Row 1: []
  {
    ListViewBuilder b;
    col.set_value(1, b.finish_pod<int32_t>());
  }
  // Row 2: [99]
  {
    ListViewBuilder b;
    b.append_pod<int32_t>(99);
    col.set_value(2, b.finish_pod<int32_t>());
  }
  // Row 3: [-1, -2]
  {
    ListViewBuilder b;
    b.append_pod<int32_t>(-1);
    b.append_pod<int32_t>(-2);
    col.set_value(3, b.finish_pod<int32_t>());
  }

  // Verify
  {
    ListView lv = col.get_view(0);
    ASSERT_EQ(lv.size(), 3u);
    EXPECT_EQ(lv.GetElem<int32_t>(0), 10);
    EXPECT_EQ(lv.GetElem<int32_t>(1), 20);
    EXPECT_EQ(lv.GetElem<int32_t>(2), 30);
  }
  {
    ListView lv = col.get_view(1);
    EXPECT_EQ(lv.size(), 0u);
  }
  {
    ListView lv = col.get_view(2);
    ASSERT_EQ(lv.size(), 1u);
    EXPECT_EQ(lv.GetElem<int32_t>(0), 99);
  }
  {
    ListView lv = col.get_view(3);
    ASSERT_EQ(lv.size(), 2u);
    EXPECT_EQ(lv.GetElem<int32_t>(0), -1);
    EXPECT_EQ(lv.GetElem<int32_t>(1), -2);
  }
}

TEST(ListColumnTest, SetAndGetVarcharList) {
  std::string tmp_dir = "/tmp/test_list_column_varchar";
  if (std::filesystem::exists(tmp_dir)) {
    std::filesystem::remove_all(tmp_dir);
  }
  std::filesystem::create_directories(tmp_dir);

  DataType list_type = DataType::List(DataType::Varchar(256));
  ListColumn col(list_type);
  col.open("col", tmp_dir, tmp_dir);
  col.resize(2);

  // Row 0: ["alice", "bob"]
  {
    ListViewBuilder b;
    b.append_blob("alice");
    b.append_blob("bob");
    col.set_value(0, b.finish_varlen());
  }
  // Row 1: ["single"]
  {
    ListViewBuilder b;
    b.append_blob("single");
    col.set_value(1, b.finish_varlen());
  }

  {
    ListView lv = col.get_view(0);
    ASSERT_EQ(lv.size(), 2u);
    EXPECT_EQ(lv.GetChildStringView(0), "alice");
    EXPECT_EQ(lv.GetChildStringView(1), "bob");
  }
  {
    ListView lv = col.get_view(1);
    ASSERT_EQ(lv.size(), 1u);
    EXPECT_EQ(lv.GetChildStringView(0), "single");
  }
}

// ---------------------------------------------------------------------------
// ListColumn — dump and reload from disk
// ---------------------------------------------------------------------------
TEST(ListColumnTest, DumpAndReload) {
  std::string work_dir = "/tmp/test_list_column_dump_work";
  std::string snap_dir = "/tmp/test_list_column_dump_snap";
  if (std::filesystem::exists(work_dir)) {
    std::filesystem::remove_all(work_dir);
  }
  if (std::filesystem::exists(snap_dir)) {
    std::filesystem::remove_all(snap_dir);
  }
  std::filesystem::create_directories(work_dir);
  std::filesystem::create_directories(snap_dir);

  DataType list_type = DataType::List(DataType(DataTypeId::kInt64));

  // Write phase: open with work_dir, then dump to snap_dir
  {
    ListColumn col(list_type);
    col.open("scores", work_dir, work_dir);
    col.resize(3);

    ListViewBuilder b;
    b.append_pod<int64_t>(100L);
    b.append_pod<int64_t>(200L);
    col.set_value(0, b.finish_pod<int64_t>());

    b.reset();
    b.append_pod<int64_t>(300L);
    col.set_value(1, b.finish_pod<int64_t>());

    b.reset();
    col.set_value(2, b.finish_pod<int64_t>());  // empty

    col.dump(snap_dir + "/scores");
  }

  // Reload phase: open with snap_dir as snapshot
  {
    ListColumn col(list_type);
    col.open("scores", snap_dir, "");

    {
      ListView lv = col.get_view(0);
      ASSERT_EQ(lv.size(), 2u);
      EXPECT_EQ(lv.GetElem<int64_t>(0), 100L);
      EXPECT_EQ(lv.GetElem<int64_t>(1), 200L);
    }
    {
      ListView lv = col.get_view(1);
      ASSERT_EQ(lv.size(), 1u);
      EXPECT_EQ(lv.GetElem<int64_t>(0), 300L);
    }
    {
      ListView lv = col.get_view(2);
      EXPECT_EQ(lv.size(), 0u);
    }
  }
}

// ---------------------------------------------------------------------------
// ListColumn — resize with non-empty default value
// ---------------------------------------------------------------------------
TEST(ListColumnTest, ResizeWithDefaultValue) {
  std::string tmp_dir = "/tmp/test_list_column_resize_default";
  if (std::filesystem::exists(tmp_dir)) {
    std::filesystem::remove_all(tmp_dir);
  }
  std::filesystem::create_directories(tmp_dir);

  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.open("col", tmp_dir, tmp_dir);

  // Build a default blob: [7, 8]
  ListViewBuilder b;
  b.append_pod<int32_t>(7);
  b.append_pod<int32_t>(8);
  std::string default_blob = b.finish_pod<int32_t>();
  Property default_prop = Property::from_list_data(default_blob);

  // Resize to 3 rows with the default.
  col.resize(3, default_prop);

  // All three rows should read back as [7, 8].
  for (size_t i = 0; i < 3; ++i) {
    ListView lv = col.get_view(i);
    ASSERT_EQ(lv.size(), 2u) << "row " << i;
    EXPECT_EQ(lv.GetElem<int32_t>(0), 7) << "row " << i;
    EXPECT_EQ(lv.GetElem<int32_t>(1), 8) << "row " << i;
  }

  // Grow by 2 more rows without a default — they should be empty lists.
  col.resize(5);
  {
    ListView lv = col.get_view(3);
    EXPECT_EQ(lv.size(), 0u);
  }
  {
    ListView lv = col.get_view(4);
    EXPECT_EQ(lv.size(), 0u);
  }

  // Shrink to 1 and grow to 4 again with a new default: [99]
  b.reset();
  b.append_pod<int32_t>(99);
  Property default2 = Property::from_list_data(b.finish_pod<int32_t>());
  col.resize(1);
  col.resize(4, default2);
  {
    ListView lv = col.get_view(0);  // survived the shrink — still [7,8]
    ASSERT_EQ(lv.size(), 2u);
    EXPECT_EQ(lv.GetElem<int32_t>(0), 7);
    EXPECT_EQ(lv.GetElem<int32_t>(1), 8);
  }
  for (size_t i = 1; i < 4; ++i) {
    ListView lv = col.get_view(i);
    ASSERT_EQ(lv.size(), 1u) << "row " << i;
    EXPECT_EQ(lv.GetElem<int32_t>(0), 99) << "row " << i;
  }

  // Resize with an empty default — new slots should be empty lists.
  Property empty_default = Property::from_list_data("");
  col.resize(6, empty_default);
  for (size_t i = 4; i < 6; ++i) {
    ListView lv = col.get_view(i);
    EXPECT_EQ(lv.size(), 0u) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// ListViewToValue bridge
// ---------------------------------------------------------------------------
TEST(ListViewToValueTest, Int32ToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  ListViewBuilder b;
  b.append_pod<int32_t>(5);
  b.append_pod<int32_t>(6);
  b.append_pod<int32_t>(7);
  std::string blob = b.finish_pod<int32_t>();

  ListView lv(list_type, blob);
  Value val = ListViewToValue(lv);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 3u);
  EXPECT_EQ(children[0].GetValue<int32_t>(), 5);
  EXPECT_EQ(children[1].GetValue<int32_t>(), 6);
  EXPECT_EQ(children[2].GetValue<int32_t>(), 7);
}

TEST(ListViewToValueTest, DoubleToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kDouble));

  ListViewBuilder b;
  b.append_pod<double>(3.14);
  b.append_pod<double>(2.72);
  std::string blob = b.finish_pod<double>();

  ListView lv(list_type, blob);
  Value val = ListViewToValue(lv);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 2u);
  EXPECT_DOUBLE_EQ(children[0].GetValue<double>(), 3.14);
  EXPECT_DOUBLE_EQ(children[1].GetValue<double>(), 2.72);
}

TEST(ListViewToValueTest, VarcharToValue) {
  DataType list_type = DataType::List(DataType::Varchar(256));

  ListViewBuilder b;
  b.append_blob("foo");
  b.append_blob("bar");
  std::string blob = b.finish_varlen();

  ListView lv(list_type, blob);
  Value val = ListViewToValue(lv);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), 2u);
  EXPECT_EQ(StringValue::Get(children[0]), "foo");
  EXPECT_EQ(StringValue::Get(children[1]), "bar");
}

TEST(ListViewToValueTest, EmptyListToValue) {
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  ListViewBuilder b;
  std::string blob = b.finish_pod<int32_t>();

  ListView lv(list_type, blob);
  Value val = ListViewToValue(lv);

  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  EXPECT_EQ(children.size(), 0u);
}

TEST(ListViewToValueTest, NestedListToValue) {
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);

  ListViewBuilder inner_b;
  inner_b.append_pod<int32_t>(1);
  inner_b.append_pod<int32_t>(2);
  std::string inner_blob = inner_b.finish_pod<int32_t>();

  ListViewBuilder outer_b;
  outer_b.append_blob(inner_blob);
  std::string outer_blob = outer_b.finish_varlen();

  ListView outer_lv(outer_type, outer_blob);
  Value outer_val = ListViewToValue(outer_lv);

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
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListViewBuilder b;
  b.append_pod<int32_t>(1);
  b.append_pod<int32_t>(2);
  std::string blob = b.finish_pod<int32_t>();

  Property p = Property::from_list_data(blob);
  EXPECT_EQ(p.type(), DataTypeId::kList);
  EXPECT_EQ(p.as_list_data(), blob);
}

TEST(PropertyListTest, SetListData) {
  DataType list_type = DataType::List(DataType::Varchar(64));
  ListViewBuilder b;
  b.append_blob("x");
  b.append_blob("y");
  std::string blob = b.finish_varlen();

  Property p;
  p.set_list_data(blob);
  EXPECT_EQ(p.type(), DataTypeId::kList);
  EXPECT_EQ(p.as_list_data(), blob);
}
