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

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_broker.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/nest_column.h"
#include "neug/utils/property/property.h"
#include "unittest/utils.h"

using namespace neug;
using namespace neug::execution;

// ===========================================================================
// Helpers
// ===========================================================================

template <typename T>
static void ExpectValueListEq(const Value& val,
                              std::initializer_list<T> expected) {
  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), expected.size());
  size_t i = 0;
  for (auto& e : expected) {
    EXPECT_EQ(children[i].template GetValue<T>(), e) << "index " << i;
    ++i;
  }
}

static void ExpectValueStringListEq(
    const Value& val, std::initializer_list<std::string_view> expected) {
  ASSERT_EQ(val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(val);
  ASSERT_EQ(children.size(), expected.size());
  size_t i = 0;
  for (auto& e : expected) {
    EXPECT_EQ(StringValue::Get(children[i]), e) << "index " << i;
    ++i;
  }
}

// Test fixture with CheckpointManager for ListColumn tests.
class ListColumnFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ = std::filesystem::temp_directory_path() /
                ("test_list_column_" + std::to_string(::getpid()) + "_" +
                 GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    ws_.Open(temp_dir_.string());
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::shared_ptr<Checkpoint> MakeCheckpoint() { return make_checkpoint(ws_); }

 private:
  std::filesystem::path temp_dir_;
  CheckpointManager ws_;

  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};

// ---------------------------------------------------------------------------
// ListColumn — set / get via Value API
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, SetAndGetIntList) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(4);

  col.set_any(
      0,
      Value::LIST(DataType(DataTypeId::kInt32),
                  {Value::INT32(10), Value::INT32(20), Value::INT32(30)}),
      true);
  col.set_any(1, Value::LIST(DataType(DataTypeId::kInt32), {}), true);
  col.set_any(2, Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(99)}),
              true);
  col.set_any(3,
              Value::LIST(DataType(DataTypeId::kInt32),
                          {Value::INT32(-1), Value::INT32(-2)}),
              true);

  ExpectValueListEq<int32_t>(col.get_any(0), {10, 20, 30});
  EXPECT_EQ(ListValue::GetChildren(col.get_any(1)).size(), 0u);
  ExpectValueListEq<int32_t>(col.get_any(2), {99});
  ExpectValueListEq<int32_t>(col.get_any(3), {-1, -2});
}

TEST_F(ListColumnFixture, SetAndGetVarcharList) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType::Varchar(256));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(2);

  col.set_any(
      0,
      Value::LIST(DataType::Varchar(256), {Value::STRING(std::string("alice")),
                                           Value::STRING(std::string("bob"))}),
      true);
  col.set_any(1,
              Value::LIST(DataType::Varchar(256),
                          {Value::STRING(std::string("single"))}),
              true);

  ExpectValueStringListEq(col.get_any(0), {"alice", "bob"});
  ExpectValueStringListEq(col.get_any(1), {"single"});
}

// ---------------------------------------------------------------------------
// ListColumn — dump and reload (with child column round-trip via broker)
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, DumpAndReload) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt64));

  CheckpointManifest meta;
  const std::string col_key = "test_col";
  {
    ListColumn col(list_type);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(3);

    col.set_any(0,
                Value::LIST(DataType(DataTypeId::kInt64),
                            {Value::INT64(100), Value::INT64(200)}),
                true);
    col.set_any(1,
                Value::LIST(DataType(DataTypeId::kInt64), {Value::INT64(300)}),
                true);
    col.set_any(2, Value::LIST(DataType(DataTypeId::kInt64), {}), true);

    col.DumpTo(*ckp, meta, col_key);
  }

  {
    ModuleBroker store;
    store.Open(*ckp, meta, MemoryLevel::kInMemory);
    auto col = store.TakeModule<ListColumn>(col_key);
    col->SetListType(list_type);
    col->RestoreChildren(store, col_key);

    ExpectValueListEq<int64_t>(col->get_any(0), {100L, 200L});
    ExpectValueListEq<int64_t>(col->get_any(1), {300L});
    EXPECT_EQ(ListValue::GetChildren(col->get_any(2)).size(), 0u);
  }
}

// ---------------------------------------------------------------------------
// ListColumn — resize with non-empty default value
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, ResizeWithDefaultValue) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);

  Value default_val = Value::LIST(DataType(DataTypeId::kInt32),
                                  {Value::INT32(7), Value::INT32(8)});
  col.resize(3, default_val);

  for (size_t i = 0; i < 3; ++i) {
    ExpectValueListEq<int32_t>(col.get_any(i), {7, 8});
  }

  col.resize(5);
  EXPECT_EQ(ListValue::GetChildren(col.get_any(3)).size(), 0u);
  EXPECT_EQ(ListValue::GetChildren(col.get_any(4)).size(), 0u);

  Value default2 =
      Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(99)});
  col.resize(1);
  col.resize(4, default2);

  ExpectValueListEq<int32_t>(col.get_any(0), {7, 8});
  for (size_t i = 1; i < 4; ++i) {
    ExpectValueListEq<int32_t>(col.get_any(i), {99});
  }

  Value empty_default =
      Value::LIST(DataType(DataTypeId::kInt32), std::vector<Value>{});
  col.resize(6, empty_default);
  for (size_t i = 4; i < 6; ++i) {
    EXPECT_EQ(ListValue::GetChildren(col.get_any(i)).size(), 0u) << "row " << i;
  }
}

// ---------------------------------------------------------------------------
// Concurrent writes to ListColumn (child column model)
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, ConcurrentWrites) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(100);

  std::atomic<size_t> counter(0);
  std::vector<std::thread> threads;
  const int thread_num = 8;

  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&]() {
      while (true) {
        size_t idx = counter.fetch_add(1);
        if (idx >= 100)
          break;
        col.set_any(idx,
                    Value::LIST(DataType(DataTypeId::kInt32),
                                {Value::INT32(static_cast<int32_t>(idx))}),
                    true);
      }
    });
  }
  for (auto& th : threads)
    th.join();

  for (size_t i = 0; i < 100; ++i) {
    ExpectValueListEq<int32_t>(col.get_any(i), {static_cast<int32_t>(i)});
  }
}

// ---------------------------------------------------------------------------
// Mixed POD and non-POD concurrent writes
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, MixedPodNonPodConcurrent) {
  auto ckp = MakeCheckpoint();
  DataType pod_type = DataType::List(DataType(DataTypeId::kInt64));
  DataType nonpod_type = DataType::List(DataType::Varchar(256));

  ListColumn pod_col(pod_type);
  ListColumn nonpod_col(nonpod_type);
  pod_col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  nonpod_col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  pod_col.resize(50);
  nonpod_col.resize(50);

  std::atomic<size_t> counter(0);
  std::vector<std::thread> threads;
  const int thread_num = 8;

  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&]() {
      while (true) {
        size_t idx = counter.fetch_add(1);
        if (idx >= 50)
          break;
        pod_col.set_any(
            idx,
            Value::LIST(DataType(DataTypeId::kInt64),
                        {Value::INT64(static_cast<int64_t>(idx)),
                         Value::INT64(static_cast<int64_t>(idx * 2))}),
            true);
        nonpod_col.set_any(
            idx,
            Value::LIST(DataType::Varchar(256),
                        {Value::STRING("str_" + std::to_string(idx))}),
            true);
      }
    });
  }
  for (auto& th : threads)
    th.join();

  for (size_t i = 0; i < 50; ++i) {
    auto val = pod_col.get_any(i);
    const auto& children = ListValue::GetChildren(val);
    ASSERT_EQ(children.size(), 2u) << "row " << i;
    EXPECT_EQ(children[0].GetValue<int64_t>(), static_cast<int64_t>(i));
    EXPECT_EQ(children[1].GetValue<int64_t>(), static_cast<int64_t>(i * 2));

    ExpectValueStringListEq(nonpod_col.get_any(i),
                            {"str_" + std::to_string(i)});
  }
}

// ---------------------------------------------------------------------------
// Update same row grows child frontier (old data becomes hole)
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, UpdateGrowsChildFrontier) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(2);

  col.set_any(0, Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(1)}),
              true);
  uint64_t frontier_after_first = col.child_frontier();

  col.set_any(0,
              Value::LIST(DataType(DataTypeId::kInt32),
                          {Value::INT32(10), Value::INT32(20)}),
              true);
  uint64_t frontier_after_second = col.child_frontier();

  EXPECT_GT(frontier_after_second, frontier_after_first);
  ExpectValueListEq<int32_t>(col.get_any(0), {10, 20});
}

// ---------------------------------------------------------------------------
// Nested list: List<List<int32>> set/get round-trip
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, NestedListStorage) {
  auto ckp = MakeCheckpoint();
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);
  ListColumn col(outer_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(3);

  Value row0 = Value::LIST(
      inner_type,
      {Value::LIST(DataType(DataTypeId::kInt32),
                   {Value::INT32(1), Value::INT32(2)}),
       Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(3)})});
  Value row1 = Value::LIST(inner_type, {});
  Value row2 =
      Value::LIST(inner_type, {Value::LIST(DataType(DataTypeId::kInt32), {})});

  col.set_any(0, row0, true);
  col.set_any(1, row1, true);
  col.set_any(2, row2, true);

  {
    auto val = col.get_any(0);
    const auto& outer = ListValue::GetChildren(val);
    ASSERT_EQ(outer.size(), 2u);
    ExpectValueListEq<int32_t>(outer[0], {1, 2});
    ExpectValueListEq<int32_t>(outer[1], {3});
  }
  {
    auto val = col.get_any(1);
    EXPECT_EQ(ListValue::GetChildren(val).size(), 0u);
  }
  {
    auto val = col.get_any(2);
    const auto& outer = ListValue::GetChildren(val);
    ASSERT_EQ(outer.size(), 1u);
    EXPECT_EQ(ListValue::GetChildren(outer[0]).size(), 0u);
  }

  ASSERT_NE(col.child_column(), nullptr);
  auto* inner_lc = dynamic_cast<ListColumn*>(col.child_column());
  ASSERT_NE(inner_lc, nullptr);
}

// ---------------------------------------------------------------------------
// Nested list dump and reload via ModuleBroker
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, NestedListDumpReload) {
  auto ckp = MakeCheckpoint();
  DataType inner_type = DataType::List(DataType(DataTypeId::kInt32));
  DataType outer_type = DataType::List(inner_type);

  CheckpointManifest meta;
  const std::string col_key = "nested_col";
  {
    ListColumn col(outer_type);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(2);

    col.set_any(
        0,
        Value::LIST(inner_type,
                    {Value::LIST(DataType(DataTypeId::kInt32),
                                 {Value::INT32(10), Value::INT32(20)})}),
        true);
    col.set_any(
        1,
        Value::LIST(
            inner_type,
            {Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(30)}),
             Value::LIST(DataType(DataTypeId::kInt32),
                         {Value::INT32(40), Value::INT32(50)})}),
        true);

    col.DumpTo(*ckp, meta, col_key);
  }

  {
    ModuleBroker store;
    store.Open(*ckp, meta, MemoryLevel::kInMemory);
    auto col = store.TakeModule<ListColumn>(col_key);
    col->SetListType(outer_type);
    col->RestoreChildren(store, col_key);

    auto val0 = col->get_any(0);
    const auto& outer0 = ListValue::GetChildren(val0);
    ASSERT_EQ(outer0.size(), 1u);
    ExpectValueListEq<int32_t>(outer0[0], {10, 20});

    auto val1 = col->get_any(1);
    const auto& outer1 = ListValue::GetChildren(val1);
    ASSERT_EQ(outer1.size(), 2u);
    ExpectValueListEq<int32_t>(outer1[0], {30});
    ExpectValueListEq<int32_t>(outer1[1], {40, 50});
  }
}

// ---------------------------------------------------------------------------
// Dump compacts child column by eliminating holes from updates
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, DumpCompactsChildHoles) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  CheckpointManifest meta;
  const std::string col_key = "compact_col";
  {
    ListColumn col(list_type);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(3);

    col.set_any(
        0,
        Value::LIST(DataType(DataTypeId::kInt32),
                    {Value::INT32(1), Value::INT32(2), Value::INT32(3)}),
        true);
    col.set_any(1,
                Value::LIST(DataType(DataTypeId::kInt32),
                            {Value::INT32(10), Value::INT32(20)}),
                true);
    col.set_any(2,
                Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(100)}),
                true);

    // 6 live children, child_frontier == 6
    EXPECT_EQ(col.child_frontier(), 6u);

    // Update row 0 with a longer list — old 3 children become holes
    col.set_any(0,
                Value::LIST(DataType(DataTypeId::kInt32),
                            {Value::INT32(7), Value::INT32(8)}),
                true);
    // child_frontier grew: 6 + 2 = 8, but only 5 children are live
    EXPECT_EQ(col.child_frontier(), 8u);

    // Update row 1 — old 2 children become holes
    col.set_any(
        1, Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(99)}), true);
    // child_frontier: 8 + 1 = 9, live children: 4
    EXPECT_EQ(col.child_frontier(), 9u);

    col.DumpTo(*ckp, meta, col_key);
    // After Dump (which runs compact), compaction should have run
    EXPECT_EQ(col.child_frontier(), 4u);
  }

  // Reload and verify data integrity
  {
    ModuleBroker store;
    store.Open(*ckp, meta, MemoryLevel::kInMemory);
    auto col = store.TakeModule<ListColumn>(col_key);
    col->SetListType(list_type);
    col->RestoreChildren(store, col_key);

    ExpectValueListEq<int32_t>(col->get_any(0), {7, 8});
    ExpectValueListEq<int32_t>(col->get_any(1), {99});
    ExpectValueListEq<int32_t>(col->get_any(2), {100});
    EXPECT_EQ(col->child_frontier(), 4u);
  }
}

// ---------------------------------------------------------------------------
// Dump compaction expands shared entries into sequential layout for offset
// format
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, DumpCompactsSharedDefaults) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  CheckpointManifest meta;
  const std::string col_key = "compact_dedup";
  {
    ListColumn col(list_type);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);

    // resize with a non-empty default — all rows share the same child segment
    auto default_val = Value::LIST(DataType(DataTypeId::kInt32),
                                   {Value::INT32(42), Value::INT32(43)});
    col.resize(5, default_val);
    // All 5 rows point to the same {offset, 2} entry
    // child_frontier == 2 (shared default written once)
    EXPECT_EQ(col.child_frontier(), 2u);

    // Update row 2 — creates a new segment, old shared entry still valid for
    // others
    col.set_any(
        2, Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(99)}), true);
    EXPECT_EQ(col.child_frontier(), 3u);

    col.DumpTo(*ckp, meta, col_key);
    // After compaction: shared entries are fully expanded for sequential
    // layout. Each row gets its own region: 2+2+1+2+2 = 9 total child elements.
    EXPECT_EQ(col.child_frontier(), 9u);
  }

  {
    ModuleBroker store;
    store.Open(*ckp, meta, MemoryLevel::kInMemory);
    auto col = store.TakeModule<ListColumn>(col_key);
    col->SetListType(list_type);
    col->RestoreChildren(store, col_key);

    for (size_t i : {0u, 1u, 3u, 4u}) {
      ExpectValueListEq<int32_t>(col->get_any(i), {42, 43});
    }
    ExpectValueListEq<int32_t>(col->get_any(2), {99});
  }
}

// ---------------------------------------------------------------------------
// Large list (>65535 elements) — verifies 64-bit length support
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, LargeListExceeds16BitLimit) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));
  ListColumn col(list_type);
  col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(1);

  const size_t N = 70000;  // exceeds old 16-bit limit of 65535
  std::vector<Value> children;
  children.reserve(N);
  for (size_t i = 0; i < N; ++i) {
    children.push_back(Value::INT32(static_cast<int32_t>(i)));
  }
  col.set_any(0, Value::LIST(DataType(DataTypeId::kInt32), std::move(children)),
              true);

  auto val = col.get_any(0);
  const auto& result = ListValue::GetChildren(val);
  ASSERT_EQ(result.size(), N);
  EXPECT_EQ(result[0].GetValue<int32_t>(), 0);
  EXPECT_EQ(result[N - 1].GetValue<int32_t>(), static_cast<int32_t>(N - 1));
}

// ---------------------------------------------------------------------------
// Large list dump and reload round-trip (offset vector format)
// ---------------------------------------------------------------------------
TEST_F(ListColumnFixture, LargeListDumpReload) {
  auto ckp = MakeCheckpoint();
  DataType list_type = DataType::List(DataType(DataTypeId::kInt32));

  CheckpointManifest meta;
  const std::string col_key = "large_list_col";
  const size_t N = 70000;
  {
    ListColumn col(list_type);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(2);

    std::vector<Value> children;
    children.reserve(N);
    for (size_t i = 0; i < N; ++i) {
      children.push_back(Value::INT32(static_cast<int32_t>(i)));
    }
    col.set_any(0,
                Value::LIST(DataType(DataTypeId::kInt32), std::move(children)),
                true);
    col.set_any(
        1, Value::LIST(DataType(DataTypeId::kInt32), {Value::INT32(42)}), true);

    col.DumpTo(*ckp, meta, col_key);
  }

  {
    ModuleBroker store;
    store.Open(*ckp, meta, MemoryLevel::kInMemory);
    auto col = store.TakeModule<ListColumn>(col_key);
    col->SetListType(list_type);
    col->RestoreChildren(store, col_key);

    auto val0 = col->get_any(0);
    const auto& result = ListValue::GetChildren(val0);
    ASSERT_EQ(result.size(), N);
    EXPECT_EQ(result[0].GetValue<int32_t>(), 0);
    EXPECT_EQ(result[N - 1].GetValue<int32_t>(), static_cast<int32_t>(N - 1));

    ExpectValueListEq<int32_t>(col->get_any(1), {42});
  }
}
