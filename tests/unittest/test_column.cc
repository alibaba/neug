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
#include <chrono>
#include <filesystem>
#include <memory>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/list_property_column.h"
#include "neug/utils/property/vec_column.h"
#include "unittest/utils.h"

namespace neug {
namespace test {

namespace {

// Test data for int32 column
static const std::vector<int32_t> kInt32TestData = {10, 20, 30, 40, 50};

// Test data for string column
static const std::vector<std::string_view> kStringTestData = {
    "hello", "world", "test", "cow", "verify"};

struct ColumnCowSignature {
  size_t element_num{0};
  int64_t value_sum{0};
  size_t first_element_size{0};
};

// Build signature for int32 column
ColumnCowSignature build_column_signature(const TypedColumn<int32_t>& col) {
  ColumnCowSignature sig;
  sig.element_num = col.size();
  for (size_t i = 0; i < col.size(); ++i) {
    sig.value_sum += col.get_view(i);
  }
  if (col.size() > 0) {
    sig.first_element_size = sizeof(int32_t);
  }
  return sig;
}

// Build signature for string column
ColumnCowSignature build_column_signature(
    const TypedColumn<std::string_view>& col) {
  ColumnCowSignature sig;
  sig.element_num = col.size();
  for (size_t i = 0; i < col.size(); ++i) {
    auto view = col.get_view(i);
    // Sum the first character code of each string for verification
    if (!view.empty()) {
      sig.value_sum += static_cast<int32_t>(view[0]);
    }
  }
  if (col.size() > 0) {
    sig.first_element_size = col.get_view(0).size();
  }
  return sig;
}

void expect_signature_eq(const ColumnCowSignature& lhs,
                         const ColumnCowSignature& rhs) {
  EXPECT_EQ(lhs.element_num, rhs.element_num);
  EXPECT_EQ(lhs.value_sum, rhs.value_sum);
  EXPECT_EQ(lhs.first_element_size, rhs.first_element_size);
}

#ifndef NDEBUG
TEST(ArrayValueTest, ConstructorValidatesPayloadShapeInDebug) {
  auto array_type = DataType::Array(DataType::INT32, 2);

  auto build_invalid_array_wrong_size = [&array_type]() {
    std::vector<Value> values;
    values.emplace_back(Value::INT32(1));
    auto value = Value::ARRAY(array_type, std::move(values));
    (void) value;
  };

  auto build_invalid_array_wrong_type = [&array_type]() {
    std::vector<Value> values;
    values.emplace_back(Value::INT32(1));
    values.emplace_back(Value::INT64(2));
    auto value = Value::ARRAY(array_type, std::move(values));
    (void) value;
  };

  EXPECT_THROW(build_invalid_array_wrong_size(),
               exception::InvalidArgumentException);
  EXPECT_THROW(build_invalid_array_wrong_type(),
               exception::InvalidArgumentException);
}
#endif

// Apply mutations to int32 column
void apply_column_mutations(TypedColumn<int32_t>& col) {
  if (col.size() > 0) {
    col.set_value(0, 999);  // Modify first element
    if (col.size() > 1) {
      col.set_value(1, 888);  // Modify second element
    }
  }
}

// Apply mutations to string column
void apply_column_mutations(TypedColumn<std::string_view>& col) {
  if (col.size() > 0) {
    col.set_value(0, "mutated");  // Modify first element
    if (col.size() > 1) {
      col.set_value(1, "changed");  // Modify second element
    }
  }
}

template <typename ELEMENT_T, MemoryLevel OPEN_LEVEL,
          MemoryLevel MATERIALIZE_LEVEL>
struct ColumnMaterializeLevelCase {
  using ElementType = ELEMENT_T;
  static constexpr MemoryLevel kOpenLevel = OPEN_LEVEL;
  static constexpr MemoryLevel kMaterializeLevel = MATERIALIZE_LEVEL;
};

using Int32Cases = ::testing::Types<
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kInMemory,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kInMemory,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kInMemory,
                               MemoryLevel::kSyncToFile>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kSyncToFile>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kSyncToFile,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kSyncToFile,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<int32_t, MemoryLevel::kSyncToFile,
                               MemoryLevel::kSyncToFile>>;

using StringCases = ::testing::Types<
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kInMemory,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kInMemory,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kInMemory,
                               MemoryLevel::kSyncToFile>,
    ColumnMaterializeLevelCase<std::string_view,
                               MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<std::string_view,
                               MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<std::string_view,
                               MemoryLevel::kHugePagePreferred,
                               MemoryLevel::kSyncToFile>,
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                               MemoryLevel::kInMemory>,
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                               MemoryLevel::kHugePagePreferred>,
    ColumnMaterializeLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                               MemoryLevel::kSyncToFile>>;

template <typename CASE_T>
class TypedColumnInt32CowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("typed_column_int32_cow_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()) +
         "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    checkpoint_mgr_.Open(temp_dir_.string());
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::shared_ptr<Checkpoint> create_checkpoint() {
    return make_checkpoint(checkpoint_mgr_);
  }

 private:
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }

 protected:
  CheckpointManager checkpoint_mgr_;
  std::filesystem::path temp_dir_;
};

TYPED_TEST_SUITE(TypedColumnInt32CowTest, Int32Cases);

TYPED_TEST(TypedColumnInt32CowTest, CowIsolationAndDumpOpenMatrix) {
  TypedColumn<int32_t> original;
  auto base_ckp = this->create_checkpoint();
  original.Open(*base_ckp, ModuleDescriptor(), TypeParam::kOpenLevel);
  original.resize(kInt32TestData.size());
  for (size_t i = 0; i < kInt32TestData.size(); ++i) {
    original.set_value(i, kInt32TestData[i]);
  }

  auto original_before = build_column_signature(original);

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<TypedColumn<int32_t>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);
  // Detach detaches IDataContainer so writes to cow don't affect
  // original.
  cow->Detach(*base_ckp, TypeParam::kMaterializeLevel);

  apply_column_mutations(*cow);
  auto cow_after = build_column_signature(*cow);

  auto original_after_cow_mutation = build_column_signature(original);
  expect_signature_eq(original_after_cow_mutation, original_before);

  apply_column_mutations(original);
  auto original_after_self_mutation = build_column_signature(original);
  EXPECT_NE(original_after_self_mutation.value_sum, original_before.value_sum);

  auto cow_after_original_mutation = build_column_signature(*cow);
  expect_signature_eq(cow_after_original_mutation, cow_after);

  auto dump_ckp = this->create_checkpoint();
  auto cow_desc = dump_module_descriptor(*cow, *dump_ckp, "int32_column");
  TypedColumn<int32_t> reopened;
  reopened.Open(*dump_ckp, cow_desc, MemoryLevel::kInMemory);
  auto reopened_sig = build_column_signature(reopened);
  expect_signature_eq(reopened_sig, cow_after);
}

template <typename CASE_T>
class TypedColumnStringCowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("typed_column_string_cow_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()) +
         "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    checkpoint_mgr_.Open(temp_dir_.string());
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::shared_ptr<Checkpoint> create_checkpoint() {
    return make_checkpoint(checkpoint_mgr_);
  }

 private:
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }

 protected:
  CheckpointManager checkpoint_mgr_;
  std::filesystem::path temp_dir_;
};

TYPED_TEST_SUITE(TypedColumnStringCowTest, StringCases);

TYPED_TEST(TypedColumnStringCowTest, CowIsolationAndDumpOpenMatrix) {
  TypedColumn<std::string_view> original;
  auto base_ckp = this->create_checkpoint();
  original.Open(*base_ckp, ModuleDescriptor(), TypeParam::kOpenLevel);
  original.resize(kStringTestData.size());
  for (size_t i = 0; i < kStringTestData.size(); ++i) {
    original.set_value(i, kStringTestData[i]);
  }

  auto original_before = build_column_signature(original);

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<TypedColumn<std::string_view>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);
  // Detach detaches IDataContainer so writes to cow don't affect
  // original.
  cow->Detach(*base_ckp, TypeParam::kMaterializeLevel);

  apply_column_mutations(*cow);
  auto cow_after = build_column_signature(*cow);

  auto original_after_cow_mutation = build_column_signature(original);
  expect_signature_eq(original_after_cow_mutation, original_before);

  apply_column_mutations(original);
  auto original_after_self_mutation = build_column_signature(original);
  EXPECT_NE(original_after_self_mutation.value_sum, original_before.value_sum);

  auto cow_after_original_mutation = build_column_signature(*cow);
  expect_signature_eq(cow_after_original_mutation, cow_after);

  auto dump_ckp = this->create_checkpoint();
  auto cow_desc = dump_module_descriptor(*cow, *dump_ckp, "string_column");
  TypedColumn<std::string_view> reopened;
  reopened.Open(*dump_ckp, cow_desc, MemoryLevel::kInMemory);
  auto reopened_sig = build_column_signature(reopened);
  expect_signature_eq(reopened_sig, cow_after);
}

TEST(StringColumnTest, CopyItemDoesNotAppendPayload) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("string_column_copy_item_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  StringColumn column;
  column.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  column.resize(3);
  column.set_value(0, "dead");
  column.set_value(1, "keep-one");
  column.set_value(2, "keep-two");

  auto available_space = column.available_space();
  column.copy_item(0, 1);
  column.copy_item(1, 2);
  column.shrink_items(2);
  EXPECT_EQ(column.available_space(), available_space);
  EXPECT_EQ(column.size(), 2);
  EXPECT_EQ(column.get_view(0), "keep-one");
  EXPECT_EQ(column.get_view(1), "keep-two");

  std::filesystem::remove_all(temp_dir);
}

TEST(ArrayColumnTest, SetAnyRequiresArrayValue) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("array_column_set_any_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  if (std::filesystem::exists(temp_dir)) {
    std::filesystem::remove_all(temp_dir);
  }
  std::filesystem::create_directories(temp_dir);

  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  auto array_type = DataType::Array(DataType::INT32, 2);
  ArrayColumn column(array_type);
  column.Open(*ckp, ModuleDescriptor(), MemoryLevel::kInMemory);
  column.resize(1);

  std::vector<Value> list_values = {Value::INT32(1), Value::INT32(2)};
  auto list_value = Value::LIST(DataType::INT32, std::move(list_values));
  EXPECT_THROW({ column.set_any(0, list_value, true); },
               exception::InvalidArgumentException);

  std::vector<Value> array_values = {Value::INT32(3), Value::INT32(4)};
  auto array_value = Value::ARRAY(array_type, std::move(array_values));
  EXPECT_NO_THROW({ column.set_any(0, array_value, true); });

  auto stored = column.get_any(0);
  ASSERT_EQ(stored.type(), array_type);
  const auto& stored_values = ArrayValue::GetChildren(stored);
  ASSERT_EQ(stored_values.size(), 2);
  EXPECT_EQ(stored_values[0].GetValue<int32_t>(), 3);
  EXPECT_EQ(stored_values[1].GetValue<int32_t>(), 4);

  std::filesystem::remove_all(temp_dir);
}

TEST(ListPropertyColumnTest, RecursiveLifecycle) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("list_property_column_recursive_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  auto string_list_type = DataType::List(DataType::VARCHAR);
  auto pair_type = DataType::Array(string_list_type, 2);
  auto outer_type = DataType::List(pair_type);
  auto strings = [&](std::initializer_list<const char*> values) {
    std::vector<Value> children;
    for (auto value : values) {
      children.push_back(Value::STRING(value));
    }
    return Value::LIST(DataType::VARCHAR, std::move(children));
  };
  auto pair = [&](Value lhs, Value rhs) {
    std::vector<Value> children;
    children.push_back(std::move(lhs));
    children.push_back(std::move(rhs));
    return Value::ARRAY(pair_type, std::move(children));
  };
  auto outer = [&](std::vector<Value> values) {
    return Value::LIST(pair_type, std::move(values));
  };

  ListPropertyColumn column(outer_type);
  column.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  column.resize(2);
  auto initial = outer({pair(strings({"a"}), strings({})),
                        pair(strings({"b", "c"}), strings({"d"}))});
  column.set_any(0, initial, true);
  column.set_any(1, outer({}), true);
  EXPECT_EQ(column.get_any(0), initial);
  EXPECT_EQ(ListValue::GetChildren(column.get_any(1)).size(), 0);

  auto clone_module = column.Clone();
  auto* clone = dynamic_cast<ListPropertyColumn*>(clone_module.get());
  ASSERT_NE(clone, nullptr);
  clone->Detach(*ckp, MemoryLevel::kInMemory);
  auto clone_value = outer({pair(strings({"x", "y"}), strings({"z"}))});
  clone->set_any(0, clone_value, true);
  EXPECT_EQ(column.get_any(0), initial);
  EXPECT_EQ(clone->get_any(0), clone_value);

  auto final_value = outer({pair(strings({}), strings({"last"}))});
  clone->set_any(0, final_value, true);
  CheckpointManifest manifest;
  clone->Dump(*ckp, manifest, "list");
  ListPropertyColumn reopened;
  reopened.Open(*ckp, manifest, *manifest.module("list"),
                MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.list_type(), outer_type);
  EXPECT_EQ(reopened.get_any(0), final_value);
  EXPECT_EQ(ListValue::GetChildren(reopened.get_any(1)).size(), 0);

  std::filesystem::remove_all(temp_dir);
}

TEST(ListPropertyColumnTest, DumpCompactsByPhysicalOffset) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("list_property_column_in_place_compaction_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  auto list_type = DataType::List(DataType::INT32);
  auto list = [](std::initializer_list<int32_t> values) {
    std::vector<Value> children;
    for (auto value : values) {
      children.push_back(Value::INT32(value));
    }
    return Value::LIST(DataType::INT32, std::move(children));
  };

  ListPropertyColumn column(list_type);
  column.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  column.resize(3);
  column.set_any(0, list({10}), true);
  column.set_any(1, list({20, 21}), true);
  column.set_any(0, list({30, 31, 32}), true);

  CheckpointManifest manifest;
  column.Dump(*ckp, manifest, "list");

  ListPropertyColumn reopened;
  reopened.Open(*ckp, manifest, *manifest.module("list"),
                MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_any(0), list({30, 31, 32}));
  EXPECT_EQ(reopened.get_any(1), list({20, 21}));
  EXPECT_TRUE(ListValue::GetChildren(reopened.get_any(2)).empty());

  std::filesystem::remove_all(temp_dir);
}

TEST(ListPropertyColumnTest, ResizeDefaultAndTypeContract) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("list_property_column_resize_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  auto list_type = DataType::List(DataType::INT32);
  auto list = [](std::initializer_list<int32_t> values) {
    std::vector<Value> children;
    for (auto value : values) {
      children.push_back(Value::INT32(value));
    }
    return Value::LIST(DataType::INT32, std::move(children));
  };

  ListPropertyColumn column(list_type);
  column.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  column.resize(3);
  for (size_t i = 0; i < column.size(); ++i) {
    EXPECT_TRUE(ListValue::GetChildren(column.get_any(i)).empty());
  }

  column.set_any(0, list({1, 2}), true);
  column.set_any(0, list({3, 4}), false);
  EXPECT_EQ(column.get_any(0), list({3, 4}));
  column.set_any(0, list({5}), true);
  EXPECT_EQ(column.get_any(0), list({5}));
  column.set_any(0, list({}), true);
  EXPECT_TRUE(ListValue::GetChildren(column.get_any(0)).empty());
  column.set_any(1, list({6}), false);
  EXPECT_EQ(column.get_any(1), list({6}));
  column.set_any(1, Value(list_type), true);
  EXPECT_TRUE(ListValue::GetChildren(column.get_any(1)).empty());
  EXPECT_THROW(column.set_any(2, list({7}), false),
               exception::StorageException);

  column.resize(1);
  column.resize(3);
  EXPECT_TRUE(ListValue::GetChildren(column.get_any(1)).empty());
  EXPECT_TRUE(ListValue::GetChildren(column.get_any(2)).empty());

  EXPECT_THROW(
      column.set_any(0, Value::LIST(DataType::INT64, {Value::INT64(1)}), true),
      exception::InvalidArgumentException);
  EXPECT_THROW(
      column.set_any(0, Value::LIST(DataType::INT32, {Value::INT64(1)}), true),
      exception::InvalidArgumentException);

  std::filesystem::remove_all(temp_dir);
}

TEST(ListPropertyColumnTest, ExceedsMaxListLength) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("list_property_column_maxlen_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);

  auto list_type = DataType::List(DataType::INT32);
  ListPropertyColumn column(list_type);
  column.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  column.resize(1);

  // 65535 elements (max for 16-bit length) should succeed
  {
    std::vector<Value> children;
    children.reserve(65535);
    for (int32_t i = 0; i < 65535; ++i) {
      children.push_back(Value::INT32(i));
    }
    column.set_any(0, Value::LIST(DataType::INT32, std::move(children)), true);
    EXPECT_EQ(ListValue::GetChildren(column.get_any(0)).size(), 65535);
  }

  // 65536 elements exceeds 16-bit length field — must throw
  {
    std::vector<Value> children;
    children.reserve(65536);
    for (int32_t i = 0; i < 65536; ++i) {
      children.push_back(Value::INT32(i));
    }
    EXPECT_THROW(
        column.set_any(0, Value::LIST(DataType::INT32, std::move(children)),
                       true),
        exception::RuntimeError);
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(VecColumnTest, AccessResizeCloneAndDumpOpen) {
  auto temp_dir =
      std::filesystem::temp_directory_path() /
      ("vec_column_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(temp_dir.string());
  auto ckp = make_checkpoint(checkpoint_mgr);
  DefaultIndexIDAccessor backing_accessor;
  backing_accessor.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  VecColumnBackedIndexIDAccessor backed_accessor(backing_accessor);
  EXPECT_THROW(backed_accessor.UpsertVID(0), exception::RuntimeError);
  auto allocated_index_id = backing_accessor.UpsertVID(0);
  EXPECT_EQ(backed_accessor.UpsertVID(0), allocated_index_id);

  constexpr uint64_t dimension = 2;
  auto array_type = DataType::Array(DataType::FLOAT, dimension);
  auto make_array = [&](float first, float second) {
    return Value::ARRAY(array_type,
                        {Value::FLOAT(first), Value::FLOAT(second)});
  };
  auto default_value = make_array(0.0f, 0.0f);
  auto buffer = ckp->CreateRuntimeContainer(2 * dimension * sizeof(float),
                                            MemoryLevel::kInMemory);
  auto accessor = std::make_unique<DefaultIndexIDAccessor>();
  accessor->Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  VecColumn column(std::move(buffer), std::move(accessor), array_type, 2,
                   default_value, *ckp, MemoryLevel::kInMemory);

  column.set_any(0, make_array(1.0f, 2.0f), true);
  column.set_any(1, make_array(3.0f, 4.0f), true);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(column.get_any(1))[1].GetValue<float>(), 4.0f);
  EXPECT_THROW(column.set_any(2, make_array(5.0f, 6.0f), false),
               exception::StorageException);
  EXPECT_EQ(column.get_offset_accessor()->GetIndexIDByVID(2), INVALID_INDEX_ID);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(column.get_any(0))[0].GetValue<float>(), 1.0f);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(column.get_any(1))[1].GetValue<float>(), 4.0f);

  const void* buffer_before_shrink = column.get_buffer_ptr();
  column.resize(1);
  EXPECT_EQ(column.size(), 2);
  EXPECT_EQ(column.get_buffer_ptr(), buffer_before_shrink);

  auto clone_module = column.Clone();
  auto* clone = dynamic_cast<VecColumn*>(clone_module.get());
  ASSERT_NE(clone, nullptr);
  const void* old_buffer = clone->get_buffer_ptr();
  column.resize(5000);
  EXPECT_NE(column.get_buffer_ptr(), old_buffer);
  EXPECT_EQ(clone->get_buffer_ptr(), old_buffer);
  auto cloned_first_value = clone->get_any(0);
  auto cloned_second_value = clone->get_any(1);
  const auto& cloned_first = ArrayValue::GetChildren(cloned_first_value);
  const auto& cloned_second = ArrayValue::GetChildren(cloned_second_value);
  ASSERT_EQ(cloned_first.size(), dimension);
  ASSERT_EQ(cloned_second.size(), dimension);
  EXPECT_FLOAT_EQ(cloned_first[0].GetValue<float>(), 1.0f);
  EXPECT_FLOAT_EQ(cloned_first[1].GetValue<float>(), 2.0f);
  EXPECT_FLOAT_EQ(cloned_second[0].GetValue<float>(), 3.0f);
  EXPECT_FLOAT_EQ(cloned_second[1].GetValue<float>(), 4.0f);

  column.set_any(4096, make_array(5.0f, 6.0f), true);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(column.get_any(4096))[0].GetValue<float>(), 5.0f);

  auto* column_accessor = column.get_offset_accessor();
  auto old_index_id = column_accessor->GetIndexIDByVID(0);
  column.set_any(0, make_array(7.0f, 8.0f), false);
  auto new_index_id = column_accessor->GetIndexIDByVID(0);
  EXPECT_NE(new_index_id, old_index_id);
  EXPECT_EQ(column_accessor->GetVIDByIndexID(old_index_id), INVALID_VID);
  EXPECT_EQ(column_accessor->GetVIDByIndexID(new_index_id), 0);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(column.get_any(0))[1].GetValue<float>(), 8.0f);

  CheckpointManifest manifest;
  column.Dump(*ckp, manifest, "vec");
  auto manifest_path = temp_dir / "vec_manifest.json";
  manifest.Save(manifest_path.string());
  CheckpointManifest loaded_manifest;
  loaded_manifest.Load(manifest_path.string());
  VecColumn reopened;
  reopened.Open(*ckp, loaded_manifest, *loaded_manifest.module("vec"),
                MemoryLevel::kInMemory);
  EXPECT_FLOAT_EQ(
      ArrayValue::GetChildren(reopened.get_any(4096))[1].GetValue<float>(),
      6.0f);

  std::filesystem::remove_all(temp_dir);
}

}  // namespace

}  // namespace test
}  // namespace neug
