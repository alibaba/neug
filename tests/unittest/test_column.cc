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

  EXPECT_THROW(
      {
        std::vector<execution::Value> values = {execution::Value::INT32(1)};
        auto value = execution::Value::ARRAY(array_type, std::move(values));
        (void) value;
      },
      exception::InvalidArgumentException);

  EXPECT_THROW(
      {
        std::vector<execution::Value> values = {execution::Value::INT32(1),
                                                execution::Value::INT64(2)};
        auto value = execution::Value::ARRAY(array_type, std::move(values));
        (void) value;
      },
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

  std::vector<execution::Value> list_values = {execution::Value::INT32(1),
                                               execution::Value::INT32(2)};
  auto list_value =
      execution::Value::LIST(DataType::INT32, std::move(list_values));
  EXPECT_THROW({ column.set_any(0, list_value, true); },
               exception::InvalidArgumentException);

  std::vector<execution::Value> array_values = {execution::Value::INT32(3),
                                                execution::Value::INT32(4)};
  auto array_value =
      execution::Value::ARRAY(array_type, std::move(array_values));
  EXPECT_NO_THROW({ column.set_any(0, array_value, true); });

  auto stored = column.get_any(0);
  ASSERT_EQ(stored.type(), array_type);
  const auto& stored_values = execution::ArrayValue::GetChildren(stored);
  ASSERT_EQ(stored_values.size(), 2);
  EXPECT_EQ(stored_values[0].GetValue<int32_t>(), 3);
  EXPECT_EQ(stored_values[1].GetValue<int32_t>(), 4);

  std::filesystem::remove_all(temp_dir);
}

}  // namespace

}  // namespace test
}  // namespace neug
