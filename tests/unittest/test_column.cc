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
#include "neug/storages/workspace.h"
#include "neug/utils/property/column.h"

namespace neug {
namespace test {

namespace {

// Test data for int32 column
static const std::vector<int32_t> kInt32TestData = {10, 20, 30, 40, 50};

// Test data for string column
static const std::vector<std::string_view> kStringTestData = {
    "hello", "world", "test", "fork", "verify"};

struct ColumnForkSignature {
  size_t element_num{0};
  int64_t value_sum{0};
  size_t first_element_size{0};
};

// Build signature for int32 column
ColumnForkSignature build_column_signature(const TypedColumn<int32_t>& col) {
  ColumnForkSignature sig;
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
ColumnForkSignature build_column_signature(
    const TypedColumn<std::string_view>& col) {
  ColumnForkSignature sig;
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

void expect_signature_eq(const ColumnForkSignature& lhs,
                         const ColumnForkSignature& rhs) {
  EXPECT_EQ(lhs.element_num, rhs.element_num);
  EXPECT_EQ(lhs.value_sum, rhs.value_sum);
  EXPECT_EQ(lhs.first_element_size, rhs.first_element_size);
}

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

template <typename ELEMENT_T, MemoryLevel OPEN_LEVEL, MemoryLevel FORK_LEVEL>
struct ColumnForkLevelCase {
  using ElementType = ELEMENT_T;
  static constexpr MemoryLevel kOpenLevel = OPEN_LEVEL;
  static constexpr MemoryLevel kForkLevel = FORK_LEVEL;
};

using Int32Cases = ::testing::Types<
    ColumnForkLevelCase<int32_t, MemoryLevel::kInMemory,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kInMemory,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kInMemory,
                        MemoryLevel::kSyncToFile>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kSyncToFile>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kSyncToFile,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kSyncToFile,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<int32_t, MemoryLevel::kSyncToFile,
                        MemoryLevel::kSyncToFile>>;

using StringCases = ::testing::Types<
    ColumnForkLevelCase<std::string_view, MemoryLevel::kInMemory,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kInMemory,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kInMemory,
                        MemoryLevel::kSyncToFile>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kHugePagePreferred,
                        MemoryLevel::kSyncToFile>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                        MemoryLevel::kInMemory>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                        MemoryLevel::kHugePagePreferred>,
    ColumnForkLevelCase<std::string_view, MemoryLevel::kSyncToFile,
                        MemoryLevel::kSyncToFile>>;

template <typename CASE_T>
class TypedColumnInt32ForkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("typed_column_int32_fork_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()) +
         "_" + GetTestName());
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

  Checkpoint& create_checkpoint() {
    auto ckp_id = ws_.CreateCheckpoint();
    return ws_.GetCheckpoint(ckp_id);
  }

 private:
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }

 protected:
  Workspace ws_;
  std::filesystem::path temp_dir_;
};

TYPED_TEST_SUITE(TypedColumnInt32ForkTest, Int32Cases);

TYPED_TEST(TypedColumnInt32ForkTest, ForkIsolationAndDumpOpenMatrix) {
  TypedColumn<int32_t> original;
  auto& base_ckp = this->create_checkpoint();
  original.Open(base_ckp, ModuleDescriptor(), TypeParam::kOpenLevel);
  original.resize(kInt32TestData.size());
  for (size_t i = 0; i < kInt32TestData.size(); ++i) {
    original.set_value(i, kInt32TestData[i]);
  }

  auto original_before = build_column_signature(original);

  auto fork_module = original.Fork(base_ckp, TypeParam::kForkLevel);
  auto* forked = dynamic_cast<TypedColumn<int32_t>*>(fork_module.get());
  ASSERT_NE(forked, nullptr);

  apply_column_mutations(*forked);
  auto fork_after = build_column_signature(*forked);

  auto original_after_fork_mutation = build_column_signature(original);
  expect_signature_eq(original_after_fork_mutation, original_before);

  apply_column_mutations(original);
  auto original_after_self_mutation = build_column_signature(original);
  EXPECT_NE(original_after_self_mutation.value_sum, original_before.value_sum);

  auto fork_after_original_mutation = build_column_signature(*forked);
  expect_signature_eq(fork_after_original_mutation, fork_after);

  auto& dump_ckp = this->create_checkpoint();
  auto fork_desc = forked->Dump(dump_ckp);
  TypedColumn<int32_t> reopened;
  reopened.Open(dump_ckp, fork_desc, MemoryLevel::kInMemory);
  auto reopened_sig = build_column_signature(reopened);
  expect_signature_eq(reopened_sig, fork_after);
}

template <typename CASE_T>
class TypedColumnStringForkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("typed_column_string_fork_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()) +
         "_" + GetTestName());
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

  Checkpoint& create_checkpoint() {
    auto ckp_id = ws_.CreateCheckpoint();
    return ws_.GetCheckpoint(ckp_id);
  }

 private:
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }

 protected:
  Workspace ws_;
  std::filesystem::path temp_dir_;
};

TYPED_TEST_SUITE(TypedColumnStringForkTest, StringCases);

TYPED_TEST(TypedColumnStringForkTest, ForkIsolationAndDumpOpenMatrix) {
  TypedColumn<std::string_view> original;
  auto& base_ckp = this->create_checkpoint();
  original.Open(base_ckp, ModuleDescriptor(), TypeParam::kOpenLevel);
  original.resize(kStringTestData.size());
  for (size_t i = 0; i < kStringTestData.size(); ++i) {
    original.set_value(i, kStringTestData[i]);
  }

  auto original_before = build_column_signature(original);

  auto fork_module = original.Fork(base_ckp, TypeParam::kForkLevel);
  auto* forked =
      dynamic_cast<TypedColumn<std::string_view>*>(fork_module.get());
  ASSERT_NE(forked, nullptr);

  apply_column_mutations(*forked);
  auto fork_after = build_column_signature(*forked);

  auto original_after_fork_mutation = build_column_signature(original);
  expect_signature_eq(original_after_fork_mutation, original_before);

  apply_column_mutations(original);
  auto original_after_self_mutation = build_column_signature(original);
  EXPECT_NE(original_after_self_mutation.value_sum, original_before.value_sum);

  auto fork_after_original_mutation = build_column_signature(*forked);
  expect_signature_eq(fork_after_original_mutation, fork_after);

  auto& dump_ckp = this->create_checkpoint();
  auto fork_desc = forked->Dump(dump_ckp);
  TypedColumn<std::string_view> reopened;
  reopened.Open(dump_ckp, fork_desc, MemoryLevel::kInMemory);
  auto reopened_sig = build_column_signature(reopened);
  expect_signature_eq(reopened_sig, fork_after);
}

}  // namespace

}  // namespace test
}  // namespace neug
