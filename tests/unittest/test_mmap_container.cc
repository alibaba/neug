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

#include <cstring>
#include <filesystem>
#include <string>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/file_mmap_container.h"

namespace {

template <typename Container>
class DirtyCheckCountingContainer : public Container {
 public:
  bool IsDirty() override {
    ++dirty_checks;
    return Container::IsDirty();
  }

  size_t dirty_checks = 0;
};

}  // namespace

class MMapContainerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "neug_mmap_test";
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string CreateTestFile(const std::string& name, const char* payload,
                             size_t payload_size) {
    neug::FilePrivateMMap container;
    container.OpenAnonymous(payload_size);
    if (payload_size > 0) {
      std::memcpy(container.GetData(), payload, payload_size);
    }
    auto path = (test_dir_ / name).string();
    container.Dump(path);
    return path;
  }

  std::filesystem::path test_dir_;
};

TEST_F(MMapContainerTest, CommitRuntimeFileSkipsDirtyCheck) {
  neug::CheckpointManager manager;
  manager.Open(test_dir_.string());
  auto staging = manager.CreateStaging();
  auto checkpoint = staging.checkpoint();

  for (bool dirty : {false, true}) {
    SCOPED_TRACE(dirty);
    std::string payload = "runtime checkpoint payload";
    auto initial = checkpoint->CreateRuntimeContainer(
        payload.size(), neug::MemoryLevel::kSyncToFile);
    std::memcpy(initial->GetData(), payload.data(), payload.size());
    initial->Sync();
    const auto runtime_path = initial->GetPath();
    initial->Close();

    DirtyCheckCountingContainer<neug::FileSharedMMap> container;
    container.Open(runtime_path);
    if (dirty) {
      payload[0] = 'R';
      static_cast<char*>(container.GetData())[0] = payload[0];
    }
    const auto object_path = checkpoint->Commit(container);
    EXPECT_EQ(container.dirty_checks, 0u);
    EXPECT_EQ(container.GetData(), nullptr);
    EXPECT_FALSE(std::filesystem::exists(runtime_path));
    EXPECT_EQ(std::filesystem::path(object_path).parent_path(),
              test_dir_ / "checkpoint" / "objects");

    neug::FilePrivateMMap reopened;
    reopened.Open(object_path);
    ASSERT_EQ(reopened.GetDataSize(), payload.size());
    EXPECT_EQ(std::memcmp(reopened.GetData(), payload.data(), payload.size()),
              0);
    // Dump must still persist a checksum matching the committed payload.
    EXPECT_FALSE(reopened.IsDirty());
  }
}

TEST_F(MMapContainerTest, CommitObjectChecksDirtyStateBeforeReuse) {
  neug::CheckpointManager manager;
  manager.Open(test_dir_.string());
  auto staging = manager.CreateStaging();
  auto checkpoint = staging.checkpoint();
  const std::string original_payload = "immutable checkpoint payload";
  neug::FilePrivateMMap initial;
  initial.Open(CreateTestFile("initial.bin", original_payload.data(),
                              original_payload.size()));
  const auto original_path = checkpoint->Commit(initial);

  for (bool dirty : {false, true}) {
    SCOPED_TRACE(dirty);
    std::string payload = original_payload;
    DirtyCheckCountingContainer<neug::FilePrivateMMap> container;
    container.Open(original_path);
    if (dirty) {
      payload[0] = 'I';
      static_cast<char*>(container.GetData())[0] = payload[0];
    }
    const auto committed_path = checkpoint->Commit(container);
    EXPECT_EQ(container.dirty_checks, 1u);
    EXPECT_EQ(container.GetData(), nullptr);
    EXPECT_EQ(committed_path == original_path, !dirty);

    neug::FilePrivateMMap reopened;
    reopened.Open(committed_path);
    ASSERT_EQ(reopened.GetDataSize(), payload.size());
    EXPECT_EQ(std::memcmp(reopened.GetData(), payload.data(), payload.size()),
              0);
    EXPECT_FALSE(reopened.IsDirty());
  }

  neug::FilePrivateMMap original;
  original.Open(original_path);
  ASSERT_EQ(original.GetDataSize(), original_payload.size());
  EXPECT_EQ(std::memcmp(original.GetData(), original_payload.data(),
                        original_payload.size()),
            0);
  EXPECT_FALSE(original.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_NoMapping) {
  neug::FilePrivateMMap container;
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_AnonymousMMap) {
  neug::FilePrivateMMap container;
  container.OpenAnonymous(64);
  EXPECT_TRUE(container.IsDirty());

  // Small anonymous mmap (< FileHeader size) should also be dirty
  neug::FilePrivateMMap small_container;
  small_container.OpenAnonymous(8);
  EXPECT_TRUE(small_container.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_Resize) {
  neug::FilePrivateMMap container;
  container.Resize(128);
  EXPECT_TRUE(container.IsDirty());

  container.Resize(0);
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_FileBacked) {
  const char payload[] = "hello world test data";
  auto path = CreateTestFile("test.bin", payload, sizeof(payload));

  // Unmodified file is not dirty
  neug::FilePrivateMMap container;
  container.Open(path);
  EXPECT_FALSE(container.IsDirty());

  // Mutating data makes it dirty
  static_cast<char*>(container.GetData())[0] ^= 0xFF;
  EXPECT_TRUE(container.IsDirty());

  // After close, not dirty
  container.Close();
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_SharedMMap) {
  const char payload[] = "shared mmap test payload";
  auto path = CreateTestFile("shared.bin", payload, sizeof(payload));

  neug::FileSharedMMap container;
  container.Open(path);
  EXPECT_FALSE(container.IsDirty());

  static_cast<char*>(container.GetData())[0] ^= 0xFF;
  EXPECT_TRUE(container.IsDirty());
}
