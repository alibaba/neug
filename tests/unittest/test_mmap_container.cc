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

#include "neug/storages/container/file_mmap_container.h"

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

TEST_F(MMapContainerTest, IsDirty_NoMapping) {
  neug::FilePrivateMMap container;
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, IsDirty_AnonymousMMap) {
  neug::FilePrivateMMap container;
  container.OpenAnonymous(64);
  EXPECT_TRUE(container.IsDirty());

  // Small anonymous mmap (< FileHeader size) should also be dirty
  neug::FilePrivateMMap small;
  small.OpenAnonymous(8);
  EXPECT_TRUE(small.IsDirty());
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
