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

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/file_mmap_container.h"

namespace {

// Raw MD5 bytes written by the legacy OpenSSL-backed format. Keep these
// independent of the new digest implementation so format regressions fail.
constexpr std::array<unsigned char, 16> kAbcMD5 = {
    0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0,
    0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1, 0x7f, 0x72};
constexpr std::array<unsigned char, 16> kAbdMD5 = {
    0x49, 0x11, 0xe5, 0x16, 0xe5, 0xaa, 0x21, 0xd3,
    0x27, 0x51, 0x2e, 0x0c, 0x8b, 0x19, 0x76, 0x16};
constexpr std::array<unsigned char, 16> kEmptyMD5 = {
    0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
    0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e};

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

  void WriteLegacyFile(const std::string& path, const std::string& payload,
                       const std::array<unsigned char, 16>& md5) {
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.write(reinterpret_cast<const char*>(md5.data()), md5.size());
    file.write(payload.data(), payload.size());
    file.close();
    ASSERT_TRUE(file.good());
  }

  void ExpectFileContents(const std::string& path, const std::string& payload,
                          const std::array<unsigned char, 16>& md5) {
    ASSERT_EQ(sizeof(neug::FileHeader), 16u);
    ASSERT_EQ(std::filesystem::file_size(path), 16u + payload.size());
    std::ifstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    std::array<unsigned char, 16> header{};
    file.read(reinterpret_cast<char*>(header.data()), header.size());
    ASSERT_TRUE(file.good());
    EXPECT_EQ(header, md5);
    std::string stored(payload.size(), '\0');
    file.read(stored.data(), stored.size());
    ASSERT_TRUE(file.good());
    EXPECT_EQ(stored, payload);
  }

  std::filesystem::path test_dir_;
};

TEST_F(MMapContainerTest, LegacyMD5HeaderDumpCompatibility) {
  const auto path = (test_dir_ / "legacy.bin").string();
  WriteLegacyFile(path, "abc", kAbcMD5);
  neug::FilePrivateMMap container;
  container.Open(path);
  ASSERT_EQ(container.GetDataSize(), 3u);
  EXPECT_EQ(std::memcmp(container.GetData(), "abc", 3), 0);
  EXPECT_FALSE(container.IsDirty());

  static_cast<char*>(container.GetData())[2] = 'd';
  EXPECT_TRUE(container.IsDirty());
  const auto output = (test_dir_ / "updated.bin").string();
  container.Dump(output);
  ExpectFileContents(output, "abd", kAbdMD5);
  ExpectFileContents(path, "abc", kAbcMD5);
  container.Open(output);
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, LegacyMD5HeaderSyncCompatibility) {
  const auto path = (test_dir_ / "legacy_shared.bin").string();
  WriteLegacyFile(path, "abc", kAbcMD5);
  neug::FileSharedMMap container;
  container.Open(path);
  ASSERT_EQ(container.GetDataSize(), 3u);
  EXPECT_FALSE(container.IsDirty());
  container.Sync();
  ExpectFileContents(path, "abc", kAbcMD5);

  static_cast<char*>(container.GetData())[2] = 'd';
  EXPECT_TRUE(container.IsDirty());
  container.Sync();
  EXPECT_FALSE(container.IsDirty());
  container.Close();
  ExpectFileContents(path, "abd", kAbdMD5);
  container.Open(path);
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, LegacyMD5HeaderCheckpointReuse) {
  neug::CheckpointManager manager;
  manager.Open(test_dir_.string());
  auto staging = manager.CreateStaging();
  auto checkpoint = staging.checkpoint();
  const auto path =
      (test_dir_ / "checkpoint" / "objects" / "legacy-md5.bin").string();
  WriteLegacyFile(path, "abc", kAbcMD5);
  neug::FilePrivateMMap container;
  container.Open(path);
  EXPECT_EQ(checkpoint->Commit(container), path);
  ExpectFileContents(path, "abc", kAbcMD5);

  container.Open(path);
  ASSERT_EQ(container.GetDataSize(), 3u);
  static_cast<char*>(container.GetData())[2] = 'd';
  const auto modified_path = checkpoint->Commit(container);
  EXPECT_NE(modified_path, path);
  ExpectFileContents(modified_path, "abd", kAbdMD5);
  ExpectFileContents(path, "abc", kAbcMD5);
  container.Open(modified_path);
  EXPECT_FALSE(container.IsDirty());
}

TEST_F(MMapContainerTest, EmptyDumpPreservesLegacyMD5Header) {
  const auto path = (test_dir_ / "empty.bin").string();
  neug::FilePrivateMMap container;
  container.Dump(path);
  ExpectFileContents(path, "", kEmptyMD5);
  container.Open(path);
  EXPECT_EQ(container.GetDataSize(), 0u);
  EXPECT_FALSE(container.IsDirty());
  const auto output = (test_dir_ / "empty_copy.bin").string();
  container.Dump(output);
  ExpectFileContents(output, "", kEmptyMD5);

  neug::FileSharedMMap shared;
  shared.Open(output);
  shared.Sync();
  EXPECT_FALSE(shared.IsDirty());
  shared.Close();
  ExpectFileContents(output, "", kEmptyMD5);
}

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
