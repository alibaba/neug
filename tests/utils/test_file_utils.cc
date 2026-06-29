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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "neug/utils/io/file/file_utils.h"

// Non-static helper from file_utils.cc, exposed for direct testing of the
// sparse-aware fallback path (otherwise shadowed by clonefile/FICLONE on
// supported filesystems).
namespace neug {
namespace file_utils {
void fallback_copy(const std::string& src_path, const std::string& dst_path,
                   const struct stat& src_stat);
}  // namespace file_utils
}  // namespace neug

namespace neug {
namespace test {

namespace {

struct DataSegment {
  off_t offset;
  std::string data;
};

// Write each segment at its offset (gaps stay as holes), then optionally
// truncate to `logical_size` to add a trailing hole.
void make_file(const std::string& path,
               const std::vector<DataSegment>& segments,
               off_t logical_size = -1) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  ASSERT_GE(fd, 0) << "open(" << path << "): " << ::strerror(errno);
  for (const auto& seg : segments) {
    ssize_t n = ::pwrite(fd, seg.data.data(), seg.data.size(), seg.offset);
    ASSERT_EQ(n, static_cast<ssize_t>(seg.data.size())) << ::strerror(errno);
  }
  if (logical_size >= 0) {
    ASSERT_EQ(::ftruncate(fd, logical_size), 0) << ::strerror(errno);
  }
  ::close(fd);
}

std::string read_at(const std::string& path, off_t offset, size_t len) {
  std::string buf(len, '\0');
  int fd = ::open(path.c_str(), O_RDONLY);
  EXPECT_GE(fd, 0) << ::strerror(errno);
  if (fd < 0)
    return buf;
  EXPECT_EQ(::pread(fd, buf.data(), len, offset), static_cast<ssize_t>(len));
  ::close(fd);
  return buf;
}

// {logical_size, physical_size_bytes}; physical is st_blocks * 512.
std::pair<off_t, off_t> stat_sizes(const std::string& path) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0)
    return {-1, -1};
  return {st.st_size, static_cast<off_t>(st.st_blocks) * 512};
}

}  // namespace

class FileUtilsCopyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    tmp_dir_ = std::filesystem::temp_directory_path() /
               (std::string("neug_fileutils_") + info->name() + "_" +
                std::to_string(::getpid()));
    std::filesystem::remove_all(tmp_dir_);
    std::filesystem::create_directories(tmp_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(tmp_dir_, ec);
  }

  std::string path(const std::string& name) const {
    return (tmp_dir_ / name).string();
  }

  std::filesystem::path tmp_dir_;
};

// End-to-end check of the public copy_file() wrapper on a sparse source.
// On macOS/APFS this exercises the clonefile fast path; on Linux/Btrfs
// it exercises FICLONE; elsewhere it falls through to fallback_copy.
// All paths must preserve content and not blow up sparseness.
//
// Regression for issue #440: previously, on non-COW filesystems (e.g.
// Linux ext4), reflink would fail and copy_file_range would succeed —
// but copy_file_range materializes holes into physical zero blocks,
// bloating a sparse src (4K real / 102G logical) into a 102G dst. The
// fix routes sparse sources straight to fallback_copy.
TEST_F(FileUtilsCopyTest, CopyFile_SparseFilePreservesContentAndSparseness) {
  const off_t kLogical = 64LL << 20;
  const std::vector<DataSegment> segs = {
      {0, "HEAD_DATA"},
      {kLogical / 2, "MID_DATA"},
      {kLogical - 64, "TAIL_DATA"},
  };
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, segs, kLogical));

  auto result = file_utils::copy_file(src, dst, /*overwrite=*/true);

  auto [dst_logical, dst_physical] = stat_sizes(dst);
  auto [src_logical, src_physical] = stat_sizes(src);
  EXPECT_EQ(dst_logical, kLogical);
  for (const auto& seg : segs) {
    EXPECT_EQ(read_at(dst, seg.offset, seg.data.size()), seg.data);
  }
  if (src_physical < kLogical / 2) {  // src is sparse on this FS
    // Sparse sources must never take the copy_file_range path — it
    // bloats holes on non-COW filesystems.
    EXPECT_NE(result, file_utils::CopyResult::CopyFileRange)
        << "Sparse src took copy_file_range path; risks bloating dst";
    EXPECT_LT(dst_physical, kLogical / 2)
        << "Sparse src materialized into dense dst (physical=" << dst_physical
        << ", logical=" << kLogical << ")";
  }
}

// Direct test of fallback_copy on a sparse source. This is the core
// regression test for issue #440 — without sparse-awareness, dst would
// materialize the hole bytes and bloat to logical size. Necessary as a
// dedicated test because clonefile/FICLONE will shadow this path in
// the public copy_file() on most filesystems.
TEST_F(FileUtilsCopyTest, FallbackCopy_SparseFilePreservesSparseness) {
  const off_t kLogical = 64LL << 20;
  const std::vector<DataSegment> segs = {
      {0, "HEAD_DATA"},
      {kLogical / 2, "MID_DATA"},
      {kLogical - 64, "TAIL_DATA"},
  };
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, segs, kLogical));

  struct stat src_st {};
  ASSERT_EQ(::stat(src.c_str(), &src_st), 0);
  ASSERT_NO_THROW(file_utils::fallback_copy(src, dst, src_st));

  auto [dst_logical, dst_physical] = stat_sizes(dst);
  auto [src_logical, src_physical] = stat_sizes(src);
  EXPECT_EQ(dst_logical, kLogical);
  for (const auto& seg : segs) {
    EXPECT_EQ(read_at(dst, seg.offset, seg.data.size()), seg.data);
  }
  // A sample inside a hole reads back as zeros.
  for (char c : read_at(dst, 1LL << 20, 4096))
    EXPECT_EQ(c, '\0');

  if (src_physical < kLogical / 2) {
    EXPECT_LE(dst_physical, src_physical + (4LL << 20))
        << "fallback_copy bloated dst: src_physical=" << src_physical
        << " dst_physical=" << dst_physical;
  }
}

// Guards the ENXIO edge case: on macOS, lseek(SEEK_HOLE) on an empty
// file returns ENXIO because offset 0 is already past EOF. Without the
// special-case for file_size==0, fallback_copy would throw here.
TEST_F(FileUtilsCopyTest, FallbackCopy_EmptyFile) {
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {}));

  struct stat src_st {};
  ASSERT_EQ(::stat(src.c_str(), &src_st), 0);

  ASSERT_NO_THROW(file_utils::fallback_copy(src, dst, src_st));
  EXPECT_EQ(stat_sizes(dst).first, 0);
}

}  // namespace test
}  // namespace neug
