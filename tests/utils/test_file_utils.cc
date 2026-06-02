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
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "neug/utils/file_utils.h"

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

// ─── Fixtures and helpers ───────────────────────────────────────────────────

struct ScopedFd {
  int fd = -1;
  ScopedFd() = default;
  explicit ScopedFd(int f) : fd(f) {}
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;
  ScopedFd(ScopedFd&& o) noexcept : fd(o.fd) { o.fd = -1; }
  ~ScopedFd() {
    if (fd >= 0) ::close(fd);
  }
  int get() const { return fd; }
};

struct DataSegment {
  off_t offset;
  std::string data;
};

struct FileStats {
  off_t logical;   // st_size
  off_t physical;  // st_blocks * 512 — actual on-disk allocation
};

FileStats stat_file(const std::string& path) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) {
    return {-1, -1};
  }
  return {st.st_size, static_cast<off_t>(st.st_blocks) * 512};
}

// Write each segment at its offset (gaps stay as holes), then optionally
// truncate to `logical_size` to add a trailing hole.
void make_file(const std::string& path,
               const std::vector<DataSegment>& segments,
               off_t logical_size = -1) {
  ScopedFd fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644));
  ASSERT_GE(fd.get(), 0) << "open(" << path << "): " << ::strerror(errno);
  for (const auto& seg : segments) {
    ssize_t n =
        ::pwrite(fd.get(), seg.data.data(), seg.data.size(), seg.offset);
    ASSERT_EQ(n, static_cast<ssize_t>(seg.data.size()))
        << "pwrite(" << path << ", off=" << seg.offset
        << "): " << ::strerror(errno);
  }
  if (logical_size >= 0) {
    ASSERT_EQ(::ftruncate(fd.get(), logical_size), 0)
        << "ftruncate(" << path << ", " << logical_size
        << "): " << ::strerror(errno);
  }
}

std::string read_at(const std::string& path, off_t offset, size_t len) {
  std::string buf(len, '\0');
  ScopedFd fd(::open(path.c_str(), O_RDONLY));
  EXPECT_GE(fd.get(), 0) << "open(" << path << "): " << ::strerror(errno);
  if (fd.get() < 0) return buf;
  ssize_t n = ::pread(fd.get(), buf.data(), len, offset);
  EXPECT_EQ(n, static_cast<ssize_t>(len))
      << "pread(" << path << ", off=" << offset << ", len=" << len << ")";
  return buf;
}

void expect_segments_match(const std::string& path,
                           const std::vector<DataSegment>& expected) {
  for (const auto& seg : expected) {
    EXPECT_EQ(read_at(path, seg.offset, seg.data.size()), seg.data)
        << "content mismatch at offset " << seg.offset;
  }
}

void expect_zeros(const std::string& path, off_t offset, size_t len) {
  std::string buf = read_at(path, offset, len);
  for (size_t i = 0; i < buf.size(); ++i) {
    ASSERT_EQ(buf[i], '\0')
        << "non-zero byte at offset " << (offset + static_cast<off_t>(i))
        << " in " << path;
  }
}

// Common layout: three small data segments scattered through a 64MB file.
std::vector<DataSegment> three_segments(off_t logical_size) {
  return {
      {0, "HEAD_DATA_AT_OFFSET_0"},
      {logical_size / 2, "MID_DATA_NEAR_MIDDLE"},
      {logical_size - 64, "TAIL_DATA_NEAR_EOF"},
  };
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

// ─── Tests of the public copy_file() entry point ────────────────────────────

TEST_F(FileUtilsCopyTest, SparseFile_PreservesContentAndSparseness) {
  const off_t kLogical = 64LL << 20;
  const auto segs = three_segments(kLogical);

  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, segs, kLogical));

  const auto src_stats = stat_file(src);
  ASSERT_EQ(src_stats.logical, kLogical);

  file_utils::copy_file(src, dst, /*overwrite=*/true);
  const auto dst_stats = stat_file(dst);

  EXPECT_EQ(dst_stats.logical, kLogical);
  expect_segments_match(dst, segs);
  expect_zeros(dst, 1LL << 20, 4096);

  // If the underlying FS produced a sparse source, the copy must not
  // explode it. (No assertion fires on filesystems that don't support
  // sparse — st_blocks then matches st_size and the precondition fails.)
  if (src_stats.physical < kLogical / 2) {
    EXPECT_LT(dst_stats.physical, kLogical / 2)
        << "Sparse source materialized into dense dst: dst_physical="
        << dst_stats.physical << " logical=" << kLogical;
  }
}

TEST_F(FileUtilsCopyTest, AllHoleFile_RemainsSparse) {
  const off_t kLogical = 16LL << 20;
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {}, kLogical));

  file_utils::copy_file(src, dst, true);

  const auto dst_stats = stat_file(dst);
  EXPECT_EQ(dst_stats.logical, kLogical);
  expect_zeros(dst, 0, 8192);
  expect_zeros(dst, kLogical - 8192, 8192);

  // A pure-hole file should occupy near-zero on disk on any sane FS.
  const auto src_stats = stat_file(src);
  if (src_stats.physical < 1LL << 20) {
    EXPECT_LT(dst_stats.physical, 1LL << 20)
        << "All-hole src (physical=" << src_stats.physical
        << ") materialized into dst (physical=" << dst_stats.physical << ")";
  }
}

TEST_F(FileUtilsCopyTest, DenseFile_MatchesByteByByte) {
  std::string payload(128 * 1024, '\0');
  std::mt19937 rng(42);
  for (auto& b : payload) b = static_cast<char>(rng() & 0xFF);

  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {{0, payload}}));

  file_utils::copy_file(src, dst, true);

  EXPECT_EQ(stat_file(dst).logical, static_cast<off_t>(payload.size()));
  EXPECT_EQ(read_at(dst, 0, payload.size()), payload);
}

TEST_F(FileUtilsCopyTest, EmptyFile_ProducesEmptyDst) {
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {}));

  file_utils::copy_file(src, dst, true);
  EXPECT_EQ(stat_file(dst).logical, 0);
}

TEST_F(FileUtilsCopyTest, LeadingHole_DataAfterOffset) {
  const off_t kLogical = 32LL << 20;
  const off_t kDataOff = 8LL << 20;
  const std::vector<DataSegment> segs = {
      {kDataOff, "DATA_AFTER_LEADING_HOLE"}};

  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, segs, kLogical));

  file_utils::copy_file(src, dst, true);

  EXPECT_EQ(stat_file(dst).logical, kLogical);
  expect_segments_match(dst, segs);
  expect_zeros(dst, 0, 4096);
  expect_zeros(dst, kLogical - 4096, 4096);
}

// ─── Direct tests of fallback_copy() ───────────────────────────────────────
// On macOS/APFS and Linux/Btrfs/XFS-reflink, the public copy_file() hits
// clonefile/FICLONE first and never exercises fallback_copy. These tests
// reach the helper directly so the sparse-aware fallback stays covered.

class FallbackCopyTest : public FileUtilsCopyTest {};

TEST_F(FallbackCopyTest, SparseFile_PreservesSparseness) {
  const off_t kLogical = 64LL << 20;
  const auto segs = three_segments(kLogical);

  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, segs, kLogical));

  struct stat src_st {};
  ASSERT_EQ(::stat(src.c_str(), &src_st), 0);
  const auto src_stats = stat_file(src);

  ASSERT_NO_THROW(file_utils::fallback_copy(src, dst, src_st));
  const auto dst_stats = stat_file(dst);

  EXPECT_EQ(dst_stats.logical, kLogical);
  expect_segments_match(dst, segs);
  expect_zeros(dst, 1LL << 20, 4096);

  if (src_stats.physical < kLogical / 2) {
    EXPECT_LE(dst_stats.physical, src_stats.physical + (4LL << 20))
        << "fallback_copy bloated dst: src_physical=" << src_stats.physical
        << " dst_physical=" << dst_stats.physical;
    EXPECT_LT(dst_stats.physical, kLogical / 4)
        << "fallback_copy dst occupies >25% of its logical size";
  }
}

TEST_F(FallbackCopyTest, AllHoleFile_StaysEmpty) {
  const off_t kLogical = 16LL << 20;
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {}, kLogical));

  struct stat src_st {};
  ASSERT_EQ(::stat(src.c_str(), &src_st), 0);

  ASSERT_NO_THROW(file_utils::fallback_copy(src, dst, src_st));

  const auto dst_stats = stat_file(dst);
  EXPECT_EQ(dst_stats.logical, kLogical);
  EXPECT_LT(dst_stats.physical, 1LL << 20)
      << "fallback_copy materialized an all-hole file: physical="
      << dst_stats.physical;
}

TEST_F(FallbackCopyTest, EmptyFile_HandlesZeroSize) {
  const std::string src = path("src");
  const std::string dst = path("dst");
  ASSERT_NO_FATAL_FAILURE(make_file(src, {}));

  struct stat src_st {};
  ASSERT_EQ(::stat(src.c_str(), &src_st), 0);

  ASSERT_NO_THROW(file_utils::fallback_copy(src, dst, src_st));
  EXPECT_EQ(stat_file(dst).logical, 0);
}

}  // namespace test
}  // namespace neug
