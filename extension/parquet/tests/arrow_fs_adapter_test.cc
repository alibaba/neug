/*
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <gtest/gtest.h>
#include <cstring>
#include <memory>
#include <string>

#include "neug/utils/io/vfs/file_system.h"
#include "parquet/arrow_fs_adapter.h"

namespace neug {
namespace test {

// ============================================================================
// Remote stream adapters (arrow_fs_adapter)
// ============================================================================

/// In-memory remote stream. Read() maintains its own cursor starting at 0
/// (like the real HTTP/S3 streams), which Seek() on the adapter does not
/// touch — so a broken adapter that serves sequential Read() from the
/// stream cursor after Seek() returns wrong bytes and fails these tests.
class FakeRemoteStream : public fsys::RandomAccessStream {
 public:
  explicit FakeRemoteStream(std::string data) : data_(std::move(data)) {}

  result<int64_t> Read(int64_t nbytes, void* out) override {
    auto r = ReadAt(cursor_, nbytes, out);
    if (r) {
      cursor_ += *r;
    }
    return r;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    const int64_t size = static_cast<int64_t>(data_.size());
    if (position >= size) {
      return int64_t{0};
    }
    const int64_t n = std::min(nbytes, size - position);
    std::memcpy(out, data_.data() + position, static_cast<size_t>(n));
    return n;
  }

  result<int64_t> GetSize() override {
    return static_cast<int64_t>(data_.size());
  }

  void Close() override {}

 private:
  std::string data_;
  int64_t cursor_ = 0;
};

// After Seek(), Read() must serve bytes at the adapter position (via
// ReadAt on the remote stream), not from the remote stream's own cursor.
TEST(ParquetAdapterTest, SeekThenReadRespectsPosition) {
  auto fake = std::make_shared<FakeRemoteStream>("0123456789");
  neug::parquet::StreamRandomAccessFile file(fake);

  ASSERT_TRUE(file.Seek(4).ok());
  char buf[4];
  auto r = file.Read(4, buf);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(*r, 4);
  EXPECT_EQ(std::string(buf, 4), "4567");

  // Sequential reads continue from the adapter position.
  r = file.Read(2, buf);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(std::string(buf, 2), "89");
}

TEST(ParquetAdapterTest, SeekThenReadBufferOverloadRespectsPosition) {
  auto fake = std::make_shared<FakeRemoteStream>("abcdefghij");
  neug::parquet::StreamRandomAccessFile file(fake);

  ASSERT_TRUE(file.Seek(7).ok());
  auto r = file.Read(5);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  ASSERT_EQ((*r)->size(), 3);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>((*r)->data()), (*r)->size()),
      "hij");
}

/// Remote output stream that records how it was finalized and can be
/// told to fail its next write.
class RecordingRemoteOutputStream : public fsys::OutputStream {
 public:
  result<void> Write(const void* data, int64_t nbytes) override {
    if (fail_next_write_) {
      fail_next_write_ = false;
      return tl::unexpected(
          Status(StatusCode::ERR_IO_ERROR, "simulated remote write failure"));
    }
    bytes_written_ += nbytes;
    return {};
  }

  result<void> Close() override {
    closed_ = true;
    return {};
  }

  result<void> Abort() override {
    aborted_ = true;
    return {};
  }

  void failNextWrite() { fail_next_write_ = true; }

  int64_t bytes_written_ = 0;
  bool closed_ = false;
  bool aborted_ = false;

 private:
  bool fail_next_write_ = false;
};

// After a write failure the adapter must abort the underlying stream
// (discarding the partial remote object), never finalize it.
TEST(ParquetAdapterTest, OutputAbortsInsteadOfPublishingPartialObject) {
  auto fake = std::make_shared<RecordingRemoteOutputStream>();
  fake->failNextWrite();
  {
    neug::parquet::StreamOutputStream out(fake);
    const char payload[] = "data";
    ASSERT_FALSE(out.Write(payload, 4).ok());
  }
  EXPECT_TRUE(fake->aborted_);
  EXPECT_FALSE(fake->closed_);
}

// Without failures the destructor finalizes normally.
TEST(ParquetAdapterTest, OutputClosesOnSuccessWhenNotExplicitlyClosed) {
  auto fake = std::make_shared<RecordingRemoteOutputStream>();
  {
    neug::parquet::StreamOutputStream out(fake);
    const char payload[] = "data";
    ASSERT_TRUE(out.Write(payload, 4).ok());
  }
  EXPECT_TRUE(fake->closed_);
  EXPECT_FALSE(fake->aborted_);
}

// An explicit Close() after a write failure must also abort instead of
// finalizing the underlying stream (not just the destructor path).
TEST(ParquetAdapterTest, ExplicitCloseAfterWriteFailureAborts) {
  auto fake = std::make_shared<RecordingRemoteOutputStream>();
  fake->failNextWrite();
  neug::parquet::StreamOutputStream out(fake);
  const char payload[] = "data";
  ASSERT_FALSE(out.Write(payload, 4).ok());

  auto status = out.Close();
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(fake->aborted_);
  EXPECT_FALSE(fake->closed_);

  // A second Close() is a no-op and must not touch the stream again.
  EXPECT_TRUE(out.Close().ok());
  EXPECT_TRUE(out.closed());
}

}  // namespace test
}  // namespace neug
