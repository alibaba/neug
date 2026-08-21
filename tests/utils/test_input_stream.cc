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

#include "neug/utils/io/stream/input_stream.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/csv/csv_read_config.h"

namespace neug {
namespace test {
namespace {

/// Unique per-process directory so parallel test binaries (or leftover
/// directories from other users) cannot interfere with this suite.
const std::string& inputStreamTestDir() {
  static const std::string dir =
      (std::filesystem::temp_directory_path() /
       ("neug_input_stream_test_" + std::to_string(::getpid())))
          .string();
  return dir;
}

class InputStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(inputStreamTestDir());
    std::filesystem::create_directories(inputStreamTestDir());
  }

  void TearDown() override {
    std::filesystem::remove_all(inputStreamTestDir());
  }

  std::string writeFile(const std::string& name, const std::string& content) {
    std::string path = std::string(inputStreamTestDir()) + "/" + name;
    std::ofstream out(path, std::ios::binary);
    out << content;
    return path;
  }

  static io::InputStreamOpener localOpener() {
    return
        [](const std::string& path) { return io::openLocalInputStream(path); };
  }
};

// =============== FileInputStream ===============

TEST_F(InputStreamTest, LocalReadSequentialAndEof) {
  auto path = writeFile("seq.txt", "hello world");
  auto stream = io::openLocalInputStream(path);

  auto size = stream->GetSize();
  ASSERT_TRUE(size.has_value());
  EXPECT_EQ(*size, 11);

  std::string buf(100, '\0');
  auto r1 = stream->Read(buf.data(), 5);
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(*r1, 5);
  EXPECT_EQ(buf.substr(0, 5), "hello");

  auto r2 = stream->Read(buf.data(), 100);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(*r2, 6);
  EXPECT_EQ(buf.substr(0, 6), " world");

  auto r3 = stream->Read(buf.data(), 100);
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(*r3, 0);
}

TEST_F(InputStreamTest, LocalReadAtDoesNotMovePosition) {
  auto path = writeFile("readat.txt", "hello world");
  auto stream = io::openLocalInputStream(path);

  std::string buf(100, '\0');
  auto r1 = stream->ReadAt(6, 100, buf.data());
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(*r1, 5);
  EXPECT_EQ(buf.substr(0, 5), "world");

  // ReadAt must not advance the logical position used by Read.
  auto r2 = stream->Read(buf.data(), 5);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(*r2, 5);
  EXPECT_EQ(buf.substr(0, 5), "hello");
}

TEST_F(InputStreamTest, LocalFileUriPrefix) {
  auto path = writeFile("uri.txt", "uri-content");
  auto stream = io::openLocalInputStream("file://" + path);

  std::string buf(64, '\0');
  auto r = stream->Read(buf.data(), 64);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(buf.substr(0, static_cast<size_t>(*r)), "uri-content");
}

TEST_F(InputStreamTest, LocalMissingFileThrows) {
  EXPECT_THROW(
      io::openLocalInputStream(std::string(inputStreamTestDir()) + "/nope"),
      exception::IOException);
}

// =============== bindInputStream / readFirstLine ===============

TEST_F(InputStreamTest, BindNullOpenerYieldsNullFactory) {
  EXPECT_FALSE(io::bindInputStream(nullptr, "/any/path"));
}

TEST_F(InputStreamTest, BindOpenerOpensBoundPath) {
  auto path = writeFile("bound.txt", "bound-content");
  auto factory = io::bindInputStream(localOpener(), path);
  ASSERT_TRUE(factory);

  auto stream = factory();
  std::string buf(64, '\0');
  auto r = stream->Read(buf.data(), 64);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(buf.substr(0, static_cast<size_t>(*r)), "bound-content");
}

TEST_F(InputStreamTest, ReadFirstLineVariants) {
  auto lf = writeFile("first_lf.txt", "first\nsecond\n");
  EXPECT_EQ(io::readFirstLine(io::bindInputStream(localOpener(), lf)), "first");

  auto crlf = writeFile("first_crlf.txt", "first\r\nsecond\r\n");
  EXPECT_EQ(io::readFirstLine(io::bindInputStream(localOpener(), crlf)),
            "first");

  auto noNewline = writeFile("first_no_nl.txt", "only-line");
  EXPECT_EQ(io::readFirstLine(io::bindInputStream(localOpener(), noNewline)),
            "only-line");

  // Longer than the internal 64KB read buffer.
  std::string longLine(100 * 1024, 'x');
  auto longPath = writeFile("first_long.txt", longLine);
  EXPECT_EQ(io::readFirstLine(io::bindInputStream(localOpener(), longPath)),
            longLine);
}

TEST_F(InputStreamTest, ReadFirstLineEmptyThrows) {
  auto empty = writeFile("first_empty.txt", "");
  EXPECT_THROW(io::readFirstLine(io::bindInputStream(localOpener(), empty)),
               exception::IOException);
  EXPECT_THROW(io::readFirstLine(nullptr), exception::IOException);
}

// =============== IoStreamBuf / makeIoStream ===============

TEST_F(InputStreamTest, IoStreamGetlineAcrossBufferBoundaries) {
  // ~2MB, well past the 1MB internal buffer, forces multiple fills.
  std::string content;
  constexpr int kLineCount = 70000;
  for (int i = 0; i < kLineCount; ++i) {
    content += "row-" + std::to_string(i) + "-padding-padding\n";
  }
  auto path = writeFile("stream_lines.txt", content);
  auto stream = io::makeIoStream(io::openLocalInputStream(path));

  std::string line;
  int count = 0;
  while (std::getline(*stream, line)) {
    EXPECT_EQ(line, "row-" + std::to_string(count) + "-padding-padding")
        << "mismatch at line " << count;
    ++count;
    if (count > kLineCount) {
      break;
    }
  }
  EXPECT_EQ(count, kLineCount);
}

TEST_F(InputStreamTest, IoStreamSeek) {
  auto path = writeFile("stream_seek.txt", "hello world");
  auto stream = io::makeIoStream(io::openLocalInputStream(path));

  stream->seekg(0, std::ios::end);
  EXPECT_EQ(stream->tellg(), 11);

  stream->seekg(6, std::ios::beg);
  std::string buf(16, '\0');
  stream->read(buf.data(), 16);
  EXPECT_EQ(stream->gcount(), 5);
  EXPECT_EQ(buf.substr(0, 5), "world");

  // Seek relative to the current position after a partial read.
  stream->clear();
  stream->seekg(0, std::ios::beg);
  stream->read(buf.data(), 5);
  stream->seekg(1, std::ios::cur);
  EXPECT_EQ(stream->tellg(), 6);
  stream->read(buf.data(), 5);
  EXPECT_EQ(buf.substr(0, 5), "world");
}

TEST_F(InputStreamTest, IoStreamLargeReadBypassesBuffer) {
  // Larger than the internal buffer, exercises the xsgetn direct path.
  std::string content(2 * (1 << 20) + 123, '\0');
  for (size_t i = 0; i < content.size(); ++i) {
    content[i] = static_cast<char>('a' + (i % 26));
  }
  auto path = writeFile("stream_large.bin", content);
  auto stream = io::makeIoStream(io::openLocalInputStream(path));

  std::string buf(content.size(), '\0');
  stream->read(buf.data(), static_cast<std::streamsize>(content.size()));
  EXPECT_EQ(stream->gcount(), static_cast<std::streamsize>(content.size()));
  EXPECT_EQ(buf, content);
}

// =============== read_header through a stream ===============

TEST_F(InputStreamTest, ReadHeaderThroughStreamMatchesLocal) {
  auto path = writeFile("header.csv", "id|name|score\n1|Alice|95.5\n");
  CsvReadConfig config;
  config.delimiter = '|';

  auto local = read_header(path, config);
  auto streamed =
      read_header(path, config, io::bindInputStream(localOpener(), path));

  EXPECT_EQ(streamed, (std::vector<std::string>{"id", "name", "score"}));
  EXPECT_EQ(streamed, local);
}

TEST_F(InputStreamTest, ReadHeaderStreamingWithEscaping) {
  // escaping=true routes through the manual header parser, which reads
  // the first line via readFirstLine on streams; escape chars inside
  // tokens are unescaped.
  auto path = writeFile("header_escape.csv", "id|na\\|me|score\n1|x|2\n");
  CsvReadConfig config;
  config.delimiter = '|';
  config.quoting = false;
  config.escaping = true;
  config.escape_char = '\\';

  auto local = read_header(path, config);
  auto streamed =
      read_header(path, config, io::bindInputStream(localOpener(), path));

  // Naive delimiter split is the existing semantics on both paths; the
  // escape char is consumed when unescaping each token.
  EXPECT_EQ(streamed, local);
}

// =============== IoStreamBuf error propagation ===============

/// Serves one valid read, then fails on every subsequent read — the
/// shape of a remote connection dropping mid-file. IO errors must
/// surface as exceptions, never be disguised as end of stream.
class PartialThenFailingInputStream : public io::InputStream {
 public:
  explicit PartialThenFailingInputStream(std::string data)
      : data_(std::move(data)) {}

  result<int64_t> Read(void* out, int64_t nbytes) override {
    auto r = ReadAt(position_, nbytes, out);
    if (r) {
      position_ += *r;
    }
    return r;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    if (failed_) {
      return tl::unexpected(neug::Status(neug::StatusCode::ERR_IO_ERROR,
                                         "simulated remote read failure"));
    }
    failed_ = true;  // any subsequent read fails
    const int64_t avail =
        std::max<int64_t>(0, static_cast<int64_t>(data_.size()) - position);
    const int64_t n = std::min(nbytes, avail);
    if (n > 0) {
      std::memcpy(out, data_.data() + position, static_cast<size_t>(n));
    }
    return n;
  }

  result<int64_t> GetSize() override {
    return static_cast<int64_t>(data_.size());
  }

  void Close() override {}

 private:
  std::string data_;
  int64_t position_ = 0;
  bool failed_ = false;
};

TEST_F(InputStreamTest, IoStreamReadFailureSurfacesAsException) {
  // The first read succeeds and lands in the internal buffer; refilling
  // then fails. getline must throw instead of silently stopping.
  auto stream = io::makeIoStream(
      std::make_unique<PartialThenFailingInputStream>("line1\nline2\n"));
  std::string line;
  ASSERT_NO_THROW(std::getline(*stream, line));
  EXPECT_EQ(line, "line1");
  ASSERT_NO_THROW(std::getline(*stream, line));
  EXPECT_EQ(line, "line2");
  // Buffer is exhausted; the next fill hits the simulated failure.
  EXPECT_THROW(std::getline(*stream, line), exception::IOException);
}

TEST_F(InputStreamTest, IoStreamLargeReadFailureSurfacesAsException) {
  // 3 MiB of data with a 2 MiB request exercises the xsgetn direct path
  // (bypassing the 1 MiB internal buffer); the follow-up read fails.
  const std::string data(3u << 20, 'x');
  auto stream =
      io::makeIoStream(std::make_unique<PartialThenFailingInputStream>(data));
  std::vector<char> buf(2u << 20);
  ASSERT_NO_THROW(
      stream->read(buf.data(), static_cast<std::streamsize>(buf.size())));
  EXPECT_FALSE(stream->fail());
  EXPECT_EQ(stream->gcount(), static_cast<std::streamsize>(buf.size()));
  EXPECT_THROW(
      stream->read(buf.data(), static_cast<std::streamsize>(buf.size())),
      exception::IOException);
}

}  // namespace
}  // namespace test
}  // namespace neug
