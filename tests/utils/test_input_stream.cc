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

constexpr const char* INPUT_STREAM_TEST_DIR = "/tmp/input_stream_test";

class InputStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(INPUT_STREAM_TEST_DIR);
    std::filesystem::create_directories(INPUT_STREAM_TEST_DIR);
  }

  void TearDown() override {
    std::filesystem::remove_all(INPUT_STREAM_TEST_DIR);
  }

  std::string writeFile(const std::string& name, const std::string& content) {
    std::string path = std::string(INPUT_STREAM_TEST_DIR) + "/" + name;
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
      io::openLocalInputStream(std::string(INPUT_STREAM_TEST_DIR) + "/nope"),
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

}  // namespace
}  // namespace test
}  // namespace neug
