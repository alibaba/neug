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

#include "neug/utils/io/stream/output_stream.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace test {
namespace {

constexpr const char* OUTPUT_STREAM_TEST_DIR = "/tmp/output_stream_test";

class OutputStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(OUTPUT_STREAM_TEST_DIR);
    std::filesystem::create_directories(OUTPUT_STREAM_TEST_DIR);
  }

  void TearDown() override {
    std::filesystem::remove_all(OUTPUT_STREAM_TEST_DIR);
  }

  std::string pathOf(const std::string& name) const {
    return std::string(OUTPUT_STREAM_TEST_DIR) + "/" + name;
  }

  std::string readBack(const std::string& path) const {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
  }
};

TEST_F(OutputStreamTest, WriteAndClose) {
  auto path = pathOf("basic.bin");
  auto stream = io::openLocalOutputStream(path);

  const std::string first = "hello ";
  const std::string second = "world";
  ASSERT_TRUE(stream
                  ->Write(reinterpret_cast<const uint8_t*>(first.data()),
                          static_cast<int64_t>(first.size()))
                  .ok());
  ASSERT_TRUE(stream
                  ->Write(reinterpret_cast<const uint8_t*>(second.data()),
                          static_cast<int64_t>(second.size()))
                  .ok());
  ASSERT_TRUE(stream->Close().ok());

  EXPECT_EQ(readBack(path), "hello world");
}

TEST_F(OutputStreamTest, ZeroAndNegativeLengthAreNoOps) {
  auto path = pathOf("noop.bin");
  auto stream = io::openLocalOutputStream(path);

  ASSERT_TRUE(stream->Write(nullptr, 0).ok());
  ASSERT_TRUE(stream->Write(reinterpret_cast<const uint8_t*>("x"), -1).ok());
  ASSERT_TRUE(stream->Close().ok());

  EXPECT_EQ(readBack(path), "");
}

TEST_F(OutputStreamTest, WriteBinaryContent) {
  auto path = pathOf("binary.bin");
  std::string content;
  for (int i = 0; i < 256; ++i) {
    content.push_back(static_cast<char>(i));
  }

  auto stream = io::openLocalOutputStream(path);
  ASSERT_TRUE(stream
                  ->Write(reinterpret_cast<const uint8_t*>(content.data()),
                          static_cast<int64_t>(content.size()))
                  .ok());
  ASSERT_TRUE(stream->Close().ok());

  EXPECT_EQ(readBack(path), content);
}

TEST_F(OutputStreamTest, ReopenTruncatesExistingFile) {
  auto path = pathOf("trunc.bin");
  {
    auto stream = io::openLocalOutputStream(path);
    const std::string content = "a-longer-existing-content";
    ASSERT_TRUE(stream
                    ->Write(reinterpret_cast<const uint8_t*>(content.data()),
                            static_cast<int64_t>(content.size()))
                    .ok());
    ASSERT_TRUE(stream->Close().ok());
  }
  {
    auto stream = io::openLocalOutputStream(path);
    const std::string content = "short";
    ASSERT_TRUE(stream
                    ->Write(reinterpret_cast<const uint8_t*>(content.data()),
                            static_cast<int64_t>(content.size()))
                    .ok());
    ASSERT_TRUE(stream->Close().ok());
  }

  EXPECT_EQ(readBack(path), "short");
}

TEST_F(OutputStreamTest, FileUriPrefix) {
  auto path = pathOf("uri.bin");
  auto stream = io::openLocalOutputStream("file://" + path);
  const std::string content = "uri-content";
  ASSERT_TRUE(stream
                  ->Write(reinterpret_cast<const uint8_t*>(content.data()),
                          static_cast<int64_t>(content.size()))
                  .ok());
  ASSERT_TRUE(stream->Close().ok());

  EXPECT_EQ(readBack(path), "uri-content");
}

TEST_F(OutputStreamTest, OpenMissingDirectoryThrows) {
  EXPECT_THROW(io::openLocalOutputStream(pathOf("no_such_dir/file.bin")),
               exception::IOException);
}

}  // namespace
}  // namespace test
}  // namespace neug
