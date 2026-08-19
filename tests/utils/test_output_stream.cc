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

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/write/writer.h"

namespace neug {
namespace test {
namespace {

/// Unique per-process directory so parallel test binaries (or leftover
/// directories from other users) cannot interfere with this suite.
const std::string& outputStreamTestDir() {
  static const std::string dir =
      (std::filesystem::temp_directory_path() /
       ("neug_output_stream_test_" + std::to_string(::getpid())))
          .string();
  return dir;
}

class OutputStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(outputStreamTestDir());
    std::filesystem::create_directories(outputStreamTestDir());
  }

  void TearDown() override {
    std::filesystem::remove_all(outputStreamTestDir());
  }

  std::string pathOf(const std::string& name) const {
    return std::string(outputStreamTestDir()) + "/" + name;
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

// =============== CsvQueryExportWriter stream opening ===============

/// Where a RecordingOutputStream reports to; outlives the stream itself,
/// which is destroyed when writeTable() returns.
struct FinalizeRecord {
  std::string content;
  bool closed = false;
  bool aborted = false;
};

/// An io::OutputStream that records how it was finalized, so tests can
/// distinguish between publishing (Close) and discarding (Abort).
class RecordingOutputStream : public io::OutputStream {
 public:
  explicit RecordingOutputStream(FinalizeRecord* record) : record_(record) {}

  neug::Status Write(const uint8_t* data, int64_t nbytes) override {
    if (nbytes > 0) {
      record_->content.append(reinterpret_cast<const char*>(data),
                              static_cast<size_t>(nbytes));
    }
    return neug::Status::OK();
  }

  neug::Status Close() override {
    record_->closed = true;
    return neug::Status::OK();
  }

  void Abort() override { record_->aborted = true; }

 private:
  FinalizeRecord* record_;
};

class CsvExportWriterTest : public OutputStreamTest {
 protected:
  static reader::FileSchema makeSchema(const std::string& path) {
    reader::FileSchema schema;
    schema.paths = {path};
    schema.format = "csv";
    return schema;
  }

  static std::shared_ptr<reader::EntrySchema> makeEntrySchema() {
    auto entry = std::make_shared<reader::TableEntrySchema>();
    entry->columnNames = {"id"};
    return entry;
  }
};

// The output stream must be opened lazily — only once validation passed
// and the query results are sunk — never up front (which would truncate
// the target even for a failed export).
TEST_F(CsvExportWriterTest, StreamOpenerInvokedAfterValidation) {
  bool opener_invoked = false;
  FinalizeRecord record;
  // The writer keeps its schema by reference, so it must outlive the
  // writer.
  auto schema = makeSchema(pathOf("deferred.csv"));

  writer::CsvQueryExportWriter csvWriter(schema, makeEntrySchema());
  csvWriter.setStreamOpener([&opener_invoked, &record]() {
    opener_invoked = true;
    return std::make_unique<RecordingOutputStream>(&record);
  });

  EXPECT_FALSE(opener_invoked);
  neug::QueryResponse response;  // zero rows
  auto status = csvWriter.writeTable(&response);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(opener_invoked);
  EXPECT_TRUE(record.closed);
  EXPECT_FALSE(record.aborted);
  EXPECT_EQ(record.content, "id\n");
}

// A failing validation must never invoke the opener (and thus never
// open or truncate the target).
TEST_F(CsvExportWriterTest, FailingValidationSkipsOpener) {
  bool opener_invoked = false;
  auto path = pathOf("never_opened.csv");
  auto schema = makeSchema(path);

  writer::CsvQueryExportWriter csvWriter(schema, nullptr);
  csvWriter.setStreamOpener(
      [&opener_invoked]() -> std::unique_ptr<io::OutputStream> {
        opener_invoked = true;
        return nullptr;
      });

  neug::QueryResponse response;
  auto status = csvWriter.writeTable(&response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
  EXPECT_FALSE(opener_invoked);
  EXPECT_FALSE(std::filesystem::exists(path));
}

// An opener failure surfaces as an IO error status, and the target is
// left untouched.
TEST_F(CsvExportWriterTest, FailingOpenerReturnsErrorStatus) {
  auto path = pathOf("opener_fails.csv");
  auto schema = makeSchema(path);

  writer::CsvQueryExportWriter csvWriter(schema, makeEntrySchema());
  csvWriter.setStreamOpener([]() -> std::unique_ptr<io::OutputStream> {
    THROW_IO_EXCEPTION("simulated remote open failure");
  });

  neug::QueryResponse response;
  auto status = csvWriter.writeTable(&response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_IO_ERROR);
  EXPECT_FALSE(std::filesystem::exists(path));
}

}  // namespace
}  // namespace test
}  // namespace neug
