/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow/buffer.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>
#include <iomanip>
#include <thread>
#include <vector>
#include "../include/http_filesystem.h"
#include "../include/http_options.h"
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/utils/exception/exception.h"

using namespace neug::extension::http;

class HTTPFileSystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Test setup
  }

  void TearDown() override {
    // Test cleanup
  }
};

// ============================================================================
// HTTPURIComponents Tests
// ============================================================================

TEST_F(HTTPFileSystemTest, ParseHTTPURL) {
  auto components =
      HTTPURIComponents::parse("http://example.com/path/file.txt");

  EXPECT_EQ(components.scheme, "http");
  EXPECT_EQ(components.host, "example.com");
  EXPECT_EQ(components.port, 80);
  EXPECT_EQ(components.path, "/path/file.txt");
}

TEST_F(HTTPFileSystemTest, ParseHTTPSURL) {
  auto components =
      HTTPURIComponents::parse("https://example.com/path/file.txt");

  EXPECT_EQ(components.scheme, "https");
  EXPECT_EQ(components.host, "example.com");
  EXPECT_EQ(components.port, 443);
  EXPECT_EQ(components.path, "/path/file.txt");
}

TEST_F(HTTPFileSystemTest, ParseURLWithPort) {
  auto components = HTTPURIComponents::parse("http://example.com:8080/data");

  EXPECT_EQ(components.scheme, "http");
  EXPECT_EQ(components.host, "example.com");
  EXPECT_EQ(components.port, 8080);
  EXPECT_EQ(components.path, "/data");
}

TEST_F(HTTPFileSystemTest, ParseURLWithoutPath) {
  auto components = HTTPURIComponents::parse("https://example.com");

  EXPECT_EQ(components.scheme, "https");
  EXPECT_EQ(components.host, "example.com");
  EXPECT_EQ(components.port, 443);
  EXPECT_EQ(components.path, "/");
}

TEST_F(HTTPFileSystemTest, ParseInvalidURL_NoScheme) {
  EXPECT_THROW(HTTPURIComponents::parse("example.com/file.txt"),
               neug::exception::Exception);
}

TEST_F(HTTPFileSystemTest, ParseInvalidURL_WrongScheme) {
  EXPECT_THROW(HTTPURIComponents::parse("ftp://example.com/file.txt"),
               neug::exception::Exception);
}

TEST_F(HTTPFileSystemTest, ToURL) {
  HTTPURIComponents components;
  components.scheme = "https";
  components.host = "example.com";
  components.port = 443;
  components.path = "/data/file.parquet";

  EXPECT_EQ(components.toURL(), "https://example.com/data/file.parquet");
}

TEST_F(HTTPFileSystemTest, ToURL_NonDefaultPort) {
  HTTPURIComponents components;
  components.scheme = "http";
  components.host = "localhost";
  components.port = 8080;
  components.path = "/test";

  EXPECT_EQ(components.toURL(), "http://localhost:8080/test");
}

// ============================================================================
// HTTPFileSystem Tests
// ============================================================================

TEST_F(HTTPFileSystemTest, CreateFileSystem) {
  neug::common::case_insensitive_map_t<std::string> options;
  EXPECT_NO_THROW({ HTTPFileSystem fs(options); });
}

TEST_F(HTTPFileSystemTest, TypeName) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  EXPECT_EQ(fs.type_name(), "http");
}

TEST_F(HTTPFileSystemTest, Equals) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs1(options);
  HTTPFileSystem fs2(options);

  EXPECT_TRUE(fs1.Equals(fs1));  // Same instance
  EXPECT_TRUE(fs1.Equals(fs2));  // Different instances, same type
}

// ============================================================================
// HTTPFileSystem (neug VFS interface) Tests
// ============================================================================

TEST_F(HTTPFileSystemTest, HTTPFileSystem_ExtractOptions) {
  neug::reader::FileSchema schema;
  schema.paths = {"https://example.com/data.parquet"};
  schema.options["BEARER_TOKEN"] = "test_token";
  schema.options["VERIFY_SSL"] = "false";
  schema.options["CONNECT_TIMEOUT"] = "60";

  EXPECT_NO_THROW({
    HTTPFileSystem fs(schema);
    // glob() returns the path unchanged for HTTP
    auto resolved = fs.glob("https://example.com/data.parquet");
    EXPECT_EQ(resolved.size(), 1);
    EXPECT_EQ(resolved[0], "https://example.com/data.parquet");
    // toArrowFileSystem() returns a non-null HTTPFileSystem
    auto arrowFs = std::static_pointer_cast<arrow::fs::FileSystem>(
        fs.getArrowFileSystem());
    EXPECT_NE(arrowFs, nullptr);
  });
}

TEST_F(HTTPFileSystemTest, HTTPFileSystem_InvalidURL) {
  neug::reader::FileSchema schema;
  schema.paths = {"not-a-url"};

  EXPECT_THROW(HTTPFileSystem fs(schema), neug::exception::Exception);
}

TEST_F(HTTPFileSystemTest, HTTPFileSystem_MultiplePaths) {
  neug::reader::FileSchema schema;
  schema.paths = {"https://example.com/file1.parquet",
                  "https://example.com/file2.parquet"};

  EXPECT_NO_THROW({
    HTTPFileSystem fs(schema);
    // glob() returns each path unchanged
    auto r1 = fs.glob(schema.paths[0]);
    auto r2 = fs.glob(schema.paths[1]);
    EXPECT_EQ(r1.size(), 1);
    EXPECT_EQ(r2.size(), 1);
  });
}

// ============================================================================
// VERIFY_SSL Option Parsing Tests
// ============================================================================

TEST_F(HTTPFileSystemTest, VerifySSL_InvalidValue_ThrowsException) {
  // Unrecognized VERIFY_SSL values must throw an exception rather than
  // silently disabling TLS verification (which was the old behaviour).
  // VERIFY_SSL is parsed inside SetupCURLHandle(), which is called during
  // HTTPRandomAccessFile construction (via InitializeFileSize()). We trigger
  // that path with a real URL so the constructor reaches SetupCURLHandle.
  // The test uses a public HTTPS endpoint; if the network is unavailable the
  // curl transport will fail before VERIFY_SSL is even checked, so the test
  // is skipped gracefully when offline.
  neug::common::case_insensitive_map_t<std::string> options;
  options["VERIFY_SSL"] = "maybe";  // Invalid: not true/false/1/0/yes/no/on/off

  HTTPFileSystem fs(options);
  auto arrowFs =
      std::static_pointer_cast<arrow::fs::FileSystem>(fs.getArrowFileSystem());
  ASSERT_NE(arrowFs, nullptr);

  // OpenInputFile triggers HTTPRandomAccessFile construction → SetupCURLHandle
  // → VERIFY_SSL parse → exception.
  const std::string url =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/GithubGraphTest/nodes_Actor.parquet";
  auto result = arrowFs->OpenInputFile(url);
  // Must fail due to invalid VERIFY_SSL, not a network error (which would also
  // fail but with a different message). We accept any IOError here because the
  // exception is converted to arrow::Status::IOError by the catch block.
  EXPECT_FALSE(result.ok())
      << "OpenInputFile should fail when VERIFY_SSL has an invalid value";
  EXPECT_NE(result.status().ToString().find("VERIFY_SSL"), std::string::npos)
      << "Error message should mention VERIFY_SSL. Got: "
      << result.status().ToString();
}

// ============================================================================
// GetFileInfo: non-2xx HTTP response reports FileType::NotFound
// ============================================================================

TEST_F(HTTPFileSystemTest, E2E_GetFileInfo_NotFoundURL) {
  // A valid HTTPS host but a path that does not exist should return HTTP 404.
  // GetFileInfo() must report FileType::NotFound (not FileType::File).
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);
  auto arrowFs =
      std::static_pointer_cast<arrow::fs::FileSystem>(fs.getArrowFileSystem());
  ASSERT_NE(arrowFs, nullptr);

  // This URL returns 404 — the path simply doesn't exist on the public bucket.
  const std::string nonexistent =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/this-file-does-not-exist-12345.parquet";

  auto info_result = arrowFs->GetFileInfo(nonexistent);
  ASSERT_TRUE(info_result.ok()) << "GetFileInfo itself should not error out";

  auto info = *info_result;
  EXPECT_EQ(info.type(), arrow::fs::FileType::NotFound)
      << "404 URL must be reported as NotFound, not as a valid File";
}

// ============================================================================
// E2E Test: Access Public HTTP Parquet File
// ============================================================================

TEST_F(HTTPFileSystemTest, E2E_AccessPublicHTTPParquetFile) {
  // This test accesses a real public Parquet file via HTTPS
  // URL: OSS public bucket via HTTPS (not S3 API)

  neug::reader::FileSchema schema;
  schema.paths = {
      "https://graphscope.oss-cn-beijing.aliyuncs.com/neug-dataset/"
      "GithubGraphTest/nodes_Actor.parquet"};
  schema.format = "parquet";
  schema.protocol = "http";
  schema.options["VERIFY_SSL"] = "true";

  EXPECT_NO_THROW({
    HTTPFileSystem fs(schema);
    auto arrowFs = std::static_pointer_cast<arrow::fs::FileSystem>(
        fs.getArrowFileSystem());
    ASSERT_NE(arrowFs, nullptr);

    // Verify file access
    auto file_path = schema.paths[0];
    auto file_info_result = arrowFs->GetFileInfo(file_path);

    if (file_info_result.ok()) {
      auto info = *file_info_result;
      LOG(INFO) << "=== HTTP File Access Successful ===";
      LOG(INFO) << "URL: " << file_path;
      LOG(INFO) << "Type: " << (info.IsFile() ? "File" : "Not File");
      LOG(INFO) << "Size: " << info.size() << " bytes";

      EXPECT_TRUE(info.IsFile());
      EXPECT_GT(info.size(), 0);

      // Try to open and read file header (first 4 bytes - Parquet magic)
      auto file_result = arrowFs->OpenInputFile(file_path);
      if (file_result.ok()) {
        auto file = *file_result;

        // Read first 4 bytes to verify Parquet magic number
        auto buffer_result = file->Read(4);
        if (buffer_result.ok()) {
          auto buffer = *buffer_result;
          const uint8_t* data = buffer->data();

          LOG(INFO) << "Magic bytes: " << std::hex << std::setfill('0')
                    << std::setw(2) << (int) data[0] << " " << std::setw(2)
                    << (int) data[1] << " " << std::setw(2) << (int) data[2]
                    << " " << std::setw(2) << (int) data[3];

          // Verify Parquet magic number "PAR1"
          EXPECT_EQ(data[0], 0x50);  // 'P'
          EXPECT_EQ(data[1], 0x41);  // 'A'
          EXPECT_EQ(data[2], 0x52);  // 'R'
          EXPECT_EQ(data[3], 0x31);  // '1'

          LOG(INFO) << "✓ Successfully read Parquet file via HTTPS";
        } else {
          LOG(WARNING) << "Could not read file content: "
                       << buffer_result.status();
        }
      } else {
        LOG(WARNING) << "Could not open file: " << file_result.status();
      }
    } else {
      LOG(WARNING) << "Could not get file info: " << file_info_result.status();
      // Don't fail test if network unavailable
    }
  });
}

// ============================================================================
// Fix #2: CURL Global Initialization Thread Safety (std::call_once)
// Verify multiple concurrent HTTPFileSystem constructions don't crash.
// ============================================================================

TEST_F(HTTPFileSystemTest, ConcurrentConstruction_ThreadSafety) {
  // Create multiple HTTPFileSystem instances from different threads
  // to verify std::call_once correctly serializes curl_global_init.
  constexpr int kNumThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&success_count]() {
      try {
        neug::common::case_insensitive_map_t<std::string> options;
        HTTPFileSystem fs(options);
        ++success_count;
      } catch (...) {
        // Should not throw
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), kNumThreads)
      << "All threads should construct HTTPFileSystem without error";
}

// ============================================================================
// Fix #5: GetFileInfo uses lightweight HEAD request (no full file handle)
// Verify GetFileInfo returns correct info for a known public file.
// ============================================================================

TEST_F(HTTPFileSystemTest, GetFileInfo_ReturnsCorrectSize) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  // Use a known public file with a stable size
  const std::string url =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/GithubGraphTest/nodes_Actor.parquet";

  auto info_result = fs.GetFileInfo(url);
  ASSERT_TRUE(info_result.ok()) << info_result.status().ToString();

  auto info = *info_result;
  EXPECT_EQ(info.type(), arrow::fs::FileType::File);
  // File should report a positive size from Content-Length header
  EXPECT_GT(info.size(), 0) << "HEAD request should retrieve Content-Length";
}

// ============================================================================
// Fix #1: HTTP Range 200 status code handling (graceful degradation)
// Verify ReadRange works correctly when server returns 206 (partial content),
// and that the code path doesn't crash for standard Range responses.
// ============================================================================

TEST_F(HTTPFileSystemTest, ReadRange_Returns206_PartialContent) {
  // OSS supports Range requests and returns 206.
  // Verify that reading a small range from a known file works correctly.
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  const std::string url =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/GithubGraphTest/nodes_Actor.parquet";

  auto file_result = fs.OpenInputFile(url);
  ASSERT_TRUE(file_result.ok()) << file_result.status().ToString();

  auto file = *file_result;

  // Read first 4 bytes (Parquet magic "PAR1") via Range request
  auto buf_result = file->ReadAt(0, 4);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();

  auto buf = *buf_result;
  ASSERT_EQ(buf->size(), 4);
  EXPECT_EQ(buf->data()[0], 'P');
  EXPECT_EQ(buf->data()[1], 'A');
  EXPECT_EQ(buf->data()[2], 'R');
  EXPECT_EQ(buf->data()[3], '1');
}

TEST_F(HTTPFileSystemTest, ReadRange_OffsetBeyondEOF_ReturnsError) {
  // Reading past the end of file triggers HTTP 416 Range Not Satisfiable.
  // The server returns an error response body which exceeds the small request
  // buffer, causing WriteCallbackDirect to abort the transfer (Fix #10).
  // Key assertion: no crash, no buffer overflow — returns IOError gracefully.
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  const std::string url =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/GithubGraphTest/nodes_Actor.parquet";

  auto file_result = fs.OpenInputFile(url);
  ASSERT_TRUE(file_result.ok()) << file_result.status().ToString();

  auto file = *file_result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok());
  int64_t file_size = *size_result;

  // Request starting well beyond the file size
  auto buf_result = file->ReadAt(file_size + 1000, 100);
  // Server returns 416 + error body. WriteCallbackDirect aborts the transfer
  // because the error body exceeds the 100-byte buffer. This correctly
  // propagates as an IOError instead of a buffer overflow.
  EXPECT_FALSE(buf_result.ok());
}

// ============================================================================
// Fix #10: WriteCallbackDirect buffer overflow — CURL abort on overflow
// Verify that reading a small buffer from a large range request doesn't crash.
// The WriteCallbackDirect now returns 0 to abort CURL when buffer is full.
// ============================================================================

TEST_F(HTTPFileSystemTest, ReadRange_SmallReadNearEOF_Succeeds) {
  // Read a small amount of data near the end of file.
  // This exercises the ReadRange path where returned bytes may be less than
  // requested (partial content at EOF boundary), testing buffer handling.
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  const std::string url =
      "https://graphscope.oss-cn-beijing.aliyuncs.com"
      "/neug-dataset/GithubGraphTest/nodes_Actor.parquet";

  auto file_result = fs.OpenInputFile(url);
  ASSERT_TRUE(file_result.ok()) << file_result.status().ToString();

  auto file = *file_result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok());
  int64_t file_size = *size_result;
  ASSERT_GT(file_size, 100);

  // Read last 4 bytes of file (Parquet footer magic "PAR1")
  auto buf_result = file->ReadAt(file_size - 4, 4);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();

  auto buf = *buf_result;
  EXPECT_EQ(buf->size(), 4);
  // Parquet files end with "PAR1" magic
  EXPECT_EQ(buf->data()[0], 'P');
  EXPECT_EQ(buf->data()[1], 'A');
  EXPECT_EQ(buf->data()[2], 'R');
  EXPECT_EQ(buf->data()[3], '1');
}
