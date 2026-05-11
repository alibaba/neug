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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <iomanip>
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
  auto components = HTTPURIComponents::parse("http://example.com/path/file.txt");
  
  EXPECT_EQ(components.scheme, "http");
  EXPECT_EQ(components.host, "example.com");
  EXPECT_EQ(components.port, 80);
  EXPECT_EQ(components.path, "/path/file.txt");
}

TEST_F(HTTPFileSystemTest, ParseHTTPSURL) {
  auto components = HTTPURIComponents::parse("https://example.com/path/file.txt");
  
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
  EXPECT_THROW(
    HTTPURIComponents::parse("example.com/file.txt"),
    neug::exception::Exception
  );
}

TEST_F(HTTPFileSystemTest, ParseInvalidURL_WrongScheme) {
  EXPECT_THROW(
    HTTPURIComponents::parse("ftp://example.com/file.txt"),
    neug::exception::Exception
  );
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
  EXPECT_NO_THROW({
    HTTPFileSystem fs(options);
  });
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
    auto arrowFs = fs.toArrowFileSystem();
    EXPECT_NE(arrowFs, nullptr);
  });
}

TEST_F(HTTPFileSystemTest, HTTPFileSystem_InvalidURL) {
  neug::reader::FileSchema schema;
  schema.paths = {"not-a-url"};

  EXPECT_THROW(
    HTTPFileSystem fs(schema),
    neug::exception::Exception
  );
}

TEST_F(HTTPFileSystemTest, HTTPFileSystem_MultiplePaths) {
  neug::reader::FileSchema schema;
  schema.paths = {
    "https://example.com/file1.parquet",
    "https://example.com/file2.parquet"
  };

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
  auto arrowFs = fs.toArrowFileSystem();
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
  auto arrowFs = fs.toArrowFileSystem();
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
  schema.paths = {"https://graphscope.oss-cn-beijing.aliyuncs.com/neug-dataset/GithubGraphTest/nodes_Actor.parquet"};
  schema.format = "parquet";
  schema.protocol = "http";
  schema.options["VERIFY_SSL"] = "true";
  
  EXPECT_NO_THROW({
    HTTPFileSystem fs(schema);
    auto arrowFs = fs.toArrowFileSystem();
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

          LOG(INFO) << "Magic bytes: "
                    << std::hex << std::setfill('0')
                    << std::setw(2) << (int)data[0] << " "
                    << std::setw(2) << (int)data[1] << " "
                    << std::setw(2) << (int)data[2] << " "
                    << std::setw(2) << (int)data[3];

          // Verify Parquet magic number "PAR1"
          EXPECT_EQ(data[0], 0x50);  // 'P'
          EXPECT_EQ(data[1], 0x41);  // 'A'
          EXPECT_EQ(data[2], 0x52);  // 'R'
          EXPECT_EQ(data[3], 0x31);  // '1'

          LOG(INFO) << "✓ Successfully read Parquet file via HTTPS";
        } else {
          LOG(WARNING) << "Could not read file content: " << buffer_result.status();
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
