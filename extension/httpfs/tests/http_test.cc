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
#include <atomic>
#include <cstdlib>
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
// HTTPFileSystem (neug VFS interface) Tests
// ============================================================================

TEST_F(HTTPFileSystemTest, CreateFileSystem) {
  neug::common::case_insensitive_map_t<std::string> options;
  EXPECT_NO_THROW({ HTTPFileSystem fs(options); });
}

TEST_F(HTTPFileSystemTest, Glob_ReturnsPathUnchanged) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  auto resolved = fs.glob("https://example.com/data.parquet");
  ASSERT_EQ(resolved.size(), 1);
  EXPECT_EQ(resolved[0], "https://example.com/data.parquet");
}

TEST_F(HTTPFileSystemTest, RemoteFileSystem_IsNonNull) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);
}

TEST_F(HTTPFileSystemTest, RemoteFileSystem_OpenOutputStream_NotSupported) {
  // HTTP is read-only: openOutputStream must fail with ERR_NOT_SUPPORTED
  // without touching the network.
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);

  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  auto result = remote->openOutputStream("https://example.com/data.parquet");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_NOT_SUPPORTED);
}

TEST_F(HTTPFileSystemTest, HTTPFileSystem_ExtractOptions) {
  neug::reader::FileSchema schema;
  schema.paths = {"https://example.com/data.parquet"};
  schema.options["BEARER_TOKEN"] = "test_token";
  schema.options["VERIFY_SSL"] = "false";
  schema.options["CONNECT_TIMEOUT"] = "60";

  EXPECT_NO_THROW({
    HTTPFileSystem fs(schema);
    auto resolved = fs.glob("https://example.com/data.parquet");
    EXPECT_EQ(resolved.size(), 1);
    EXPECT_EQ(resolved[0], "https://example.com/data.parquet");
    EXPECT_NE(fs.getRemoteFileSystem(), nullptr);
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
    auto r1 = fs.glob(schema.paths[0]);
    auto r2 = fs.glob(schema.paths[1]);
    EXPECT_EQ(r1.size(), 1);
    EXPECT_EQ(r2.size(), 1);
  });
}

// ============================================================================
// VERIFY_SSL option validation (offline — parsing fails before any I/O)
// ============================================================================

TEST_F(HTTPFileSystemTest, VerifySSL_InvalidValue_ThrowsException) {
  // Unrecognized VERIFY_SSL values must throw an exception rather than
  // silently disabling TLS verification. The option is parsed during stream
  // construction, before any network activity, so this test works offline.
  neug::common::case_insensitive_map_t<std::string> options;
  options["VERIFY_SSL"] = "maybe";  // Invalid: not true/false/1/0/yes/no/on/off

  HTTPFileSystem fs(options);
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  // openInputStream catches the parse exception and reports it as an error.
  auto result = remote->openInputStream("https://example.com/f");
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().error_message().find("VERIFY_SSL"),
            std::string::npos)
      << "Error message should mention VERIFY_SSL. Got: "
      << result.error().ToString();
}

// ============================================================================
// CURL global initialization thread safety (std::call_once)
// Verify multiple concurrent HTTPFileSystem constructions don't crash.
// ============================================================================

TEST_F(HTTPFileSystemTest, ConcurrentConstruction_ThreadSafety) {
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
// Integration test — guarded by the HTTP_TEST_URL environment variable.
// Set HTTP_TEST_URL to an https URL serving a Parquet file to exercise the
// real read path (HEAD probe, ranged GET). Skipped when unset or offline.
// ============================================================================

class HTTPIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* url = std::getenv("HTTP_TEST_URL");
    if (url == nullptr || url[0] == '\0') {
      GTEST_SKIP() << "HTTP_TEST_URL not set; skipping HTTP integration test";
    }
    test_url_ = url;
  }

  std::string test_url_;
};

TEST_F(HTTPIntegrationTest, ExistsSizeAndRangedRead) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  // exists() + getSize() via HEAD probe.
  auto exists_result = remote->exists(test_url_);
  ASSERT_TRUE(exists_result.has_value()) << exists_result.error().ToString();
  EXPECT_TRUE(*exists_result);

  auto size_result = remote->getSize(test_url_);
  ASSERT_TRUE(size_result.has_value()) << size_result.error().ToString();
  int64_t file_size = *size_result;
  EXPECT_GT(file_size, 0);

  // openInputStream + ReadAt: first 4 bytes must be the Parquet magic "PAR1".
  auto stream_result = remote->openInputStream(test_url_);
  ASSERT_TRUE(stream_result.has_value()) << stream_result.error().ToString();
  auto stream = *stream_result;

  char magic[4] = {0, 0, 0, 0};
  auto read_result = stream->ReadAt(0, 4, magic);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().ToString();
  EXPECT_EQ(*read_result, 4);
  EXPECT_EQ(magic[0], 'P');
  EXPECT_EQ(magic[1], 'A');
  EXPECT_EQ(magic[2], 'R');
  EXPECT_EQ(magic[3], '1');

  // GetSize from the stream matches the HEAD probe.
  auto stream_size = stream->GetSize();
  ASSERT_TRUE(stream_size.has_value());
  EXPECT_EQ(*stream_size, file_size);

  // Read past EOF returns 0 bytes (clamped against file size).
  char buf[16];
  auto eof_result = stream->ReadAt(file_size + 1000, 16, buf);
  ASSERT_TRUE(eof_result.has_value());
  EXPECT_EQ(*eof_result, 0);

  stream->Close();
}

TEST_F(HTTPIntegrationTest, NonExistentURL_ReportsNotFound) {
  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  // Derive a URL that almost certainly does not exist next to the test file.
  std::string missing = test_url_ + ".this-does-not-exist-12345";

  auto exists_result = remote->exists(missing);
  // exists() never throws; probe failure is reported as false.
  ASSERT_TRUE(exists_result.has_value());
  EXPECT_FALSE(*exists_result);

  auto size_result = remote->getSize(missing);
  ASSERT_FALSE(size_result.has_value());
  EXPECT_EQ(size_result.error().error_code(), neug::StatusCode::ERR_NOT_FOUND);
}
