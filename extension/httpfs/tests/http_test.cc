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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
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
// Range-unsupporting server: ReadAt must reject a 200 response instead of
// serving offset-0 bytes as data at the requested position.
// ============================================================================

/// Minimal loopback HTTP server that ignores the Range header and always
/// answers 200 with the full body — the shape of a server without Range
/// support.
class NoRangeHTTPServer {
 public:
  explicit NoRangeHTTPServer(std::string body) : body_(std::move(body)) {}

  ~NoRangeHTTPServer() { stop(); }

  bool start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      return false;
    }
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // ephemeral port
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) !=
        0) {
      return false;
    }
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    if (::listen(listen_fd_, 4) != 0) {
      return false;
    }
    running_ = true;
    thread_ = std::thread([this] { serve(); });
    return true;
  }

  void stop() {
    if (!running_.exchange(false)) {
      return;
    }
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

 private:
  void serve() {
    while (running_) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        break;
      }
      handle(fd);
      ::close(fd);
    }
  }

  void handle(int fd) {
    // Read until the request headers are complete.
    std::string request;
    char buf[1024];
    while (request.find("\r\n\r\n") == std::string::npos) {
      ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
      if (n <= 0) {
        return;
      }
      request.append(buf, static_cast<size_t>(n));
    }
    const bool is_head = request.compare(0, 5, "HEAD ") == 0;
    // Always 200 with the full body; the Range header is ignored.
    std::string response =
        "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(body_.size()) +
        "\r\nConnection: close\r\n\r\n";
    if (!is_head) {
      response += body_;
    }
    ::send(fd, response.data(), response.size(), 0);
  }

  std::string body_;
  std::thread thread_;
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> running_{false};
};

TEST_F(HTTPFileSystemTest, ReadAtRejectsServerWithoutRangeSupport) {
  NoRangeHTTPServer server("0123456789");
  ASSERT_TRUE(server.start());

  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  auto stream_result = remote->openInputStream(
      "http://127.0.0.1:" + std::to_string(server.port()) + "/file.bin");
  ASSERT_TRUE(stream_result.has_value()) << stream_result.error().ToString();
  auto stream = std::move(*stream_result);

  // The server answers 200 with bytes from offset 0; those must not be
  // served as data at position 2 (silent corruption). The read must fail.
  char buf[4];
  auto r = stream->ReadAt(2, 4, buf);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().error_code(), neug::StatusCode::ERR_IO_ERROR);
  EXPECT_NE(r.error().error_message().find("Range support is required"),
            std::string::npos)
      << r.error().error_message();

  stream->Close();
  server.stop();
}

// ============================================================================
// Range-supporting keep-alive server: verifies the readahead cache (small
// sequential reads collapse into few ranged GETs) and CURLSH connection
// reuse (all requests share one TCP connection).
// ============================================================================

/// Loopback HTTP server with HEAD + Range support and HTTP/1.1 keep-alive.
/// Counts accepted connections and served requests so tests can observe
/// readahead hits and connection reuse.
class RangeKeepAliveHTTPServer {
 public:
  explicit RangeKeepAliveHTTPServer(std::string body)
      : body_(std::move(body)) {}

  ~RangeKeepAliveHTTPServer() { stop(); }

  bool start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      return false;
    }
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // ephemeral port
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) !=
        0) {
      return false;
    }
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    if (::listen(listen_fd_, 4) != 0) {
      return false;
    }
    running_ = true;
    thread_ = std::thread([this] { serve(); });
    return true;
  }

  void stop() {
    if (!running_.exchange(false)) {
      return;
    }
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    // The keep-alive connection may sit blocked in recv(); shutting down
    // the listen socket alone would not unblock it.
    const int fd = active_fd_.load();
    if (fd >= 0) {
      ::shutdown(fd, SHUT_RDWR);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }
  int connections() const { return connections_.load(); }
  int requests() const { return requests_.load(); }

 private:
  void serve() {
    while (running_) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        break;
      }
      connections_.fetch_add(1);
      active_fd_.store(fd);
      handle(fd);
      active_fd_.store(-1);
      ::close(fd);
    }
  }

  void handle(int fd) {
    std::string pending;
    char buf[4096];
    // Keep-alive: serve requests on this connection until the peer closes.
    while (running_) {
      size_t hdr_end;
      while ((hdr_end = pending.find("\r\n\r\n")) == std::string::npos) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
          return;
        }
        pending.append(buf, static_cast<size_t>(n));
      }
      const std::string request = pending.substr(0, hdr_end + 4);
      pending.erase(0, hdr_end + 4);
      requests_.fetch_add(1);

      const bool is_head = request.compare(0, 5, "HEAD ") == 0;
      // Parse "Range: bytes=start-end" when present.
      int64_t start = 0;
      int64_t end = static_cast<int64_t>(body_.size()) - 1;
      bool has_range = false;
      size_t range_pos = request.find("Range: bytes=");
      if (range_pos != std::string::npos) {
        has_range = true;
        size_t val_pos = range_pos + strlen("Range: bytes=");
        size_t dash = request.find('-', val_pos);
        start = std::stoll(request.substr(val_pos, dash - val_pos));
        size_t line_end = request.find("\r\n", dash + 1);
        end = std::stoll(request.substr(dash + 1, line_end - dash - 1));
        end = std::min(end, static_cast<int64_t>(body_.size()) - 1);
      }

      std::string response;
      if (is_head) {
        response =
            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\n"
            "Content-Length: " +
            std::to_string(body_.size()) + "\r\n\r\n";
      } else if (has_range) {
        const int64_t slice_len = end - start + 1;
        response =
            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n"
            "Content-Range: bytes " +
            std::to_string(start) + "-" + std::to_string(end) + "/" +
            std::to_string(body_.size()) +
            "\r\nContent-Length: " + std::to_string(slice_len) + "\r\n\r\n";
        if (!sendAll(fd, response)) {
          return;
        }
        if (!sendAll(fd, body_.substr(static_cast<size_t>(start),
                                      static_cast<size_t>(slice_len)))) {
          return;
        }
        continue;
      } else {
        response = "HTTP/1.1 200 OK\r\nContent-Length: " +
                   std::to_string(body_.size()) + "\r\n\r\n" + body_;
      }
      if (!sendAll(fd, response)) {
        return;
      }
    }
  }

  static bool sendAll(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
      ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
      if (n <= 0) {
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

  std::string body_;
  std::thread thread_;
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> running_{false};
  std::atomic<int> connections_{0};
  std::atomic<int> requests_{0};
  std::atomic<int> active_fd_{-1};
};

TEST_F(HTTPFileSystemTest, ReadaheadAndConnectionReuse) {
  // 8 MiB body with a verifiable repeating pattern.
  constexpr int64_t kBodySize = 8 * 1024 * 1024;
  std::string body(static_cast<size_t>(kBodySize), '\0');
  for (int64_t i = 0; i < kBodySize; ++i) {
    body[static_cast<size_t>(i)] = static_cast<char>('A' + (i % 26));
  }
  RangeKeepAliveHTTPServer server(std::move(body));
  ASSERT_TRUE(server.start());

  neug::common::case_insensitive_map_t<std::string> options;
  HTTPFileSystem fs(options);
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  auto stream_result = remote->openInputStream(
      "http://127.0.0.1:" + std::to_string(server.port()) + "/file.bin");
  ASSERT_TRUE(stream_result.has_value()) << stream_result.error().ToString();
  auto stream = std::move(*stream_result);

  // Opening probes the size with a single HEAD request.
  EXPECT_EQ(server.requests(), 1);

  // First read fetches the readahead window (one ranged GET).
  char buf[1024];
  auto r1 = stream->ReadAt(0, sizeof(buf), buf);
  ASSERT_TRUE(r1.has_value()) << r1.error().ToString();
  EXPECT_EQ(*r1, static_cast<int64_t>(sizeof(buf)));
  EXPECT_EQ(buf[0], 'A');
  EXPECT_EQ(buf[25], 'Z');
  EXPECT_EQ(buf[26], 'A');
  EXPECT_EQ(server.requests(), 2);

  // Subsequent reads inside the cached window hit no network at all.
  auto r2 = stream->ReadAt(1024, sizeof(buf), buf);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(*r2, static_cast<int64_t>(sizeof(buf)));
  EXPECT_EQ(buf[0], static_cast<char>('A' + (1024 % 26)));
  EXPECT_EQ(server.requests(), 2) << "readahead hit expected no request";

  // A read far outside the window triggers exactly one more ranged GET.
  constexpr int64_t kFarOffset = 6 * 1024 * 1024;
  auto r3 = stream->ReadAt(kFarOffset, sizeof(buf), buf);
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(*r3, static_cast<int64_t>(sizeof(buf)));
  EXPECT_EQ(buf[0], static_cast<char>('A' + (kFarOffset % 26)));
  EXPECT_EQ(server.requests(), 3);

  // All requests share one keep-alive TCP connection thanks to the CURLSH
  // connection cache.
  EXPECT_EQ(server.connections(), 1);

  stream->Close();
  server.stop();
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
