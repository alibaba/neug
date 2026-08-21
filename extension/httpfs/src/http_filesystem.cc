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

#include "http_filesystem.h"
#include "http_options.h"

#include <curl/curl.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include "remote_io_utils.h"
#include "s3_client.h"  // EnsureCurlInitialized

namespace neug {
namespace extension {
namespace http {

// ============================================================================
// HTTPURIComponents Implementation
// ============================================================================

HTTPURIComponents HTTPURIComponents::parse(const std::string& uri) {
  HTTPURIComponents components;

  // Find scheme
  size_t scheme_end = uri.find("://");
  if (scheme_end == std::string::npos) {
    THROW_IO_EXCEPTION("Invalid HTTP URI (missing scheme): " + uri);
  }

  components.scheme = uri.substr(0, scheme_end);
  if (components.scheme != "http" && components.scheme != "https") {
    THROW_IO_EXCEPTION("Invalid HTTP URI scheme (expected http or https): " +
                       components.scheme);
  }

  // Parse authority and path
  size_t authority_start = scheme_end + 3;
  size_t path_start = uri.find('/', authority_start);

  std::string authority;
  if (path_start == std::string::npos) {
    authority = uri.substr(authority_start);
    components.path = "/";
  } else {
    authority = uri.substr(authority_start, path_start - authority_start);
    components.path = uri.substr(path_start);
  }

  // Parse host and port
  size_t port_sep = authority.find(':');
  if (port_sep == std::string::npos) {
    components.host = authority;
    // Default ports
    components.port = (components.scheme == "https") ? 443 : 80;
  } else {
    components.host = authority.substr(0, port_sep);
    std::string port_str = authority.substr(port_sep + 1);
    try {
      components.port = std::stoi(port_str);
    } catch (...) { THROW_IO_EXCEPTION("Invalid port number: " + port_str); }
  }

  return components;
}

std::string HTTPURIComponents::toURL() const {
  std::ostringstream oss;
  oss << scheme << "://" << host;

  // Only include port if non-default
  if ((scheme == "http" && port != 80) || (scheme == "https" && port != 443)) {
    oss << ":" << port;
  }

  oss << path;
  return oss.str();
}

// ============================================================================
// CURL helpers
// ============================================================================

namespace {

// Callback for reading response directly into buffer
size_t WriteCallbackDirect(void* contents, size_t size, size_t nmemb,
                           void* userp) {
  size_t real_size = size * nmemb;
  auto* info = static_cast<std::pair<void*, size_t>*>(userp);

  size_t to_copy = std::min(real_size, info->second);
  if (to_copy > 0) {
    std::memcpy(info->first, contents, to_copy);
    info->first = static_cast<uint8_t*>(info->first) + to_copy;
    info->second -= to_copy;
  }

  if (real_size > to_copy) {
    // Buffer is full — signal CURL to abort the transfer so that the
    // caller knows the data was truncated rather than silently dropped.
    LOG(WARNING) << "WriteCallbackDirect: buffer full, aborting transfer. "
                 << "real_size=" << real_size << ", copied=" << to_copy
                 << ", discarding " << (real_size - to_copy) << " bytes";
    return 0;  // returning 0 tells CURL to stop the transfer
  }

  return real_size;
}

// Callback that discards the response body
size_t DiscardCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  return size * nmemb;
}

// Parse "Content-Range: bytes START-END/TOTAL" or "bytes */TOTAL".
// Returns TOTAL, or -1 if not found/parseable.
int64_t ParseContentRangeTotal(const std::string& header_value) {
  auto slash_pos = header_value.find('/');
  if (slash_pos == std::string::npos)
    return -1;
  auto total_str = header_value.substr(slash_pos + 1);
  if (total_str == "*")
    return -1;
  try {
    return std::stoll(total_str);
  } catch (...) { return -1; }
}

// Captures the Content-Range header value from response headers.
struct HeaderCollector {
  std::string content_range;
};

size_t HeaderCaptureCallback(void* contents, size_t size, size_t nmemb,
                             void* userp) {
  size_t real_size = size * nmemb;
  auto* collector = static_cast<HeaderCollector*>(userp);
  std::string header(static_cast<char*>(contents), real_size);
  // Strip trailing \r\n
  while (!header.empty() && (header.back() == '\r' || header.back() == '\n')) {
    header.pop_back();
  }
  if (header.find("Content-Range: ") == 0) {
    collector->content_range = header.substr(15);
  }
  return real_size;
}

bool parseBool(const std::string& value, const std::string& key) {
  std::string v = value;
  std::transform(v.begin(), v.end(), v.begin(), ::tolower);
  if (v == "true" || v == "1" || v == "yes" || v == "on") {
    return true;
  }
  if (v == "false" || v == "0" || v == "no" || v == "off") {
    return false;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION(
      "Invalid " + key + " value '" + value +
      "'. Expected 'true'/'false', '1'/'0', 'yes'/'no', or 'on'/'off'.");
}

/// Resolved, typed HTTP options extracted from the case-insensitive option map.
struct HTTPOptions {
  std::vector<std::string> custom_headers;  // includes Authorization
  bool verify_ssl = HTTPConfigDefaults::kVerifySSLDefault;
  std::string ca_cert_file;
  int connect_timeout = HTTPConfigDefaults::kConnectTimeoutDefault;
  int request_timeout = HTTPConfigDefaults::kRequestTimeoutDefault;
  std::string proxy;
  std::string proxy_userpass;

  static HTTPOptions fromMap(
      const common::case_insensitive_map_t<std::string>& options) {
    HTTPOptions out;

    // Authentication
    std::string bearer_token;
    auto bearer_it = options.find(HTTPConfigOptionKeys::kBearerToken);
    if (bearer_it != options.end()) {
      bearer_token = bearer_it->second;
    }
    auto auth_header_it =
        options.find(HTTPConfigOptionKeys::kAuthorizationHeader);
    if (auth_header_it != options.end() && bearer_token.empty()) {
      out.custom_headers.push_back("Authorization: " + auth_header_it->second);
    } else if (!bearer_token.empty()) {
      out.custom_headers.push_back("Authorization: Bearer " + bearer_token);
    }

    // Custom headers ("Key1:Value1;Key2:Value2")
    auto headers_it = options.find(HTTPConfigOptionKeys::kCustomHeaders);
    if (headers_it != options.end()) {
      const std::string& headers_str = headers_it->second;
      size_t pos = 0;
      while (pos < headers_str.size()) {
        size_t sep = headers_str.find(';', pos);
        std::string header = (sep == std::string::npos)
                                 ? headers_str.substr(pos)
                                 : headers_str.substr(pos, sep - pos);
        if (!header.empty()) {
          out.custom_headers.push_back(header);
        }
        pos = (sep == std::string::npos) ? headers_str.size() : sep + 1;
      }
    }

    // TLS
    auto verify_it = options.find(HTTPConfigOptionKeys::kVerifySSL);
    if (verify_it != options.end()) {
      out.verify_ssl =
          parseBool(verify_it->second, HTTPConfigOptionKeys::kVerifySSL);
    }
    auto ca_it = options.find(HTTPConfigOptionKeys::kCACertFile);
    if (ca_it != options.end()) {
      out.ca_cert_file = ca_it->second;
    }

    // Timeouts
    auto connect_it = options.find(HTTPConfigOptionKeys::kConnectTimeout);
    if (connect_it != options.end()) {
      try {
        out.connect_timeout = std::stoi(connect_it->second);
      } catch (const std::exception& e) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Invalid CONNECT_TIMEOUT value: '" + connect_it->second +
            "'. Must be an integer. Error: " + e.what());
      }
    }
    auto request_it = options.find(HTTPConfigOptionKeys::kRequestTimeout);
    if (request_it != options.end()) {
      try {
        out.request_timeout = std::stoi(request_it->second);
      } catch (const std::exception& e) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Invalid REQUEST_TIMEOUT value: '" + request_it->second +
            "'. Must be an integer. Error: " + e.what());
      }
    }

    // Proxy
    auto proxy_it = options.find(HTTPConfigOptionKeys::kHTTPProxy);
    if (proxy_it != options.end()) {
      out.proxy = proxy_it->second;
      auto proxy_user_it =
          options.find(HTTPConfigOptionKeys::kHTTPProxyUsername);
      auto proxy_pass_it =
          options.find(HTTPConfigOptionKeys::kHTTPProxyPassword);
      if (proxy_user_it != options.end() && proxy_pass_it != options.end()) {
        out.proxy_userpass =
            proxy_user_it->second + ":" + proxy_pass_it->second;
      }
    }

    return out;
  }
};

void SetupCURLHandle(CURL* curl, const HTTPOptions& opts,
                     struct curl_slist* header_list) {
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, opts.verify_ssl ? 1L : 0L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, opts.verify_ssl ? 2L : 0L);
  if (!opts.ca_cert_file.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, opts.ca_cert_file.c_str());
  }

  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                   static_cast<long>(opts.connect_timeout));
  curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                   static_cast<long>(opts.request_timeout));

  if (!opts.proxy.empty()) {
    curl_easy_setopt(curl, CURLOPT_PROXY, opts.proxy.c_str());
    if (!opts.proxy_userpass.empty()) {
      curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, opts.proxy_userpass.c_str());
    }
  }

  if (header_list) {
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
  }
}

// ============================================================================
// HTTPRandomAccessStream — ranged reads over an HTTP(S) URL
// ============================================================================

/// Random-access read stream over an HTTP(S) URL.
///
/// Server requirements: the endpoint MUST support HEAD requests (used to
/// probe the size at open time) and Range requests (used for ranged
/// reads). A server that ignores Range and answers 200 would deliver
/// misaligned bytes, so such responses are rejected with an error rather
/// than silently misread.
///
/// Small reads are served from a readahead block, and TCP/TLS connections
/// are reused across requests through a shared CURLSH cache.
class HTTPRandomAccessStream : public fsys::RandomAccessStream {
 public:
  HTTPRandomAccessStream(
      std::string url,
      const common::case_insensitive_map_t<std::string>& options,
      std::shared_ptr<CurlShare> curl_share)
      : url_(std::move(url)),
        opts_(HTTPOptions::fromMap(options)),
        curl_share_(std::move(curl_share)) {
    // Build the (immutable) header list once.
    for (const auto& header : opts_.custom_headers) {
      header_list_ = curl_slist_append(header_list_, header.c_str());
    }
    // Probe the file up-front so construction fails for missing resources.
    // (The caller catches the exception and converts it to a result error.)
    auto size = probeSize();
    if (!size) {
      THROW_IO_EXCEPTION("Failed to probe HTTP file size for " + url_ + ": " +
                         size.error().error_message());
    }
    file_size_ = *size;
  }

  ~HTTPRandomAccessStream() override {
    if (header_list_) {
      curl_slist_free_all(header_list_);
    }
  }

  result<int64_t> Read(int64_t nbytes, void* out) override {
    auto n = ReadAt(position_, nbytes, out);
    if (n) {
      position_ += *n;
    }
    return n;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    if (closed_) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "HTTP stream is closed: " + url_);
    }
    if (position < 0 || nbytes < 0) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid read: position=" + std::to_string(position) +
                              ", nbytes=" + std::to_string(nbytes));
    }
    if (nbytes == 0) {
      return int64_t{0};
    }
    if (file_size_ >= 0 && position >= file_size_) {
      return int64_t{0};
    }

    const int64_t served = readahead_.tryServe(position, nbytes, out);
    if (served >= 0) {
      return served;
    }
    // Fetch at least the readahead window so subsequent sequential small
    // reads hit the cache instead of issuing one request per read.
    int64_t fetch_len = std::max(nbytes, kRemoteReadaheadBytes);
    if (file_size_ >= 0) {
      fetch_len = std::min(fetch_len, file_size_ - position);
    }
    fetch_buf_.resize(static_cast<size_t>(fetch_len));
    auto got = fetchRange(position, fetch_len, fetch_buf_.data());
    if (!got) {
      return tl::unexpected(got.error());
    }
    readahead_.store(position, fetch_buf_.data(), *got);
    const int64_t copy_len = std::min(*got, nbytes);
    if (copy_len > 0) {
      std::memcpy(out, fetch_buf_.data(), static_cast<size_t>(copy_len));
    }
    return copy_len;
  }

  /// Issue a single ranged GET into `out` (capacity `nbytes`).
  result<int64_t> fetchRange(int64_t position, int64_t nbytes, void* out) {
    CURL* curl = curl_easy_init();
    if (!curl) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to initialize CURL handle");
    }
    if (curl_share_ && curl_share_->handle()) {
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_->handle());
    }

    SetupCURLHandle(curl, opts_, header_list_);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());

    // Range header: "start-end" (CURL adds the "bytes=" prefix)
    std::string range_value =
        std::to_string(position) + "-" + std::to_string(position + nbytes - 1);
    curl_easy_setopt(curl, CURLOPT_RANGE, range_value.c_str());

    std::pair<void*, size_t> write_info{out, static_cast<size_t>(nbytes)};
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallbackDirect);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &write_info);

    CURLcode res = curl_easy_perform(curl);
    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    curl_easy_cleanup(curl);

    // Check the status code first: a server that ignores Range answers 200
    // with the whole file, which overflows the (small) range buffer and
    // aborts the transfer with CURLE_WRITE_ERROR — that specific failure
    // must surface as the clearer "no Range support" error.
    if (response_code == 416) {
      // Range Not Satisfiable - position is beyond end of file
      return int64_t{0};
    }
    if (response_code == 200) {
      if (position == 0) {
        // Server doesn't support Range, but we're reading from offset 0,
        // so the whole-file response is valid. Accept whatever bytes we got.
        int64_t bytes_received =
            nbytes - static_cast<int64_t>(write_info.second);
        return bytes_received;
      }
      // Otherwise, the server ignored Range and returned misaligned data.
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_IO_ERROR,
          "Server ignored the Range request (returned 200); Range support "
          "is required: " +
              url_);
    }
    if (res != CURLE_OK) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_IO_ERROR,
          "HTTP Range request failed: " + std::string(curl_easy_strerror(res)) +
              " (" + url_ + ")");
    }

    int64_t bytes_received = nbytes - static_cast<int64_t>(write_info.second);

    if (response_code == 206) {
      return bytes_received;
    }
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "HTTP Range request failed with status " +
                            std::to_string(response_code) + " (" + url_ + ")");
  }

  result<int64_t> GetSize() override {
    if (closed_) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "HTTP stream is closed: " + url_);
    }
    return file_size_;
  }

  void Close() override { closed_ = true; }

 private:
  // HEAD request to determine the file size (-1 when unavailable).
  // Falls back to GET with Range: bytes=0-0 if HEAD fails or returns no
  // Content-Length, parsing Content-Range to extract the total size.
  result<int64_t> probeSize() {
    // Try HEAD first.
    CURL* curl = curl_easy_init();
    if (!curl) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to initialize CURL for HEAD request");
    }
    if (curl_share_ && curl_share_->handle()) {
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_->handle());
    }

    SetupCURLHandle(curl, opts_, header_list_);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, DiscardCallback);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    double content_length = -1;
    if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
      curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD,
                        &content_length);
    }
    curl_easy_cleanup(curl);

    if (content_length >= 0) {
      return static_cast<int64_t>(content_length);
    }

    // HEAD failed or returned no Content-Length; fallback to GET with
    // Range: bytes=0-0 and parse Content-Range to get total size.
    return probeSizeViaGetRange();
  }

  // Fallback: GET with Range: bytes=0-0, parse Content-Range for total size.
  result<int64_t> probeSizeViaGetRange() {
    CURL* curl = curl_easy_init();
    if (!curl) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to initialize CURL for GET range probe");
    }
    if (curl_share_ && curl_share_->handle()) {
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_->handle());
    }

    SetupCURLHandle(curl, opts_, header_list_);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, "0-0");

    HeaderCollector collector;
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCaptureCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &collector);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, DiscardCallback);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "GET range probe failed for " + url_ + ": " +
                              std::string(curl_easy_strerror(res)));
    }
    if (http_code != 200 && http_code != 206) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "GET range probe returned HTTP " +
                              std::to_string(http_code) + " for " + url_);
    }

    if (!collector.content_range.empty()) {
      int64_t total = ParseContentRangeTotal(collector.content_range);
      if (total >= 0) {
        return total;
      }
    }
    LOG(WARNING) << "Could not determine file size for " << url_;
    return int64_t{-1};
  }

  std::string url_;
  HTTPOptions opts_;
  std::shared_ptr<CurlShare> curl_share_;
  struct curl_slist* header_list_ = nullptr;
  int64_t file_size_ = -1;
  int64_t position_ = 0;
  bool closed_ = false;
  ReadaheadCache readahead_;
  std::vector<char> fetch_buf_;
};

// ============================================================================
// HTTPRemoteFileSystem — fsys::RemoteFileSystem over HTTP(S)
// ============================================================================

class HTTPRemoteFileSystem : public fsys::RemoteFileSystem {
 public:
  explicit HTTPRemoteFileSystem(
      const common::case_insensitive_map_t<std::string>& options)
      : options_(options) {}

  result<std::shared_ptr<fsys::RandomAccessStream>> openInputStream(
      const std::string& path) override {
    try {
      HTTPURIComponents::parse(path);
      return std::shared_ptr<fsys::RandomAccessStream>(
          std::make_shared<HTTPRandomAccessStream>(path, options_,
                                                   curl_share_));
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to open HTTP file: " + std::string(e.what()));
    }
  }

  result<std::shared_ptr<fsys::OutputStream>> openOutputStream(
      const std::string& path) override {
    // HTTP/HTTPS sources are read-only; exports must target local paths or
    // s3:// / oss:// destinations instead.
    RETURN_STATUS_ERROR(
        neug::StatusCode::ERR_NOT_SUPPORTED,
        "Writing over HTTP is not supported (read-only filesystem): " + path);
  }

  result<bool> exists(const std::string& path) override {
    auto size = headProbe(path);
    if (!size) {
      // Any failure (incl. 404) is treated as "not found" for existence
      // checks; transport-level nuances are surfaced by openInputStream.
      return false;
    }
    return true;
  }

  result<int64_t> getSize(const std::string& path) override {
    return headProbe(path);
  }

 private:
  result<int64_t> headProbe(const std::string& path) {
    CURL* curl = curl_easy_init();
    if (!curl) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to create CURL handle for HEAD request");
    }
    if (curl_share_ && curl_share_->handle()) {
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_->handle());
    }

    HTTPOptions opts = HTTPOptions::fromMap(options_);
    struct curl_slist* header_list = nullptr;
    for (const auto& header : opts.custom_headers) {
      header_list = curl_slist_append(header_list, header.c_str());
    }
    SetupCURLHandle(curl, opts, header_list);
    curl_easy_setopt(curl, CURLOPT_URL, path.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, DiscardCallback);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }
    double content_length = -1;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "HEAD request failed for " + path + ": " +
                              std::string(curl_easy_strerror(res)));
    }
    if (http_code < 200 || http_code >= 300) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_NOT_FOUND,
                          "HEAD request returned HTTP " +
                              std::to_string(http_code) + " for " + path);
    }
    return content_length >= 0 ? static_cast<int64_t>(content_length)
                               : int64_t{-1};
  }

  common::case_insensitive_map_t<std::string> options_;
  // Shared DNS/TLS/TCP connection cache for all streams and probes opened
  // through this filesystem.
  std::shared_ptr<CurlShare> curl_share_ = std::make_shared<CurlShare>();
};

}  // namespace

// ============================================================================
// HTTPFileSystem Implementation
// ============================================================================

HTTPFileSystem::HTTPFileSystem(
    const common::case_insensitive_map_t<std::string>& options)
    : options_(options) {
  s3::EnsureCurlInitialized();
  remote_fs_ = std::make_shared<HTTPRemoteFileSystem>(options_);
}

HTTPFileSystem::HTTPFileSystem(const reader::FileSchema& schema)
    : HTTPFileSystem(schema.options) {
  // Validate all paths are HTTP(S) URLs
  for (const auto& path : schema.paths) {
    try {
      HTTPURIComponents::parse(path);
    } catch (const exception::Exception& e) {
      THROW_IO_EXCEPTION("Invalid HTTP URL: " + path + " - " + e.what());
    }
  }
}

// --- neug::fsys::FileSystem interface ---

std::vector<std::string> HTTPFileSystem::glob(const std::string& path) {
  // HTTP has no directory listing or glob expansion; return path unchanged.
  return {path};
}

std::shared_ptr<fsys::RemoteFileSystem> HTTPFileSystem::getRemoteFileSystem()
    const {
  return remote_fs_;
}

std::unique_ptr<fsys::FileSystem> CreateHTTPFileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<HTTPFileSystem>(schema);
}

}  // namespace http
}  // namespace extension
}  // namespace neug
