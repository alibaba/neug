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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/memory.h>
#include <curl/curl.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include <mutex>
#include <sstream>
#include <thread>
#include <chrono>

namespace neug {
namespace extension {
namespace http {

// Static members initialization
bool HTTPFileSystem::curl_global_initialized_ = false;
std::mutex HTTPFileSystem::curl_init_mutex_;

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
    } catch (...) {
      THROW_IO_EXCEPTION("Invalid port number: " + port_str);
    }
  }
  
  return components;
}

std::string HTTPURIComponents::toURL() const {
  std::ostringstream oss;
  oss << scheme << "://" << host;
  
  // Only include port if non-default
  if ((scheme == "http" && port != 80) || 
      (scheme == "https" && port != 443)) {
    oss << ":" << port;
  }
  
  oss << path;
  return oss.str();
}

// ============================================================================
// CURL callback functions
// ============================================================================

namespace {

// Callback for reading response directly into buffer
size_t WriteCallbackDirect(void* contents, size_t size, size_t nmemb, void* userp) {
  size_t real_size = size * nmemb;
  auto* info = static_cast<std::pair<void*, size_t>*>(userp);
  
  LOG(INFO) << "WriteCallbackDirect called: real_size=" << real_size 
            << ", buffer remaining=" << info->second;
  
  size_t to_copy = std::min(real_size, info->second);
  if (to_copy > 0) {
    std::memcpy(info->first, contents, to_copy);
    info->first = static_cast<uint8_t*>(info->first) + to_copy;
    info->second -= to_copy;
  }
  
  if (real_size > to_copy) {
    LOG(ERROR) << "WriteCallbackDirect: Received more data than buffer can hold! "
               << "real_size=" << real_size << ", to_copy=" << to_copy
               << ", discarding " << (real_size - to_copy) << " bytes";
  }
  
  // IMPORTANT: Always return real_size to indicate success
  // Returning less than real_size signals an error to CURL
  // We handle buffer overflow by simply not copying excess data
  return real_size;
}

// Callback for HEAD requests (no body)
size_t HeaderCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  return size * nmemb;  // Discard headers
}

}  // anonymous namespace

// ============================================================================
// HTTPRandomAccessFile Implementation
// ============================================================================

HTTPRandomAccessFile::HTTPRandomAccessFile(
    const std::string& url,
    const common::case_insensitive_map_t<std::string>& options)
    : url_(url),
      options_(options),
      curl_handle_(nullptr),
      file_size_(-1),
      position_(0),
      closed_(false),
      header_list_(nullptr) {
  
  // Initialize CURL handle
  curl_handle_ = curl_easy_init();
  if (!curl_handle_) {
    THROW_IO_EXCEPTION("Failed to initialize CURL handle");
  }
  
  // Extract authentication options
  auto bearer_it = options_.find(HTTPConfigOptionKeys::kBearerToken);
  if (bearer_it != options_.end()) {
    bearer_token_ = bearer_it->second;
  }
  
  auto auth_header_it = options_.find(HTTPConfigOptionKeys::kAuthorizationHeader);
  if (auth_header_it != options_.end() && bearer_token_.empty()) {
    custom_headers_.push_back("Authorization: " + auth_header_it->second);
  } else if (!bearer_token_.empty()) {
    custom_headers_.push_back("Authorization: Bearer " + bearer_token_);
  }
  
  // Parse custom headers
  auto headers_it = options_.find(HTTPConfigOptionKeys::kCustomHeaders);
  if (headers_it != options_.end()) {
    std::string headers_str = headers_it->second;
    size_t pos = 0;
    while (pos < headers_str.size()) {
      size_t sep = headers_str.find(';', pos);
      std::string header = (sep == std::string::npos) 
          ? headers_str.substr(pos)
          : headers_str.substr(pos, sep - pos);
      
      if (!header.empty()) {
        custom_headers_.push_back(header);
      }
      
      pos = (sep == std::string::npos) ? headers_str.size() : sep + 1;
    }
  }
  
  // Initialize file size
  auto status = InitializeFileSize();
  if (!status.ok()) {
    curl_easy_cleanup(curl_handle_);
    THROW_IO_EXCEPTION("Failed to initialize HTTP file: " + status.ToString());
  }
  
  LOG(INFO) << "HTTPRandomAccessFile opened: " << url_ 
            << " (size: " << file_size_ << " bytes)";
}

HTTPRandomAccessFile::~HTTPRandomAccessFile() {
  if (header_list_) {
    curl_slist_free_all(header_list_);
  }
  if (curl_handle_) {
    curl_easy_cleanup(curl_handle_);
  }
}

arrow::Status HTTPRandomAccessFile::InitializeFileSize() {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return arrow::Status::IOError("Failed to initialize CURL for HEAD request");
  }
  
  // Setup for HEAD request
  SetupCURLHandle(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);  // HEAD request
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HeaderCallback);
  
  CURLcode res = curl_easy_perform(curl);
  
  if (res != CURLE_OK) {
    curl_easy_cleanup(curl);
    return arrow::Status::IOError("HEAD request failed: " + 
                                   std::string(curl_easy_strerror(res)));
  }
  
  // Get Content-Length
  double content_length = -1;
  res = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
  
  if (res == CURLE_OK && content_length >= 0) {
    file_size_ = static_cast<int64_t>(content_length);
  } else {
    // Content-Length not available, try a range request
    curl_easy_cleanup(curl);
    
    // Try to read last byte to determine size
    curl = curl_easy_init();
    SetupCURLHandle(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, "0-0");  // Just read first byte
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HeaderCallback);
    
    res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
      // Check Content-Range header for total size
      // This is a fallback - in practice, most servers provide Content-Length
      file_size_ = -1;  // Unknown size
      LOG(WARNING) << "Could not determine file size for " << url_;
    } else {
      curl_easy_cleanup(curl);
      return arrow::Status::IOError("Failed to determine file size: " +
                                     std::string(curl_easy_strerror(res)));
    }
  }
  
  curl_easy_cleanup(curl);
  return arrow::Status::OK();
}

void HTTPRandomAccessFile::SetupCURLHandle(CURL* curl) {
  // Basic options
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
  
  // IMPORTANT: Don't include headers in the body
  // This prevents HTTP headers from being sent to the write callback
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
  
  // SSL/TLS verification
  auto verify_it = options_.find(HTTPConfigOptionKeys::kVerifySSL);
  bool verify_ssl = HTTPConfigDefaults::kVerifySSLDefault;
  if (verify_it != options_.end()) {
    verify_ssl = (verify_it->second == "true" || verify_it->second == "1");
  }
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, verify_ssl ? 1L : 0L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, verify_ssl ? 2L : 0L);
  
  // CA certificate file
  auto ca_cert_it = options_.find(HTTPConfigOptionKeys::kCACertFile);
  if (ca_cert_it != options_.end()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_it->second.c_str());
  }
  
  // Timeouts
  auto connect_timeout_it = options_.find(HTTPConfigOptionKeys::kConnectTimeout);
  int connect_timeout = HTTPConfigDefaults::kConnectTimeoutDefault;
  if (connect_timeout_it != options_.end()) {
    connect_timeout = std::stoi(connect_timeout_it->second);
  }
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connect_timeout);
  
  auto request_timeout_it = options_.find(HTTPConfigOptionKeys::kRequestTimeout);
  int request_timeout = HTTPConfigDefaults::kRequestTimeoutDefault;
  if (request_timeout_it != options_.end()) {
    request_timeout = std::stoi(request_timeout_it->second);
  }
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, request_timeout);
  
  // Proxy
  auto proxy_it = options_.find(HTTPConfigOptionKeys::kHTTPProxy);
  if (proxy_it != options_.end()) {
    curl_easy_setopt(curl, CURLOPT_PROXY, proxy_it->second.c_str());
    
    auto proxy_user_it = options_.find(HTTPConfigOptionKeys::kHTTPProxyUsername);
    auto proxy_pass_it = options_.find(HTTPConfigOptionKeys::kHTTPProxyPassword);
    if (proxy_user_it != options_.end() && proxy_pass_it != options_.end()) {
      std::string userpass = proxy_user_it->second + ":" + proxy_pass_it->second;
      curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, userpass.c_str());
    }
  }
  
  // Custom headers
  if (!custom_headers_.empty()) {
    // Clear existing header list if it exists
    if (header_list_) {
      curl_slist_free_all(header_list_);
      header_list_ = nullptr;
    }
    // Rebuild header list
    for (const auto& header : custom_headers_) {
      header_list_ = curl_slist_append(header_list_, header.c_str());
    }
  }
  if (header_list_) {
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list_);
  }
}

arrow::Status HTTPRandomAccessFile::ReadRange(int64_t offset, int64_t length, void* buffer) {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  
  LOG(INFO) << "HTTPRandomAccessFile::ReadRange - offset=" << offset 
            << ", length=" << length << ", url=" << url_;
  
  // Setup GET request with HTTP Range header (RFC 7233)
  curl_easy_reset(curl_handle_);
  SetupCURLHandle(curl_handle_);
  curl_easy_setopt(curl_handle_, CURLOPT_URL, url_.c_str());
  
  // Set Range header: "bytes=offset-end"
  // IMPORTANT: CURLOPT_RANGE expects "start-end" format without "bytes="
  // CURL will automatically add the "Range: bytes=" prefix
  std::string range_value = std::to_string(offset) + "-" + 
                           std::to_string(offset + length - 1);
  curl_easy_setopt(curl_handle_, CURLOPT_RANGE, range_value.c_str());
  
  LOG(INFO) << "HTTP Range request: bytes=" << range_value << " for URL: " << url_;
  
  // Setup direct write to buffer
  std::pair<void*, size_t> write_info{buffer, static_cast<size_t>(length)};
  curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION, WriteCallbackDirect);
  curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA, &write_info);
  
  // Perform request
  CURLcode res = curl_easy_perform(curl_handle_);
  
  if (res != CURLE_OK) {
    return arrow::Status::IOError("HTTP Range request failed: " +
                                   std::string(curl_easy_strerror(res)));
  }
  
  // Verify response code
  // 200 = OK (server doesn't support Range, sent full file)
  // 206 = Partial Content (server supports Range, sent requested range)
  long response_code = 0;
  curl_easy_getinfo(curl_handle_, CURLINFO_RESPONSE_CODE, &response_code);
  
  LOG(INFO) << "HTTP response code: " << response_code 
            << " for Range: bytes=" << offset << "-" << (offset + length - 1);
  
  if (response_code == 206) {
    // Success: server sent the requested range
    // Check how much data was actually written
    if (write_info.second > 0) {
      // Buffer wasn't completely filled - server sent less data than requested
      LOG(WARNING) << "HTTP Range request returned less data than expected. "
                   << "Requested: " << length << " bytes, "
                   << "Received: " << (length - write_info.second) << " bytes";
    }
    return arrow::Status::OK();
  } else if (response_code == 200) {
    // Server doesn't support Range requests and sent the full file
    // This is a critical error because we can't fit the full file in our buffer
    return arrow::Status::IOError(
        "Server doesn't support HTTP Range requests (returned 200 instead of 206). "
        "Cannot read file efficiently without Range support.");
  } else {
    return arrow::Status::IOError("HTTP Range request failed with status " +
                                   std::to_string(response_code));
  }
}

arrow::Result<int64_t> HTTPRandomAccessFile::Tell() const {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  return position_;
}

arrow::Result<int64_t> HTTPRandomAccessFile::GetSize() {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  return file_size_;
}

arrow::Status HTTPRandomAccessFile::Seek(int64_t position) {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  if (position < 0) {
    return arrow::Status::Invalid("Negative seek position");
  }
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> HTTPRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  
  auto status = ReadRange(position, nbytes, out);
  if (!status.ok()) {
    return status;
  }
  
  return nbytes;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HTTPRandomAccessFile::ReadAt(
    int64_t position, int64_t nbytes) {
  if (closed_) {
    return arrow::Status::Invalid("File is closed");
  }
  
  auto buffer_result = arrow::AllocateBuffer(nbytes);
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }
  
  auto buffer = std::move(buffer_result).ValueOrDie();
  auto status = ReadRange(position, nbytes, buffer->mutable_data());
  if (!status.ok()) {
    return status;
  }
  
  return buffer;
}

arrow::Result<int64_t> HTTPRandomAccessFile::Read(int64_t nbytes, void* out) {
  auto result = ReadAt(position_, nbytes, out);
  if (result.ok()) {
    position_ += *result;
  }
  return result;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HTTPRandomAccessFile::Read(int64_t nbytes) {
  auto result = ReadAt(position_, nbytes);
  if (result.ok()) {
    position_ += nbytes;
  }
  return result;
}

arrow::Status HTTPRandomAccessFile::Close() {
  closed_ = true;
  return arrow::Status::OK();
}

bool HTTPRandomAccessFile::closed() const {
  return closed_;
}

// ============================================================================
// HTTPFileSystem Implementation
// ============================================================================

HTTPFileSystem::HTTPFileSystem(const common::case_insensitive_map_t<std::string>& options)
    : options_(options) {
  // Initialize CURL globally (thread-safe)
  std::lock_guard<std::mutex> lock(curl_init_mutex_);
  if (!curl_global_initialized_) {
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
      THROW_IO_EXCEPTION("Failed to initialize CURL globally: " +
                         std::string(curl_easy_strerror(res)));
    }
    curl_global_initialized_ = true;
    LOG(INFO) << "CURL global initialization completed";
  }
}

HTTPFileSystem::~HTTPFileSystem() {
  // Note: We don't call curl_global_cleanup() because it's shared
  // across all HTTPFileSystem instances
}

bool HTTPFileSystem::Equals(const arrow::fs::FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  // For simplicity, consider all HTTP filesystems equal
  // In a full implementation, you might compare options
  return true;
}

arrow::Result<arrow::fs::FileInfo> HTTPFileSystem::GetFileInfo(
    const std::string& path) {
  LOG(INFO) << "HTTPFileSystem::GetFileInfo: " << path;
  
  // Validate path is HTTP(S) URL
  try {
    auto components = HTTPURIComponents::parse(path);
  } catch (const exception::Exception& e) {
    return arrow::Status::Invalid("Invalid HTTP URL: " + path);
  }
  
  // Try to get file size via HEAD request
  auto file_result = OpenInputFile(path);
  if (!file_result.ok()) {
    // File doesn't exist or inaccessible
    arrow::fs::FileInfo info;
    info.set_path(path);
    info.set_type(arrow::fs::FileType::NotFound);
    return info;
  }
  
  auto file = *file_result;
  auto size_result = file->GetSize();
  
  arrow::fs::FileInfo info;
  info.set_path(path);
  info.set_type(arrow::fs::FileType::File);
  
  if (size_result.ok()) {
    info.set_size(*size_result);
  } else {
    info.set_size(-1);  // Unknown size
  }
  
  return info;
}

arrow::Result<std::vector<arrow::fs::FileInfo>> HTTPFileSystem::GetFileInfo(
    const arrow::fs::FileSelector& selector) {
  // HTTP doesn't support directory listing
  return arrow::Status::NotImplemented(
      "Directory listing not supported for HTTP filesystem");
}

arrow::Status HTTPFileSystem::CreateDir(const std::string& path, bool recursive) {
  return arrow::Status::NotImplemented(
      "CreateDir not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteDir(const std::string& path) {
  return arrow::Status::NotImplemented(
      "DeleteDir not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteDirContents(const std::string& path,
                                                 bool missing_dir_ok) {
  return arrow::Status::NotImplemented(
      "DeleteDirContents not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteRootDirContents() {
  return arrow::Status::NotImplemented(
      "DeleteRootDirContents not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteFile(const std::string& path) {
  return arrow::Status::NotImplemented(
      "DeleteFile not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::Move(const std::string& src, 
                                    const std::string& dest) {
  return arrow::Status::NotImplemented(
      "Move not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::CopyFile(const std::string& src,
                                        const std::string& dest) {
  return arrow::Status::NotImplemented(
      "CopyFile not supported for HTTP filesystem (read-only)");
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>> 
HTTPFileSystem::OpenInputStream(const std::string& path) {
  // For now, just return the RandomAccessFile (which is also an InputStream)
  return OpenInputFile(path);
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> 
HTTPFileSystem::OpenInputFile(const std::string& path) {
  LOG(INFO) << "HTTPFileSystem::OpenInputFile: " << path;
  
  try {
    auto file = std::make_shared<HTTPRandomAccessFile>(path, options_);
    return file;
  } catch (const exception::Exception& e) {
    return arrow::Status::IOError("Failed to open HTTP file: " + 
                                   std::string(e.what()));
  }
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
HTTPFileSystem::OpenOutputStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  return arrow::Status::NotImplemented(
      "OpenOutputStream not supported for HTTP filesystem (read-only)");
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
HTTPFileSystem::OpenAppendStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  return arrow::Status::NotImplemented(
      "OpenAppendStream not supported for HTTP filesystem (read-only)");
}

// ============================================================================
// HTTPFileSystemProvider Implementation
// ============================================================================

function::FileInfo<arrow::fs::FileSystem> HTTPFileSystemProvider::provide(
    const reader::FileSchema& schema) {
  
  LOG(INFO) << "HTTPFileSystemProvider::provide called for " << schema.paths.size() << " paths";
  
  // Validate all paths are HTTP(S) URLs
  for (const auto& path : schema.paths) {
    try {
      auto components = HTTPURIComponents::parse(path);
      LOG(INFO) << "Parsed HTTP URL: " << components.toURL();
    } catch (const exception::Exception& e) {
      THROW_IO_EXCEPTION("Invalid HTTP URL: " + path + " - " + e.what());
    }
  }
  
  // Create HTTP filesystem with schema options directly
  // HTTPFileSystem will extract only the HTTP-related options it needs
  auto http_fs = std::make_shared<HTTPFileSystem>(schema.options);
  
  // Return FileInfo with HTTP filesystem and original paths
  // Note: No glob expansion for HTTP (no directory listing support)
  return function::FileInfo<arrow::fs::FileSystem>{
      schema.paths,  // resolvedPaths: keep original HTTP URLs
      http_fs        // fileSystem: Arrow-compatible HTTP filesystem
  };
}

}  // namespace http
}  // namespace extension
}  // namespace neug
