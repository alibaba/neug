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

#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <curl/curl.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_sys/file_system.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace extension {
namespace http {

/**
 * HTTP/HTTPS URI components structure for parsing HTTP(S) URLs
 * 
 * Supports formats:
 * - https://example.com/path/to/file.parquet
 * - http://example.com:8080/data/file.csv
 */
struct HTTPURIComponents {
  std::string scheme;    // "http" or "https"
  std::string host;      // Hostname or IP address
  int port;              // Port number (default: 80 for http, 443 for https)
  std::string path;      // Path component (/path/to/file)
  
  /**
   * Parse HTTP(S) URI
   * Throws exception if URI format is invalid
   */
  static HTTPURIComponents parse(const std::string& uri);
  
  /**
   * Reconstruct full URL from components
   */
  std::string toURL() const;
};

/**
 * HTTP RandomAccessFile implementation using libcurl
 * 
 * Supports:
 * - HTTP Range requests for partial reading
 * - Connection reuse for efficient I/O
 * - TLS/SSL for HTTPS
 * - Authentication via Bearer token or custom headers
 */
class HTTPRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  /**
   * Create HTTP file handle
   * 
   * @param url Full HTTP(S) URL
   * @param options HTTP options (auth, headers, etc.)
   */
  HTTPRandomAccessFile(const std::string& url, 
                       const common::case_insensitive_map_t<std::string>& options);
  
  ~HTTPRandomAccessFile() override;

  // arrow::io::RandomAccessFile interface
  arrow::Result<int64_t> Tell() const override;
  arrow::Result<int64_t> GetSize() override;
  arrow::Status Seek(int64_t position) override;
  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
  arrow::Status Close() override;
  bool closed() const override;

 private:
  /**
   * Perform HTTP Range request
   * @return Result<int64_t> Number of bytes actually read, or error status
   */
  arrow::Result<int64_t> ReadRange(int64_t offset, int64_t length, void* buffer);
  
  /**
   * Initialize file size via HEAD request
   */
  arrow::Status InitializeFileSize();
  
  /**
   * Setup CURL handle with common options
   */
  void SetupCURLHandle(CURL* curl);

  std::string url_;
  common::case_insensitive_map_t<std::string> options_;
  CURL* curl_handle_;
  int64_t file_size_;
  int64_t position_;
  bool closed_;
  
  // Authentication
  std::string bearer_token_;
  std::vector<std::string> custom_headers_;
  struct curl_slist* header_list_;
};

/**
 * HTTP FileSystem - implements both arrow::fs::FileSystem and neug::fsys::FileSystem.
 *
 * As arrow::fs::FileSystem: provides read-only HTTP/HTTPS file access via
 * libcurl with Range request support, integrating with Arrow Dataset API.
 *
 * As neug::fsys::FileSystem: glob() returns the path unchanged (HTTP has no
 * directory listing), toArrowFileSystem() constructs a new HTTPFileSystem
 * instance from the stored options.
 */
class HTTPFileSystem : public arrow::fs::FileSystem, public fsys::FileSystem {
 public:
  // Construct from raw options (used internally and by toArrowFileSystem()).
  explicit HTTPFileSystem(const common::case_insensitive_map_t<std::string>& options);

  // Construct from FileSchema (used by CreateHTTPFileSystem factory).
  explicit HTTPFileSystem(const reader::FileSchema& schema);

  ~HTTPFileSystem() override;

  // --- arrow::fs::FileSystem interface ---
  std::string type_name() const override { return "http"; }

  bool Equals(const arrow::fs::FileSystem& other) const override;

  arrow::Result<arrow::fs::FileInfo> GetFileInfo(const std::string& path) override;

  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const arrow::fs::FileSelector& selector) override;

  arrow::Status CreateDir(const std::string& path, bool recursive = true) override;

  arrow::Status DeleteDir(const std::string& path) override;

  arrow::Status DeleteDirContents(const std::string& path,
                                  bool missing_dir_ok = false) override;

  arrow::Status DeleteRootDirContents() override;

  arrow::Status DeleteFile(const std::string& path) override;

  arrow::Status Move(const std::string& src, const std::string& dest) override;

  arrow::Status CopyFile(const std::string& src, const std::string& dest) override;

  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const std::string& path) override;

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override;

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override;

  // --- neug::fsys::FileSystem interface ---
  // HTTP has no directory listing; returns the path unchanged.
  std::vector<std::string> glob(const std::string& path) override;

  // Returns a new HTTPFileSystem instance built from the stored options.
  // Each call produces an independent instance; the caller owns it exclusively.
  std::unique_ptr<arrow::fs::FileSystem> toArrowFileSystem() override;

 private:
  common::case_insensitive_map_t<std::string> options_;

  // Global CURL initialization (shared across all instances).
  static bool curl_global_initialized_;
  static std::mutex curl_init_mutex_;
};

/**
 * Factory function: constructs an HTTPFileSystem from a FileSchema.
 * Registered for "http" and "https" protocols in FileSystemRegistry.
 */
std::unique_ptr<fsys::FileSystem> CreateHTTPFileSystem(
    const reader::FileSchema& schema);

}  // namespace http
}  // namespace extension
}  // namespace neug
