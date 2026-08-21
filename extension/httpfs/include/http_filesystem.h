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

#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"

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
  std::string scheme;  // "http" or "https"
  std::string host;    // Hostname or IP address
  int port;            // Port number (default: 80 for http, 443 for https)
  std::string path;    // Path component (/path/to/file)

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
 * HTTP FileSystem - implements neug::fsys::FileSystem plus the
 * Arrow-agnostic fsys::RemoteFileSystem contract via getRemoteFileSystem().
 *
 * Read-only HTTP/HTTPS file access via libcurl with Range request support.
 * glob() returns the path unchanged (HTTP has no directory listing); the
 * remote filesystem handle serves read streams and rejects writes.
 */
class HTTPFileSystem : public fsys::FileSystem {
 public:
  // Construct from raw options.
  explicit HTTPFileSystem(
      const common::case_insensitive_map_t<std::string>& options);

  // Construct from FileSchema (used by CreateHTTPFileSystem factory).
  explicit HTTPFileSystem(const reader::FileSchema& schema);

  ~HTTPFileSystem() override = default;

  // --- neug::fsys::FileSystem interface ---
  // HTTP has no directory listing; returns the path unchanged.
  std::vector<std::string> glob(const std::string& path) override;

  // Returns a RemoteFileSystem built from the stored options.
  std::shared_ptr<fsys::RemoteFileSystem> getRemoteFileSystem() const override;

 private:
  common::case_insensitive_map_t<std::string> options_;
  std::shared_ptr<fsys::RemoteFileSystem> remote_fs_;
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
