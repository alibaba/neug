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
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "glob_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"
#include "s3_client.h"

namespace neug {
namespace extension {
namespace s3 {

/**
 * S3/OSS URI components structure for parsing cloud storage URIs
 *
 * Supports formats:
 * - s3://bucket-name/path/to/object
 * - oss://bucket-name/path/to/object  (Alibaba Cloud OSS)
 */
struct S3URIComponents {
  std::string scheme;     // "s3" or "oss"
  std::string bucket;     // Bucket name
  std::string objectKey;  // Object key (path within bucket)
  bool hasGlob;           // Whether key contains glob pattern (* or ?)

  /**
   * Parse S3/OSS URI: s3://bucket-name/path or oss://bucket-name/path
   * Throws exception if URI format is invalid
   */
  static S3URIComponents parse(const std::string& uri);

  /**
   * Parse an S3 path that may be a full URI ("s3://bucket/key") or a bare
   * "bucket/key" path (as produced by glob()). Throws on invalid input.
   */
  static S3URIComponents parseFlexible(const std::string& path);

  /// Rebuild a full URI: "<scheme>://<bucket>/<key>"
  std::string toURI() const;
};

/**
 * S3FileSystem implements the neug::fsys::FileSystem interface for accessing
 * S3-compatible storage (AWS S3, Alibaba Cloud OSS, MinIO, etc.) WITHOUT any
 * Arrow/AWS-SDK dependency: the S3 protocol is implemented on libcurl
 * (see S3Client) with SigV4 signing.
 *
 * Features:
 * - Automatic OSS/MinIO detection and addressing optimization
 * - Credential sources: explicit options, environment variables, anonymous
 * - Glob pattern expansion via ListObjectsV2
 * - Read (ranged GET) and write (PutObject / Multipart Upload) paths
 */
class S3FileSystem : public fsys::FileSystem {
 public:
  explicit S3FileSystem(const reader::FileSchema& schema);

  // fsys::FileSystem interface
  // Returns full URIs ("<scheme>://<bucket>/<key>") for each matched object.
  std::vector<std::string> glob(const std::string& path) override;
  std::shared_ptr<fsys::RemoteFileSystem> getRemoteFileSystem() const override;

  /**
   * Build S3ClientConfig from schema configuration.
   * Static so it can be called without initializing a full S3FileSystem
   * (useful for testing option building without a real S3 connection).
   */
  static S3ClientConfig buildS3Config(const reader::FileSchema& schema);

 private:
  std::shared_ptr<S3Client> client_;
  std::shared_ptr<fsys::RemoteFileSystem> remote_fs_;
};

/**
 * Factory function to create an S3FileSystem from a FileSchema.
 * Registers as the factory for "s3" and "oss" protocols in FileSystemRegistry.
 */
std::unique_ptr<fsys::FileSystem> CreateS3FileSystem(
    const reader::FileSchema& schema);

}  // namespace s3
}  // namespace extension
}  // namespace neug
