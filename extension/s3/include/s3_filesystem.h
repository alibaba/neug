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
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/api.h>
#include <memory>
#include <string>
#include <vector>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"
#include "neug/compiler/function/read_function.h"
#include "glob_utils.h"

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
  std::string bucket;       // Bucket name
  std::string objectKey;    // Object key (path within bucket)
  bool hasGlob;            // Whether key contains glob pattern (* or ?)
  
  /**
   * Parse S3/OSS URI: s3://bucket-name/path or oss://bucket-name/path
   * Throws exception if URI format is invalid
   */
  static S3URIComponents parse(const std::string& uri);
};

/**
 * S3 File System Provider for accessing S3 and S3-compatible storage
 * 
 * Supports:
 * - AWS S3
 * - Alibaba Cloud OSS
 * - MinIO
 * - Other S3-compatible storage
 * 
 * Features:
 * - Automatic OSS/MinIO detection and optimization
 * - Multiple credential sources (explicit, environment, IAM/RAM roles)
 * - Glob pattern expansion
 * - Internal/VPC endpoint support
 */
class S3FileSystemProvider
    : public function::FileSystemProvider<arrow::fs::FileSystem> {
 public:
  /**
   * Create S3 filesystem and resolve paths
   * 
   * @param schema File schema with S3 paths and options
   * @return FileInfo with filesystem instance and resolved paths
   * @throws neug::Exception if initialization fails
   */
  function::FileInfo<arrow::fs::FileSystem> provide(
      const reader::FileSchema& schema) override;
      
  /**
   * Build Arrow S3Options from schema configuration
   * 
   * Exposed as public primarily for testing and diagnostics.
   */
  arrow::fs::S3Options buildS3Options(const reader::FileSchema& schema);
  
  /**
   * Resolve S3 paths with glob pattern expansion
   * 
   * Exposed as public primarily for testing.
   */
  std::vector<std::string> resolveS3Paths(
      std::shared_ptr<arrow::fs::S3FileSystem> fs,
      const std::vector<std::string>& paths);
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
