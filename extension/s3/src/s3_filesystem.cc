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

#include "s3_filesystem.h"
#include "s3_options.h"
#include <glog/logging.h>
#include <algorithm>
#include <arrow/io/api.h>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace s3 {

// S3/OSS URI Parser Implementation
S3URIComponents S3URIComponents::parse(const std::string& uri) {
  S3URIComponents components;
  
  // Validate URI starts with "s3://" or "oss://"
  std::string path;
  if (uri.substr(0, 5) == "s3://") {
    path = uri.substr(5);  // Remove "s3://" prefix
  } else if (uri.substr(0, 6) == "oss://") {
    path = uri.substr(6);  // Remove "oss://" prefix
  } else {
    THROW_IO_EXCEPTION("Invalid S3/OSS URI: must start with 's3://' or 'oss://', got: " + uri);
  }
  
  // Find first '/' to separate bucket and key
  size_t slash_pos = path.find('/');
  
  if (slash_pos == std::string::npos) {
    // No slash found - just bucket name
    components.bucket = path;
    components.objectKey = "";
  } else {
    components.bucket = path.substr(0, slash_pos);
    components.objectKey = path.substr(slash_pos + 1);
  }
  
  // Validate bucket name is not empty
  if (components.bucket.empty()) {
    THROW_IO_EXCEPTION("Invalid S3 URI: missing bucket name in " + uri);
  }
  
  // Basic bucket name validation (AWS S3 naming rules)
  // Bucket names must be 3-63 characters, lowercase letters, numbers, hyphens
  if (components.bucket.length() < 3 || components.bucket.length() > 63) {
    THROW_IO_EXCEPTION("Invalid S3 bucket name: length must be 3-63 characters, got: " + components.bucket);
  }
  
  // Check for glob patterns in object key
  components.hasGlob = (components.objectKey.find('*') != std::string::npos ||
                        components.objectKey.find('?') != std::string::npos ||
                        components.objectKey.find('[') != std::string::npos);
  
  return components;
}

// S3 FileSystemProvider Implementation
function::FileInfo<arrow::fs::FileSystem> S3FileSystemProvider::provide(
    const reader::FileSchema& schema) {
  
  if (schema.paths.empty()) {
    THROW_IO_EXCEPTION("S3FileSystemProvider: no paths provided");
  }
  
  // Initialize Arrow S3 subsystem (idempotent, safe to call multiple times)
  auto init_result = arrow::fs::EnsureS3Initialized();
  if (!init_result.ok()) {
    THROW_IO_EXCEPTION("Failed to initialize Arrow S3 subsystem: " + 
                       init_result.ToString());
  }
  
  // Build S3 options
  auto s3_options = buildS3Options(schema);
  
  // Initialize S3 filesystem
  auto fs_result = arrow::fs::S3FileSystem::Make(s3_options);
  if (!fs_result.ok()) {
    LOG(ERROR) << "S3FileSystem::Make failed with status: " << fs_result.status().ToString();
    LOG(ERROR) << "  Endpoint: " << s3_options.endpoint_override;
    LOG(ERROR) << "  Region: " << s3_options.region;
    LOG(ERROR) << "  Scheme: " << s3_options.scheme;
    THROW_IO_EXCEPTION("Failed to initialize S3FileSystem: " + 
                       fs_result.status().ToString());
  }
  
  auto s3_fs = *fs_result;
  
  // Resolve paths (expand globs if needed)
  auto resolved_paths = resolveS3Paths(s3_fs, schema.paths);
  
  LOG(INFO) << "S3FileSystem initialized successfully, resolved " 
            << resolved_paths.size() << " paths";
  
  return function::FileInfo<arrow::fs::FileSystem>{resolved_paths, s3_fs};
}

arrow::fs::S3Options S3FileSystemProvider::buildS3Options(
    const reader::FileSchema& schema) {
  S3OptionsBuilder builder(schema);
  return builder.build();
}

// Glob pattern expansion for S3 paths
std::vector<std::string> S3FileSystemProvider::resolveS3Paths(
    std::shared_ptr<arrow::fs::S3FileSystem> fs,
    const std::vector<std::string>& paths) {
  std::vector<std::string> resolved_paths;

  for (const auto& path : paths) {
    auto components = S3URIComponents::parse(path);

    // Arrow FileSystem expects paths in "bucket/key" format, not "s3://bucket/key"
    std::string arrow_path = components.bucket;
    if (!components.objectKey.empty()) {
      arrow_path += "/" + components.objectKey;
    }

    if (!components.hasGlob) {
      // Direct path - just add to results
      resolved_paths.push_back(arrow_path);
      LOG(INFO) << "Direct S3 path: " << arrow_path;
    } else {
      // Glob pattern - expand using Arrow FileSystem API
      LOG(INFO) << "Expanding S3 glob pattern: " << path;

      // S3-specific: bucket is root, objectKey is pattern
      // Delegate to helper in s3_filesystem.h
      ResolvePathsWithGlobOnFs(
          fs, components.bucket, components.objectKey,
          resolved_paths, path);
    }
  }

  // Sort paths for deterministic ordering
  std::sort(resolved_paths.begin(), resolved_paths.end());

  return resolved_paths;
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
