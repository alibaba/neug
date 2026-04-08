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
#include "neug/utils/file_sys/file_system.h"

namespace {
// Thin wrapper that adapts a shared_ptr<arrow::fs::S3FileSystem> to the
// unique_ptr<arrow::fs::FileSystem> ownership model required by
// fsys::FileSystem::toArrowFileSystem().  All calls are delegated to the
// underlying shared instance.
class S3FileSystemWrapper : public arrow::fs::FileSystem {
 public:
  explicit S3FileSystemWrapper(
      std::shared_ptr<arrow::fs::S3FileSystem> inner)
      : arrow::fs::FileSystem(), inner_(std::move(inner)) {}

  std::string type_name() const override { return inner_->type_name(); }

  bool Equals(const arrow::fs::FileSystem& other) const override {
    return inner_->Equals(other);
  }

  arrow::Result<std::string> NormalizePath(std::string path) override {
    return inner_->NormalizePath(std::move(path));
  }

  arrow::Result<std::string> PathFromUri(
      const std::string& uri_string) const override {
    return inner_->PathFromUri(uri_string);
  }

  arrow::Result<std::string> MakeUri(std::string path) const override {
    return inner_->MakeUri(std::move(path));
  }

  // Pure-virtual overrides
  arrow::Result<arrow::fs::FileInfo> GetFileInfo(
      const std::string& path) override {
    return inner_->GetFileInfo(path);
  }
  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const arrow::fs::FileSelector& selector) override {
    return inner_->GetFileInfo(selector);
  }

  // Non-pure overrides: delegate to inner_ for correct S3 behavior
  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const std::vector<std::string>& paths) override {
    return inner_->GetFileInfo(paths);
  }

  arrow::Status CreateDir(const std::string& path,
                          bool recursive) override {
    return inner_->CreateDir(path, recursive);
  }
  arrow::Status DeleteDir(const std::string& path) override {
    return inner_->DeleteDir(path);
  }
  arrow::Status DeleteDirContents(const std::string& path,
                                  bool missing_dir_ok) override {
    return inner_->DeleteDirContents(path, missing_dir_ok);
  }
  arrow::Status DeleteRootDirContents() override {
    return inner_->DeleteRootDirContents();
  }
  arrow::Status DeleteFile(const std::string& path) override {
    return inner_->DeleteFile(path);
  }
  arrow::Status DeleteFiles(
      const std::vector<std::string>& paths) override {
    return inner_->DeleteFiles(paths);
  }
  arrow::Status Move(const std::string& src,
                     const std::string& dest) override {
    return inner_->Move(src, dest);
  }
  arrow::Status CopyFile(const std::string& src,
                         const std::string& dest) override {
    return inner_->CopyFile(src, dest);
  }

  // OpenInputStream: both string-path and FileInfo overloads
  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const std::string& path) override {
    return inner_->OpenInputStream(path);
  }
  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const arrow::fs::FileInfo& info) override {
    return inner_->OpenInputStream(info);
  }

  // OpenInputFile: both string-path and FileInfo overloads
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override {
    return inner_->OpenInputFile(path);
  }
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const arrow::fs::FileInfo& info) override {
    return inner_->OpenInputFile(info);
  }

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override {
    return inner_->OpenOutputStream(path, metadata);
  }
  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override {
    return inner_->OpenAppendStream(path, metadata);
  }

 private:
  std::shared_ptr<arrow::fs::S3FileSystem> inner_;
};
}  // anonymous namespace

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

// S3 FileSystem Implementation
S3FileSystem::S3FileSystem(const reader::FileSchema& schema) {
  if (schema.paths.empty()) {
    THROW_IO_EXCEPTION("S3FileSystem: no paths provided");
  }

  // Initialize Arrow S3 subsystem (idempotent, safe to call multiple times)
  auto init_result = arrow::fs::EnsureS3Initialized();
  if (!init_result.ok()) {
    THROW_IO_EXCEPTION("Failed to initialize Arrow S3 subsystem: " +
                       init_result.ToString());
  }

  // Build S3 options and create the Arrow S3FileSystem
  auto s3_options = buildS3Options(schema);
  auto fs_result = arrow::fs::S3FileSystem::Make(s3_options);
  if (!fs_result.ok()) {
    LOG(ERROR) << "S3FileSystem::Make failed with status: "
               << fs_result.status().ToString();
    LOG(ERROR) << "  Endpoint: " << s3_options.endpoint_override;
    LOG(ERROR) << "  Region: " << s3_options.region;
    LOG(ERROR) << "  Scheme: " << s3_options.scheme;
    THROW_IO_EXCEPTION("Failed to initialize S3FileSystem: " +
                       fs_result.status().ToString());
  }
  arrow_fs_ = *fs_result;
  LOG(INFO) << "S3FileSystem initialized successfully";
}

std::vector<std::string> S3FileSystem::glob(const std::string& path) {
  auto components = S3URIComponents::parse(path);

  // Arrow FileSystem expects paths in "bucket/key" format, not "s3://bucket/key"
  std::string arrow_path = components.bucket;
  if (!components.objectKey.empty()) {
    arrow_path += "/" + components.objectKey;
  }

  if (!components.hasGlob) {
    // Direct path - no expansion needed
    LOG(INFO) << "Direct S3 path: " << arrow_path;
    return {arrow_path};
  }

  // Glob pattern - expand using Arrow FileSystem API
  LOG(INFO) << "Expanding S3 glob pattern: " << path;
  std::vector<std::string> out_paths;
  ResolvePathsWithGlobOnFs(arrow_fs_, components.bucket, components.objectKey,
                           out_paths, path);
  return out_paths;
}

std::unique_ptr<arrow::fs::FileSystem> S3FileSystem::toArrowFileSystem() {
  return std::make_unique<S3FileSystemWrapper>(arrow_fs_);
}

arrow::fs::S3Options S3FileSystem::buildS3Options(
    const reader::FileSchema& schema) {
  S3OptionsBuilder builder(schema);
  return builder.build();
}

// Glob pattern expansion for S3 paths
std::vector<std::string> S3FileSystem::resolveS3Paths(
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

std::unique_ptr<fsys::FileSystem> CreateS3FileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<S3FileSystem>(schema);
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
