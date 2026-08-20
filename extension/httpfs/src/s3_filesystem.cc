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
#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include "neug/utils/exception/exception.h"
#include "remote_io_utils.h"
#include "s3_options.h"

namespace neug {
namespace extension {
namespace s3 {

// ============================================================================
// S3URIComponents
// ============================================================================

S3URIComponents S3URIComponents::parse(const std::string& uri) {
  S3URIComponents components;

  // Find scheme
  size_t scheme_end = uri.find("://");
  if (scheme_end == std::string::npos) {
    THROW_IO_EXCEPTION("Invalid S3 URI (missing scheme): " + uri);
  }

  components.scheme = uri.substr(0, scheme_end);
  if (components.scheme != "s3" && components.scheme != "oss") {
    THROW_IO_EXCEPTION("Invalid S3 URI scheme (expected s3 or oss): " +
                       components.scheme);
  }

  // Parse bucket and object key
  std::string path = uri.substr(scheme_end + 3);
  size_t slash_pos = path.find('/');

  if (slash_pos == std::string::npos) {
    components.bucket = path;
    components.objectKey = "";
  } else {
    components.bucket = path.substr(0, slash_pos);
    components.objectKey = path.substr(slash_pos + 1);
  }

  if (components.bucket.empty()) {
    THROW_IO_EXCEPTION("Invalid S3 URI: missing bucket name in " + uri);
  }

  components.hasGlob = HasGlobWildcard(components.objectKey);
  return components;
}

S3URIComponents S3URIComponents::parseFlexible(const std::string& path) {
  if (path.find("://") != std::string::npos) {
    return parse(path);
  }
  // Bare "bucket/key" form (e.g. produced by older glob() outputs).
  S3URIComponents components;
  components.scheme = "s3";
  size_t slash_pos = path.find('/');
  if (slash_pos == std::string::npos) {
    components.bucket = path;
    components.objectKey = "";
  } else {
    components.bucket = path.substr(0, slash_pos);
    components.objectKey = path.substr(slash_pos + 1);
  }
  if (components.bucket.empty()) {
    THROW_IO_EXCEPTION("Invalid S3 path: missing bucket name in " + path);
  }
  components.hasGlob = HasGlobWildcard(components.objectKey);
  return components;
}

std::string S3URIComponents::toURI() const {
  std::string uri = scheme + "://" + bucket;
  if (!objectKey.empty()) {
    uri += "/" + objectKey;
  }
  return uri;
}

// ============================================================================
// S3RandomAccessStream — ranged reads over an S3 object
// ============================================================================

namespace {

class S3RandomAccessStream : public fsys::RandomAccessStream {
 public:
  S3RandomAccessStream(std::shared_ptr<S3Client> client, std::string bucket,
                       std::string key)
      : client_(std::move(client)),
        bucket_(std::move(bucket)),
        key_(std::move(key)) {}

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
                          "S3 stream is closed: " + describe());
    }
    if (position < 0 || nbytes < 0) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid read: position=" + std::to_string(position) +
                              ", nbytes=" + std::to_string(nbytes));
    }
    if (nbytes == 0) {
      return int64_t{0};
    }
    GS_AUTO(size, ensureSize());
    if (position >= size) {
      return int64_t{0};
    }
    int64_t to_read = std::min(nbytes, size - position);
    const int64_t served = readahead_.tryServe(position, to_read, out);
    if (served >= 0) {
      return served;
    }
    // Fetch at least the readahead window so subsequent sequential small
    // reads hit the cache instead of issuing one ranged GET per read.
    int64_t fetch_len = std::max(to_read, kRemoteReadaheadBytes);
    fetch_len = std::min(fetch_len, size - position);
    fetch_buf_.resize(static_cast<size_t>(fetch_len));
    GS_AUTO(got, client_->getObjectRange(bucket_, key_, position, fetch_len,
                                         fetch_buf_.data()));
    readahead_.store(position, fetch_buf_.data(), got);
    const int64_t copy_len = std::min(got, to_read);
    if (copy_len > 0) {
      std::memcpy(out, fetch_buf_.data(), static_cast<size_t>(copy_len));
    }
    return copy_len;
  }

  result<int64_t> GetSize() override {
    if (closed_) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "S3 stream is closed: " + describe());
    }
    return ensureSize();
  }

  void Close() override { closed_ = true; }

 private:
  result<int64_t> ensureSize() {
    if (size_ >= 0) {
      return size_;
    }
    GS_ASSIGN(size_, client_->getObjectSize(bucket_, key_));
    return size_;
  }

  std::string describe() const { return bucket_ + "/" + key_; }

  std::shared_ptr<S3Client> client_;
  std::string bucket_;
  std::string key_;
  int64_t position_ = 0;
  int64_t size_ = -1;
  bool closed_ = false;
  ReadaheadCache readahead_;
  std::vector<char> fetch_buf_;
};

// ============================================================================
// S3OutputStream — buffered writes via PutObject / Multipart Upload
// ============================================================================

class S3OutputStream : public fsys::OutputStream {
 public:
  S3OutputStream(std::shared_ptr<S3Client> client, std::string bucket,
                 std::string key)
      : client_(std::move(client)),
        bucket_(std::move(bucket)),
        key_(std::move(key)),
        part_size_(client_->config().multipart_part_size) {}

  ~S3OutputStream() override {
    // Abort a dangling multipart upload if Close() was never called
    // successfully. Best-effort: ignore failures in the destructor.
    if (started_ && !completed_) {
      auto r = client_->abortMultipartUpload(bucket_, key_, upload_id_);
      if (!r) {
        LOG(WARNING) << "Failed to abort dangling multipart upload for "
                     << describe() << ": " << r.error().ToString();
      }
    }
  }

  result<void> Write(const void* data, int64_t nbytes) override {
    if (closed_) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "S3 output stream is closed: " + describe());
    }
    if (nbytes < 0) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Negative write length");
    }
    if (nbytes == 0) {
      return {};
    }
    const char* bytes = static_cast<const char*>(data);
    buffer_.append(bytes, static_cast<size_t>(nbytes));

    // Flush complete parts, but always keep at most one part buffered so a
    // chunk smaller than 5 MiB is only ever uploaded as the legal last part.
    while (buffer_.size() >= 2 * part_size_) {
      auto r = flushPart(part_size_);
      if (!r) {
        return tl::unexpected(r.error());
      }
    }
    return {};
  }

  result<void> Close() override {
    if (closed_) {
      return {};  // idempotent
    }
    closed_ = true;

    if (!started_ && parts_.empty()) {
      // Small object: single PutObject.
      return client_->putObject(bucket_, key_, buffer_.data(),
                                static_cast<int64_t>(buffer_.size()));
    }

    // Flush the tail (possibly smaller than part_size — legal last part).
    if (!buffer_.empty()) {
      auto r = flushPart(static_cast<int64_t>(buffer_.size()));
      if (!r) {
        return tl::unexpected(r.error());
      }
    }

    auto r =
        client_->completeMultipartUpload(bucket_, key_, upload_id_, parts_);
    if (!r) {
      // Best-effort abort so we don't leak an unfinished upload.
      auto abort_r = client_->abortMultipartUpload(bucket_, key_, upload_id_);
      if (!abort_r) {
        LOG(WARNING) << "Failed to abort multipart upload for " << describe()
                     << ": " << abort_r.error().ToString();
      }
      return tl::unexpected(r.error());
    }
    completed_ = true;
    LOG(INFO) << "Multipart upload completed: " << describe() << " ("
              << parts_.size() << " parts)";
    return {};
  }

  result<void> Abort() override {
    if (closed_) {
      return {};  // idempotent
    }
    closed_ = true;
    buffer_.clear();
    parts_.clear();
    if (!started_) {
      // Nothing has been uploaded yet; nothing to abort.
      return {};
    }
    auto r = client_->abortMultipartUpload(bucket_, key_, upload_id_);
    if (!r) {
      return tl::unexpected(r.error());
    }
    LOG(INFO) << "Multipart upload aborted: " << describe();
    return {};
  }

 private:
  // Upload the first `len` bytes of buffer_ as the next part.
  result<void> flushPart(int64_t len) {
    if (!started_) {
      GS_ASSIGN(upload_id_, client_->createMultipartUpload(bucket_, key_));
      started_ = true;
      LOG(INFO) << "Multipart upload started: " << describe()
                << ", upload_id=" << upload_id_;
    }
    int part_number = static_cast<int>(parts_.size()) + 1;
    GS_AUTO(etag, client_->uploadPart(bucket_, key_, upload_id_, part_number,
                                      buffer_.data(), len));
    parts_.emplace_back(part_number, etag);
    buffer_.erase(0, static_cast<size_t>(len));
    return {};
  }

  std::string describe() const { return bucket_ + "/" + key_; }

  std::shared_ptr<S3Client> client_;
  std::string bucket_;
  std::string key_;
  size_t part_size_;
  std::string buffer_;
  std::string upload_id_;
  std::vector<std::pair<int, std::string>> parts_;
  bool started_ = false;
  bool completed_ = false;
  bool closed_ = false;
};

// ============================================================================
// S3RemoteFileSystem — fsys::RemoteFileSystem over S3Client
// ============================================================================

class S3RemoteFileSystem : public fsys::RemoteFileSystem {
 public:
  explicit S3RemoteFileSystem(std::shared_ptr<S3Client> client)
      : client_(std::move(client)) {}

  result<std::shared_ptr<fsys::RandomAccessStream>> openInputStream(
      const std::string& path) override {
    try {
      auto comps = S3URIComponents::parseFlexible(path);
      if (comps.objectKey.empty()) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                            "Cannot open S3 bucket as input stream: " + path);
      }
      return std::shared_ptr<fsys::RandomAccessStream>(
          std::make_shared<S3RandomAccessStream>(client_, comps.bucket,
                                                 comps.objectKey));
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT, e.what());
    }
  }

  result<std::shared_ptr<fsys::OutputStream>> openOutputStream(
      const std::string& path) override {
    try {
      auto comps = S3URIComponents::parseFlexible(path);
      if (comps.objectKey.empty()) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                            "Cannot open S3 bucket as output stream: " + path);
      }
      return std::shared_ptr<fsys::OutputStream>(
          std::make_shared<S3OutputStream>(client_, comps.bucket,
                                           comps.objectKey));
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT, e.what());
    }
  }

  result<bool> exists(const std::string& path) override {
    try {
      auto comps = S3URIComponents::parseFlexible(path);
      if (comps.objectKey.empty()) {
        return false;
      }
      return client_->objectExists(comps.bucket, comps.objectKey);
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT, e.what());
    }
  }

  result<int64_t> getSize(const std::string& path) override {
    try {
      auto comps = S3URIComponents::parseFlexible(path);
      if (comps.objectKey.empty()) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                            "Cannot get size of S3 bucket: " + path);
      }
      return client_->getObjectSize(comps.bucket, comps.objectKey);
    } catch (const std::exception& e) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT, e.what());
    }
  }

 private:
  std::shared_ptr<S3Client> client_;
};

}  // namespace

// ============================================================================
// S3FileSystem Implementation
// ============================================================================

S3FileSystem::S3FileSystem(const reader::FileSchema& schema) {
  if (schema.paths.empty()) {
    THROW_IO_EXCEPTION("S3FileSystem: no paths provided");
  }

  // Validate all paths are S3/OSS URIs
  for (const auto& path : schema.paths) {
    try {
      S3URIComponents::parse(path);
    } catch (const exception::Exception& e) {
      THROW_IO_EXCEPTION("Invalid S3 path: " + path + " - " + e.what());
    }
  }

  auto config = buildS3Config(schema);
  client_ = std::make_shared<S3Client>(std::move(config));
  remote_fs_ = std::make_shared<S3RemoteFileSystem>(client_);

  LOG(INFO) << "S3FileSystem initialized successfully";
}

std::vector<std::string> S3FileSystem::glob(const std::string& path) {
  auto components = S3URIComponents::parse(path);

  if (!components.hasGlob) {
    // Direct path - no expansion needed
    LOG(INFO) << "Direct S3 path: " << path;
    return {path};
  }

  // Glob pattern - expand via ListObjectsV2
  LOG(INFO) << "Expanding S3 glob pattern: " << path;
  std::string prefix = LongestGlobPrefix(components.objectKey);

  auto keys_result = client_->listObjects(components.bucket, prefix);
  if (!keys_result) {
    THROW_IO_EXCEPTION("Failed to list S3 objects under " + components.bucket +
                       "/" + prefix + ": " + keys_result.error().ToString());
  }

  std::vector<std::string> matched;
  for (const auto& key : *keys_result) {
    if (MatchGlobPattern(key, components.objectKey)) {
      S3URIComponents obj = components;
      obj.objectKey = key;
      obj.hasGlob = false;
      matched.push_back(obj.toURI());
    }
  }
  std::sort(matched.begin(), matched.end());

  if (matched.empty()) {
    THROW_IO_EXCEPTION("No files matched glob pattern: " + path);
  }
  LOG(INFO) << "Glob expansion matched " << matched.size() << " object(s)";
  return matched;
}

std::shared_ptr<fsys::RemoteFileSystem> S3FileSystem::getRemoteFileSystem()
    const {
  return remote_fs_;
}

S3ClientConfig S3FileSystem::buildS3Config(const reader::FileSchema& schema) {
  S3OptionsBuilder builder(schema);
  return builder.build();
}

std::unique_ptr<fsys::FileSystem> CreateS3FileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<S3FileSystem>(schema);
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
