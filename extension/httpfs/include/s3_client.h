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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "neug/utils/result.h"
#include "remote_io_utils.h"
#include "s3_options.h"

namespace neug {
namespace extension {
namespace s3 {

// Ensure libcurl is initialized globally (idempotent, thread-safe).
void EnsureCurlInitialized();

/**
 * Minimal S3 HTTP client built on libcurl + SigV4 signing (no aws-sdk).
 *
 * Supports AWS S3, Alibaba Cloud OSS and any S3-compatible endpoint
 * (e.g. MinIO). Covers the object operations the httpfs extension needs:
 *   - HEAD   : size / existence
 *   - GET    : ranged reads
 *   - LIST   : ListObjectsV2 (glob expansion)
 *   - PUT    : single-shot writes
 *   - POST/DELETE: multipart upload (create / upload part / complete / abort)
 *
 * Thread-safety: every request uses its own CURL handle, so a single
 * S3Client instance may be shared across threads. All handles share one
 * CURLSH connection cache (DNS / TLS session / TCP connection reuse),
 * guarded by the share's lock callbacks.
 */
class S3Client {
 public:
  explicit S3Client(S3ClientConfig config);
  ~S3Client() = default;

  const S3ClientConfig& config() const { return config_; }

  // --- Read operations ---

  /// Object size in bytes (HEAD request). Error when the object is missing.
  result<int64_t> getObjectSize(const std::string& bucket,
                                const std::string& key) const;

  /// Whether the object exists (HEAD request; 404 -> false).
  result<bool> objectExists(const std::string& bucket,
                            const std::string& key) const;

  /// Ranged GET: read up to `length` bytes starting at `offset` into `out`.
  /// Returns the number of bytes actually read (may be < length near EOF).
  result<int64_t> getObjectRange(const std::string& bucket,
                                 const std::string& key, int64_t offset,
                                 int64_t length, void* out) const;

  // --- List ---

  /// List all object keys under `prefix` (handles continuation tokens).
  result<std::vector<std::string>> listObjects(const std::string& bucket,
                                               const std::string& prefix) const;

  // --- Write operations ---

  /// Single-shot PutObject.
  result<void> putObject(const std::string& bucket, const std::string& key,
                         const void* data, int64_t length) const;

  /// CreateMultipartUpload; returns the upload id.
  result<std::string> createMultipartUpload(const std::string& bucket,
                                            const std::string& key) const;

  /// UploadPart; returns the ETag of the uploaded part.
  result<std::string> uploadPart(const std::string& bucket,
                                 const std::string& key,
                                 const std::string& upload_id, int part_number,
                                 const void* data, int64_t length) const;

  /// CompleteMultipartUpload with (part number, ETag) pairs.
  result<void> completeMultipartUpload(
      const std::string& bucket, const std::string& key,
      const std::string& upload_id,
      const std::vector<std::pair<int, std::string>>& parts) const;

  /// AbortMultipartUpload (best effort; errors are reported but callers
  /// typically ignore them during cleanup).
  result<void> abortMultipartUpload(const std::string& bucket,
                                    const std::string& key,
                                    const std::string& upload_id) const;

 private:
  struct HttpResponse {
    long http_code = 0;
    std::string body;
    // Selected response headers (lowercase name -> value), e.g. "etag".
    std::vector<std::pair<std::string, std::string>> headers;
  };

  // Issue a signed request with retry on transient failures.
  // `body`/`body_len` describe the request payload (nullptr for none).
  // `received_bytes` (optional) returns how many bytes were written into
  // `range_out` for ranged GET responses.
  // `out_http_code` (optional) receives the last HTTP status code observed
  // when the request fails, so callers can branch on the code instead of
  // parsing the error message (0 if no response was ever received).
  result<HttpResponse> request(
      const std::string& method, const std::string& bucket,
      const std::string& key,
      const std::vector<std::pair<std::string, std::string>>& query_params,
      const std::vector<std::pair<std::string, std::string>>& extra_headers,
      const void* body, int64_t body_len, bool capture_body, void* range_out,
      int64_t range_out_capacity, int64_t* received_bytes = nullptr,
      long* out_http_code = nullptr) const;

  S3ClientConfig config_;
  // Shared DNS/TLS/connection cache; lives as long as the client and is
  // referenced by every request() handle via CURLOPT_SHARE.
  std::shared_ptr<CurlShare> curl_share_;
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
