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
#include <ctime>
#include <string>
#include <utility>
#include <vector>

namespace neug {
namespace extension {
namespace s3 {

// ============================================================================
// AWS Signature Version 4 signing utilities (no aws-sdk dependency).
//
// Implements the canonical SigV4 algorithm:
//   CanonicalRequest -> StringToSign -> SigningKey -> Signature
// Reference:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-auth.html OSS
// (Alibaba Cloud) is SigV4-compatible and uses the same algorithm with service
// name "s3".
// ============================================================================

struct SigV4Credentials {
  std::string access_key;
  std::string secret_key;

  bool empty() const { return access_key.empty() || secret_key.empty(); }
};

/// An S3 request to be signed.
struct SigV4Request {
  std::string method;         // GET / PUT / POST / HEAD / DELETE
  std::string host;           // e.g. "bucket.s3.amazonaws.com"
  std::string canonical_uri;  // URI-encoded path, e.g. "/bucket/key"
  /// Query parameters as raw (un-encoded) key/value pairs.
  std::vector<std::pair<std::string, std::string>> query_params;
  /// Extra request headers (raw, un-encoded), e.g. {"range", "bytes=0-9"}.
  /// host / x-amz-date / x-amz-content-sha256 are added by the signer and
  /// MUST NOT be supplied here.
  std::vector<std::pair<std::string, std::string>> extra_headers;
  /// Hex SHA256 of the request payload, or "UNSIGNED-PAYLOAD".
  std::string payload_hash;
};

/// Result of signing: everything the caller needs to send the request.
struct SigV4SignedRequest {
  /// ISO8601 basic format timestamp, e.g. "20130524T000000Z".
  std::string amz_date;
  /// Date stamp, e.g. "20130524".
  std::string date_stamp;
  /// Value of the Authorization header.
  std::string authorization;
  /// All headers that must be sent with the request (host, x-amz-date,
  /// x-amz-content-sha256, plus the signed extra headers).
  std::vector<std::pair<std::string, std::string>> headers;
};

/// Compute lowercase hex SHA256 of a byte buffer.
std::string SHA256Hex(const void* data, size_t len);
inline std::string SHA256Hex(const std::string& data) {
  return SHA256Hex(data.data(), data.size());
}

/// SHA256 of the empty payload (common constant).
inline const char* EmptyPayloadSHA256() {
  return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
}

/// Lowercase hex encoding of a byte buffer.
std::string HexEncode(const unsigned char* data, size_t len);

/// RFC3986 percent-encoding. Encodes every byte except unreserved characters
/// (A-Z a-z 0-9 - . _ ~); '/' is preserved unless encode_slash is true.
std::string UriEncode(const std::string& input, bool encode_slash);

/// Format a UTC time as ISO8601 basic ("YYYYMMDD'T'HHMMSS'Z'").
std::string Iso8601BasicFormat(std::time_t time_utc);

/// Sign an S3 request with SigV4.
///
/// @param creds       Access key / secret key. When empty (anonymous access)
///                    the request is NOT signed: returned authorization is
///                    empty and only host is listed among headers.
/// @param region      AWS region, e.g. "us-east-1" / "oss-cn-hangzhou".
/// @param service     Service name; always "s3" for S3 and OSS.
/// @param request     Request to sign (host, path, query, headers, payload).
/// @param timestamp   Explicit signing time. Pass 0 to use the current time.
/// @return            Signed request with Authorization header.
SigV4SignedRequest SignSigV4(const SigV4Credentials& creds,
                             const std::string& region,
                             const std::string& service,
                             const SigV4Request& request,
                             std::time_t timestamp = 0);

}  // namespace s3
}  // namespace extension
}  // namespace neug
