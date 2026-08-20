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

#include <cstddef>
#include <string>
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"

namespace neug {
namespace extension {
namespace s3 {

// Centralized definition of all S3-related configuration keys.
struct S3ConfigOptionKeys {
  // Endpoint: user may specify any of these via configs or env
  static constexpr const char* kEndpointCanonical =
      "OSS_ENDPOINT";  // NeuG canonical (OSS-style)
  static constexpr const char* kEndpointAws =
      "AWS_ENDPOINT_URL";  // AWS SDK standard
  static constexpr const char* kEndpointOverride =
      "ENDPOINT_OVERRIDE";  // Alternative endpoint name

  // Region: user may specify via configs or env
  static constexpr const char* kRegionCanonical =
      "OSS_REGION";  // NeuG canonical (OSS-style)
  static constexpr const char* kRegionDefault =
      "AWS_DEFAULT_REGION";  // AWS SDK standard
  static constexpr const char* kRegionAws = "AWS_REGION";

  // Credentials: access key ID and secret access key
  // Supported credential sources (in priority order):
  //   1. Explicit options (OSS_/AWS_ access key pairs in schema.options)
  //   2. Environment variables (same key names)
  //   3. Anonymous (public buckets)
  static constexpr const char* kCredentialsKind =
      "CREDENTIALS_KIND";  // Explicit / Anonymous / Default
  static constexpr const char* kAccessKeyCanonical =
      "OSS_ACCESS_KEY_ID";  // NeuG canonical (OSS-style)
  static constexpr const char* kAccessKeyAws =
      "AWS_ACCESS_KEY_ID";  // AWS-compatible alias
  static constexpr const char* kSecretAccessKeyCanonical =
      "OSS_ACCESS_KEY_SECRET";  // NeuG canonical (OSS-style)
  static constexpr const char* kSecretAccessKeyAws =
      "AWS_SECRET_ACCESS_KEY";  // AWS-compatible alias

  // TLS / addressing
  static constexpr const char* kVerifySSL = "VERIFY_SSL";     // true/false
  static constexpr const char* kCACertFile = "CA_CERT_FILE";  // CA bundle path
  static constexpr const char* kPathStyle = "PATH_STYLE";     // true/false

  // Timeouts (seconds)
  static constexpr const char* kConnectTimeout = "CONNECT_TIMEOUT";
  static constexpr const char* kRequestTimeout = "REQUEST_TIMEOUT";
};

// Valid values for CREDENTIALS_KIND option (case-insensitive)
struct S3CredentialsKindValues {
  static constexpr const char* kExplicit =
      "explicit";  // Use explicit AK/SK from schema options only
  static constexpr const char* kAnonymous =
      "anonymous";  // No credentials (public buckets)
  static constexpr const char* kDefault =
      "default";  // options -> environment -> error if none found
};

// Logical S3 options schema (NeuG-level knobs)
// This centralizes the typed configuration (name + default value) for S3.
struct S3ParseOptions {
  // Endpoint/Region: support env fallback via getOptionWithEnv
  reader::Option<std::string> endpoint =
      reader::Option<std::string>::StringOption(
          S3ConfigOptionKeys::kEndpointCanonical, "");
  reader::Option<std::string> region =
      reader::Option<std::string>::StringOption(
          S3ConfigOptionKeys::kRegionCanonical, "");

  // Credentials kind: only from schema.options (no env fallback)
  reader::Option<std::string> credentials_kind =
      reader::Option<std::string>::StringOption(
          S3ConfigOptionKeys::kCredentialsKind, "Default");

  // Timeouts: from schema.options (no env fallback)
  reader::Option<double> connect_timeout = reader::Option<double>::DoubleOption(
      S3ConfigOptionKeys::kConnectTimeout, 5.0);
  reader::Option<double> request_timeout = reader::Option<double>::DoubleOption(
      S3ConfigOptionKeys::kRequestTimeout, 30.0);
};

/**
 * Configuration of the curl-based S3 client. A plain value object produced
 * once from FileSchema options + environment variables; immutable after
 * construction and shared by all opened streams.
 */
struct S3ClientConfig {
  // Endpoint host WITHOUT scheme, e.g. "oss-cn-beijing.aliyuncs.com" or
  // "localhost:9000". Empty means default AWS S3 ("s3.<region>.amazonaws.com").
  std::string endpoint;
  // AWS/OSS region, e.g. "us-east-1" or "oss-cn-beijing".
  std::string region = "us-east-1";
  // Credentials (explicit options or environment only; no STS/IAM role).
  std::string access_key;
  std::string secret_key;
  // Public-bucket mode: requests are sent unsigned.
  bool anonymous = false;
  // "https" or "http".
  std::string scheme = "https";
  // Path-style addressing (endpoint/bucket/key) instead of virtual hosted
  // style (bucket.endpoint/key). Auto-enabled for IP/localhost endpoints.
  bool path_style = false;
  // TLS options.
  bool verify_ssl = true;
  std::string ca_cert_file;
  // Timeouts in seconds.
  int connect_timeout = 5;
  int request_timeout = 30;
  // Retry policy for transient failures (transport errors / 5xx / 429).
  int max_retries = 3;
  // Multipart upload part size (bytes); writes smaller than this are sent
  // with a single PutObject.
  size_t multipart_part_size = 8 * 1024 * 1024;

  bool hasCredentials() const {
    return !access_key.empty() && !secret_key.empty();
  }
};

/**
 * @brief S3 Options Builder - builds an S3ClientConfig from a FileSchema.
 *
 * Configuration Names (OSS-style canonical + AWS-compatible aliases):
 * - OSS_ENDPOINT or AWS_ENDPOINT_URL or ENDPOINT_OVERRIDE: Custom S3-compatible
 *   endpoint (OSS, MinIO)
 * - OSS_REGION or AWS_DEFAULT_REGION: AWS/OSS region (e.g., "us-east-1",
 *   "oss-cn-beijing")
 * - OSS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID: Access key
 * - OSS_ACCESS_KEY_SECRET or AWS_SECRET_ACCESS_KEY: Secret key
 *
 * Credential resolution (simplified; no STS / IAM role / default chain):
 *   1. Explicit options: access key pair from schema.options
 *   2. Environment variables: same key names as env vars
 *   3. Anonymous: CREDENTIALS_KIND=Anonymous, or no credentials found
 *
 * OSS-specific handling:
 *   - Virtual hosted-style addressing is enforced for OSS endpoints
 *   - Region auto-detected from endpoint (e.g. oss-cn-hangzhou.aliyuncs.com
 *     -> oss-cn-hangzhou)
 */
class S3OptionsBuilder {
 public:
  /**
   * @brief Constructs an S3OptionsBuilder with the given file schema
   * @param schema The file schema containing S3 paths and configuration
   */
  explicit S3OptionsBuilder(const reader::FileSchema& schema)
      : schema_(schema) {}

  /**
   * @brief Build S3ClientConfig from schema configuration.
   *
   * Steps:
   * 1. Resolve endpoint override (options > env)
   * 2. Resolve region (options > env > auto-detect from OSS endpoint >
   *    us-east-1)
   * 3. Resolve credentials (explicit options > environment > anonymous)
   * 4. Apply OSS-specific addressing settings
   *
   * @return Configured S3ClientConfig instance
   */
  S3ClientConfig build() const;

 private:
  const reader::FileSchema& schema_;
  S3ParseOptions parse_options_{};

  /**
   * @brief Resolve endpoint override from options or environment
   * Checks: OSS_ENDPOINT > AWS_ENDPOINT_URL > ENDPOINT_OVERRIDE
   * @return Endpoint URL (empty if using default AWS S3)
   */
  std::string resolveEndpoint() const;

  /**
   * @brief Resolve AWS region from options, environment, or endpoint
   * Checks: OSS_REGION > AWS_DEFAULT_REGION > AWS_REGION > auto-detect
   * from OSS endpoint > us-east-1
   */
  std::string resolveRegion(const std::string& endpoint) const;

  /**
   * @brief Resolve credentials into the config:
   * explicit options > environment variables > anonymous.
   */
  void configureCredentials(S3ClientConfig& config) const;

  /**
   * @brief Detect if endpoint is Alibaba Cloud OSS
   * @return true if endpoint contains "aliyuncs.com"
   */
  static bool isOSSEndpoint(const std::string& endpoint);

  /**
   * @brief Extract region from OSS endpoint pattern
   * @param endpoint OSS endpoint (e.g., "oss-cn-beijing.aliyuncs.com")
   * @return Region string (e.g., "oss-cn-beijing")
   */
  static std::string extractOSSRegion(const std::string& endpoint);
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
