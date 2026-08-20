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

#include "s3_options.h"
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"

namespace neug {
namespace extension {
namespace s3 {

// ============================================================================
// TLS CA bundle resolution
// ============================================================================

namespace {

bool fileExistsAndReadable(const std::string& path) {
  if (path.empty())
    return false;
  struct stat st;
  if (::stat(path.c_str(), &st) != 0)
    return false;
  if (!S_ISREG(st.st_mode))
    return false;
  return ::access(path.c_str(), R_OK) == 0;
}

// Resolve a CA bundle path for libcurl. Priority:
//   1. Env var SSL_CERT_FILE
//   2. Env var CURL_CA_BUNDLE
//   3. Env var AWS_CA_BUNDLE
//   4. Common distro paths
std::string resolveTlsCaFilePath() {
  for (const char* env_key :
       {"SSL_CERT_FILE", "CURL_CA_BUNDLE", "AWS_CA_BUNDLE"}) {
    const char* v = std::getenv(env_key);
    if (v && std::strlen(v) > 0 && fileExistsAndReadable(v)) {
      LOG(INFO) << "TLS CA bundle resolved from env " << env_key << "=" << v;
      return v;
    }
  }
  static const std::vector<std::string> kCommonPaths = {
      "/etc/ssl/certs/ca-certificates.crt",  // Debian/Ubuntu
      "/etc/pki/tls/certs/ca-bundle.crt",    // CentOS/RHEL/Fedora
      "/etc/ssl/cert.pem",                   // Alpine/macOS
      "/etc/ssl/ca-bundle.pem",              // OpenSUSE
  };
  for (const auto& p : kCommonPaths) {
    if (fileExistsAndReadable(p)) {
      LOG(INFO) << "TLS CA bundle resolved from system path: " << p;
      return p;
    }
  }
  return "";
}

std::string toLowerStr(std::string s) {
  // Cast to unsigned char first: passing a negative char (non-ASCII byte
  // on platforms where char is signed) to ::tolower is undefined behavior.
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

bool parseBoolOption(const std::string& value, const std::string& key,
                     bool default_value) {
  if (value.empty()) {
    return default_value;
  }
  std::string v = toLowerStr(value);
  if (v == "true" || v == "1" || v == "yes" || v == "on") {
    return true;
  }
  if (v == "false" || v == "0" || v == "no" || v == "off") {
    return false;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("Invalid " + key + " value '" + value +
                                   "'. Expected 'true' or 'false'.");
}

// Mask a credential string for safe logging: show first 4 chars + "***".
std::string maskCredential(const std::string& value) {
  if (value.empty())
    return "(not set)";
  if (value.size() <= 4)
    return "***";
  return value.substr(0, 4) + "***";
}

// Generic helper: resolve an option with multiple key aliases and environment
// variable fallback. Priority:
//   1. schema.options[any key in option_keys]
//   2. any env in env_keys
//   3. Option default
template <typename T>
T getOptionWithEnv(const reader::options_t& options,
                   const reader::Option<T>& opt,
                   std::initializer_list<const char*> option_keys,
                   std::initializer_list<const char*> env_keys) {
  bool option_key_present = false;
  std::string option_value;

  for (const char* key : option_keys) {
    auto it = options.find(key);
    if (it != options.end()) {
      option_key_present = true;
      option_value = it->second;
      break;
    }
  }

  if (option_key_present) {
    reader::options_t temp;
    temp.emplace(opt.getKey(), option_value);
    return opt.get(temp);
  }

  for (const char* env_key : env_keys) {
    const char* env_val = std::getenv(env_key);
    if (env_val && std::strlen(env_val) > 0) {
      reader::options_t env_options;
      env_options.emplace(opt.getKey(), std::string(env_val));
      return opt.get(env_options);
    }
  }

  reader::options_t empty;
  return opt.get(empty);
}

// Look up the first non-empty value among the given option keys.
std::string findFirstOption(const reader::options_t& options,
                            std::initializer_list<const char*> keys) {
  for (const char* key : keys) {
    auto it = options.find(key);
    if (it != options.end() && !it->second.empty()) {
      return it->second;
    }
  }
  return "";
}

// Look up the first non-empty value among the given env var names.
std::string findFirstEnv(std::initializer_list<const char*> env_keys) {
  for (const char* key : env_keys) {
    const char* v = std::getenv(key);
    if (v && std::strlen(v) > 0) {
      return v;
    }
  }
  return "";
}

// Detect endpoints that require path-style addressing: bare IPs or localhost
// (typical MinIO test setups).
bool looksLikeIPorLocalhost(const std::string& host) {
  if (host.empty()) {
    return false;
  }
  std::string h = host;
  size_t colon = h.find(':');
  if (colon != std::string::npos) {
    h = h.substr(0, colon);
  }
  if (h == "localhost") {
    return true;
  }
  // All-numeric dot-separated -> IPv4
  bool all_digit_or_dot = !h.empty();
  bool has_digit = false;
  for (char c : h) {
    if (c == '.') {
      continue;
    }
    if (c >= '0' && c <= '9') {
      has_digit = true;
      continue;
    }
    all_digit_or_dot = false;
    break;
  }
  return all_digit_or_dot && has_digit;
}

}  // namespace

// ============================================================================
// Main Build Method
// ============================================================================

S3ClientConfig S3OptionsBuilder::build() const {
  const auto& options = schema_.options;
  S3ClientConfig config;

  // Step 1: Resolve endpoint (from options or env)
  std::string endpoint = resolveEndpoint();

  // Step 2: Resolve region (from options or env, or auto-detect)
  config.region = resolveRegion(endpoint);

  // Step 3: Apply endpoint configuration (strip scheme, set scheme field)
  if (!endpoint.empty()) {
    std::string host = endpoint;
    if (host.find("http://") == 0) {
      config.scheme = "http";
      host = host.substr(7);
    } else if (host.find("https://") == 0) {
      config.scheme = "https";
      host = host.substr(8);
    }
    // Strip trailing slash if present
    while (!host.empty() && host.back() == '/') {
      host.pop_back();
    }
    config.endpoint = host;
    LOG(INFO) << "Using endpoint override: " << host;
  }

  // Step 4: Addressing style.
  // OSS requires virtual hosted-style; IP/localhost endpoints use path-style.
  if (isOSSEndpoint(config.endpoint)) {
    config.path_style = false;
    LOG(INFO) << "OSS endpoint detected, using virtual hosted-style addressing";
  } else if (looksLikeIPorLocalhost(config.endpoint)) {
    config.path_style = true;
    LOG(INFO) << "IP/localhost endpoint detected, using path-style addressing";
  }
  // Explicit PATH_STYLE option overrides auto-detection.
  auto path_style_it = options.find(S3ConfigOptionKeys::kPathStyle);
  if (path_style_it != options.end()) {
    config.path_style =
        parseBoolOption(path_style_it->second, S3ConfigOptionKeys::kPathStyle,
                        config.path_style);
  }

  // Step 5: Credentials (explicit options > environment > anonymous).
  configureCredentials(config);

  // Step 6: TLS options.
  auto verify_it = options.find(S3ConfigOptionKeys::kVerifySSL);
  if (verify_it != options.end()) {
    config.verify_ssl = parseBoolOption(verify_it->second,
                                        S3ConfigOptionKeys::kVerifySSL, true);
  }
  auto ca_it = options.find(S3ConfigOptionKeys::kCACertFile);
  if (ca_it != options.end() && !ca_it->second.empty()) {
    config.ca_cert_file = ca_it->second;
  } else {
    config.ca_cert_file = resolveTlsCaFilePath();
    if (config.ca_cert_file.empty() && config.scheme == "https") {
      LOG(ERROR) << "No TLS CA bundle found. HTTPS requests (OSS / S3) may "
                    "fail with certificate verification errors. Fix: install "
                    "ca-certificates or set SSL_CERT_FILE.";
    }
  }

  // Step 7: Timeouts.
  config.connect_timeout =
      static_cast<int>(parse_options_.connect_timeout.get(options));
  config.request_timeout =
      static_cast<int>(parse_options_.request_timeout.get(options));

  // Log final configuration (credentials masked)
  LOG(INFO) << "=== S3ClientConfig ===";
  LOG(INFO) << "  Region: " << config.region;
  LOG(INFO) << "  Endpoint: "
            << (config.endpoint.empty() ? "(default AWS S3)" : config.endpoint);
  LOG(INFO) << "  Scheme: " << config.scheme;
  LOG(INFO) << "  Path-style: " << (config.path_style ? "true" : "false");
  LOG(INFO) << "  Anonymous: " << (config.anonymous ? "true" : "false");
  LOG(INFO) << "  Access key: " << maskCredential(config.access_key);
  LOG(INFO) << "  Connect timeout: " << config.connect_timeout << "s";
  LOG(INFO) << "  Request timeout: " << config.request_timeout << "s";
  LOG(INFO) << "======================";

  return config;
}

// ============================================================================
// Endpoint Resolution
// ============================================================================

std::string S3OptionsBuilder::resolveEndpoint() const {
  const auto& options = schema_.options;
  std::string endpoint = getOptionWithEnv(
      options, parse_options_.endpoint,
      {S3ConfigOptionKeys::kEndpointCanonical, S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride},
      {S3ConfigOptionKeys::kEndpointCanonical, S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride});

  if (!endpoint.empty()) {
    LOG(INFO) << "Resolved endpoint: " << endpoint;
  }

  return endpoint;
}

// ============================================================================
// Region Resolution
// ============================================================================

std::string S3OptionsBuilder::resolveRegion(const std::string& endpoint) const {
  const auto& options = schema_.options;
  std::string region = getOptionWithEnv(
      options, parse_options_.region,
      {S3ConfigOptionKeys::kRegionCanonical, S3ConfigOptionKeys::kRegionDefault,
       S3ConfigOptionKeys::kRegionAws},
      {S3ConfigOptionKeys::kRegionCanonical, S3ConfigOptionKeys::kRegionDefault,
       S3ConfigOptionKeys::kRegionAws});

  if (!region.empty()) {
    LOG(INFO) << "Using explicit region: " << region;
    return region;
  }

  // Auto-detect region from OSS endpoint
  if (!endpoint.empty() && isOSSEndpoint(endpoint)) {
    std::string oss_region = extractOSSRegion(endpoint);
    LOG(INFO) << "Auto-detected region from OSS endpoint: " << oss_region;
    return oss_region;
  }

  LOG(INFO) << "Using default region: us-east-1";
  return "us-east-1";
}

// ============================================================================
// Credentials Resolution (simplified: options > environment > anonymous)
// ============================================================================

void S3OptionsBuilder::configureCredentials(S3ClientConfig& config) const {
  const auto& options = schema_.options;

  std::string kind_str =
      toLowerStr(parse_options_.credentials_kind.get(options));

  if (kind_str == S3CredentialsKindValues::kAnonymous) {
    config.anonymous = true;
    LOG(INFO) << "Configured anonymous credentials (for public buckets)";
    return;
  }
  if (kind_str != S3CredentialsKindValues::kExplicit &&
      kind_str != S3CredentialsKindValues::kDefault) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid CREDENTIALS_KIND: " + kind_str +
        ". Valid values: 'Explicit', 'Anonymous', 'Default'. "
        "STS token and IAM/RAM role credentials are not supported.");
  }

  if (kind_str == S3CredentialsKindValues::kExplicit) {
    // Explicit mode: credentials MUST come from schema.options.
    config.access_key =
        findFirstOption(options, {S3ConfigOptionKeys::kAccessKeyCanonical,
                                  S3ConfigOptionKeys::kAccessKeyAws});
    config.secret_key =
        findFirstOption(options, {S3ConfigOptionKeys::kSecretAccessKeyCanonical,
                                  S3ConfigOptionKeys::kSecretAccessKeyAws});
    if (!config.hasCredentials()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "CREDENTIALS_KIND=Explicit requires credentials in options. "
          "Supported option keys are: "
          "OSS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID, and "
          "OSS_ACCESS_KEY_SECRET or AWS_SECRET_ACCESS_KEY.");
    }
    LOG(INFO) << "Configured explicit credentials (access_key: "
              << maskCredential(config.access_key) << ")";
    return;
  }

  // Default mode: options > environment > anonymous.
  // Note: the historical special case that injected OSS_* env vars only when
  // the endpoint was OSS is gone — env vars are honored unconditionally.
  config.access_key =
      findFirstOption(options, {S3ConfigOptionKeys::kAccessKeyCanonical,
                                S3ConfigOptionKeys::kAccessKeyAws});
  config.secret_key =
      findFirstOption(options, {S3ConfigOptionKeys::kSecretAccessKeyCanonical,
                                S3ConfigOptionKeys::kSecretAccessKeyAws});
  if (!config.hasCredentials()) {
    config.access_key = findFirstEnv({S3ConfigOptionKeys::kAccessKeyCanonical,
                                      S3ConfigOptionKeys::kAccessKeyAws});
    config.secret_key =
        findFirstEnv({S3ConfigOptionKeys::kSecretAccessKeyCanonical,
                      S3ConfigOptionKeys::kSecretAccessKeyAws});
  }

  if (config.hasCredentials()) {
    LOG(INFO) << "Configured credentials (access_key: "
              << maskCredential(config.access_key) << ")";
  } else {
    // Fail loudly instead of silently downgrading to anonymous: earlier
    // versions resolved Default through ~/.aws/credentials and IAM/ECS
    // roles, which this build does not support. A silent anonymous
    // fallback would turn those deployments into hard-to-diagnose 403s.
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CREDENTIALS_KIND=Default found no credentials in options or "
        "environment. Credentials files (~/.aws/credentials) and IAM/ECS "
        "role credentials are not supported. Provide "
        "OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET (or AWS_ACCESS_KEY_ID/"
        "AWS_SECRET_ACCESS_KEY) explicitly, or set "
        "CREDENTIALS_KIND=Anonymous for public buckets.");
  }
}

// ============================================================================
// OSS-Specific Handling
// ============================================================================

bool S3OptionsBuilder::isOSSEndpoint(const std::string& endpoint) {
  return endpoint.find("aliyuncs.com") != std::string::npos;
}

std::string S3OptionsBuilder::extractOSSRegion(const std::string& endpoint) {
  std::string ep = endpoint;

  // Remove protocol prefix
  size_t protocol_pos = ep.find("://");
  if (protocol_pos != std::string::npos) {
    ep = ep.substr(protocol_pos + 3);
  }

  // Extract region: oss-cn-beijing.aliyuncs.com -> oss-cn-beijing
  if (ep.find("oss-") == 0) {
    size_t dot_pos = ep.find('.');
    if (dot_pos != std::string::npos) {
      return ep.substr(0, dot_pos);
    }
  }

  // Fallback, assume oss-cn-hangzhou as default region
  return "oss-cn-hangzhou";
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
