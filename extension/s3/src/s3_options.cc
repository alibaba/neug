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
#include <algorithm>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"

namespace neug {
namespace extension {
namespace s3 {

// ============================================================================
// Helper: Get option with environment variable fallback
// Only used for configuration values that may come from deployment environment
// ============================================================================

namespace {

// Generic helper: resolve an option with multiple key aliases and environment variable fallback.
// Priority:
//   1. schema.options[any key in option_keys]
//   2. any env in env_keys
//   3. Option default
template <typename T>
T getOptionWithEnv(const reader::options_t& options,
                   const reader::Option<T>& opt,
                   std::initializer_list<const char*> option_keys,
                   std::initializer_list<const char*> env_keys) {
  // Priority 1: schema.options - check if any key is present (even if value is empty)
  // An explicitly set empty value should override environment variables
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
  
  // If any option key is present in schema.options, use it (even if empty)
  if (option_key_present) {
    reader::options_t temp;
    temp.emplace(opt.getKey(), option_value);
    return opt.get(temp);
  }

  // Priority 2: environment variables (only if no option keys were present)
  for (const char* env_key : env_keys) {
    const char* env_val = std::getenv(env_key);
    if (env_val && std::strlen(env_val) > 0) {
      reader::options_t env_options;
      env_options.emplace(opt.getKey(), std::string(env_val));
      return opt.get(env_options);
    }
  }

  // Priority 3: Option default
  reader::options_t empty;
  return opt.get(empty);
}

}  // anonymous namespace

// ============================================================================
// Main Build Method
// ============================================================================

arrow::fs::S3Options S3OptionsBuilder::build() const {
  const auto& options = schema_.options;
  
  // Start from Arrow's defaults
  arrow::fs::S3Options s3_options = arrow::fs::S3Options::Defaults();
  
  // Step 1: Resolve endpoint (from options or env)
  std::string endpoint = resolveEndpoint();
  
  // Step 2: Resolve region (from options or env, or auto-detect)
  s3_options.region = resolveRegion(endpoint);
  
  // Step 3: Apply endpoint configuration
  if (!endpoint.empty()) {
    s3_options.endpoint_override = endpoint;
    
    // Configure HTTP/HTTPS scheme based on endpoint
    if (endpoint.find("http://") == 0) {
      s3_options.scheme = "http";
      s3_options.endpoint_override = endpoint.substr(7);
    } else if (endpoint.find("https://") == 0) {
      s3_options.scheme = "https";
      s3_options.endpoint_override = endpoint.substr(8);
    } else {
      s3_options.scheme = "https";  // Default to HTTPS
    }
    
    // Apply OSS-specific settings if needed
    applyOSSSettings(s3_options, endpoint);
    
    LOG(INFO) << "Using endpoint override: " << s3_options.endpoint_override;
  }
  
  // Step 4: Configure credentials (pass is_oss flag for Default mode OSS compatibility)
  bool is_oss = isOSSEndpoint(endpoint);
  configureCredentials(s3_options, is_oss);
  
  // Step 5: Configure timeouts
  s3_options.connect_timeout = parse_options_.connect_timeout.get(options);
  s3_options.request_timeout = parse_options_.request_timeout.get(options);
  
  // Step 6: Other settings
  s3_options.background_writes = false;  // We only read, not write for now
  
  // Log final configuration
  LOG(INFO) << "=== S3Options Configuration ===";
  LOG(INFO) << "  Region: " << s3_options.region;
  LOG(INFO) << "  Endpoint: " << (s3_options.endpoint_override.empty() ? "(default AWS S3)" : s3_options.endpoint_override);
  LOG(INFO) << "  Scheme: " << s3_options.scheme;
  LOG(INFO) << "  Connect timeout: " << s3_options.connect_timeout << "s";
  LOG(INFO) << "  Request timeout: " << s3_options.request_timeout << "s";
  LOG(INFO) << "  Access key: " << (s3_options.GetAccessKey().empty() ? "(not set)" : s3_options.GetAccessKey().substr(0, 8) + "...");
  LOG(INFO) << "  Credentials kind: " << static_cast<int>(s3_options.credentials_kind);
  LOG(INFO) << "===============================";
  
  return s3_options;
}

// ============================================================================
// Endpoint Resolution
// ============================================================================

std::string S3OptionsBuilder::resolveEndpoint() const {
  const auto& options = schema_.options;
  // Priority: schema.options[OSS_ENDPOINT or AWS_ENDPOINT_URL or ENDPOINT_OVERRIDE] >
  //           env(OSS_ENDPOINT or AWS_ENDPOINT_URL or ENDPOINT_OVERRIDE) > empty
  std::string endpoint = getOptionWithEnv(
      options,
      parse_options_.endpoint,
      {S3ConfigOptionKeys::kEndpointCanonical,
       S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride},  // schema.options keys
      {S3ConfigOptionKeys::kEndpointCanonical,
       S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride}   // env keys
  );
  
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
  // Priority: schema.options[OSS_REGION or AWS_DEFAULT_REGION] >
  //           env(OSS_REGION or AWS_DEFAULT_REGION) > auto-detect from endpoint > us-east-1
  std::string region = getOptionWithEnv(
      options,
      parse_options_.region,
      {S3ConfigOptionKeys::kRegionCanonical,
       S3ConfigOptionKeys::kRegionDefault},  // schema.options keys
      {S3ConfigOptionKeys::kRegionCanonical,
       S3ConfigOptionKeys::kRegionDefault}   // env keys
  );
  
  if (!region.empty()) {
    LOG(INFO) << "Using explicit region: " << region;
    return region;
  }
  
  // Auto-detect region from endpoint
  if (!endpoint.empty()) {
    if (isOSSEndpoint(endpoint)) {
      std::string oss_region = extractOSSRegion(endpoint);
      LOG(INFO) << "Auto-detected region from OSS endpoint: " << oss_region;
      return oss_region;
    }
  }
  
  // Default to us-east-1 (AWS default)
  LOG(INFO) << "Using default region: us-east-1";
  return "us-east-1";
}

// ============================================================================
// Credentials Resolution
// ============================================================================

arrow::fs::S3CredentialsKind S3OptionsBuilder::resolveCredentialsKind() const {
  const auto& options = schema_.options;
  
  // Get CREDENTIALS_KIND from schema.options ONLY (not from environment)
  std::string kind_str = parse_options_.credentials_kind.get(options);
  
  // Normalize to lowercase for comparison
  std::transform(kind_str.begin(), kind_str.end(), kind_str.begin(), ::tolower);
  
  if (kind_str == S3CredentialsKindValues::kExplicit) {
    return arrow::fs::S3CredentialsKind::Explicit;
  } else if (kind_str == S3CredentialsKindValues::kAnonymous) {
    return arrow::fs::S3CredentialsKind::Anonymous;
  } else if (kind_str == S3CredentialsKindValues::kDefault) {
    return arrow::fs::S3CredentialsKind::Default;
  } else if (kind_str == S3CredentialsKindValues::kRole) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CREDENTIALS_KIND='Role' is not supported yet. "
        "Use 'Explicit', 'Anonymous', or 'Default' instead.");
  } else if (kind_str == S3CredentialsKindValues::kWebIdentity) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CREDENTIALS_KIND='WebIdentity' is not supported yet. "
        "Use 'Explicit', 'Anonymous', or 'Default' instead.");
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid CREDENTIALS_KIND: " + kind_str + 
        ". Valid values: 'Explicit', 'Anonymous', 'Default'");
  }
}

void S3OptionsBuilder::configureCredentials(arrow::fs::S3Options& s3_options, bool is_oss) const {
  const auto& options = schema_.options;
  
  // Step 1: Determine credentials kind
  arrow::fs::S3CredentialsKind kind = resolveCredentialsKind();
  
  // Step 2: Configure based on the determined kind
  switch (kind) {
    case arrow::fs::S3CredentialsKind::Explicit: {
      // Explicit mode: read credentials from schema.options using OSS-style canonical
      // names (OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET) or AWS-compatible aliases.
      std::string access_key;
      std::string secret_key;

      // Resolve access key: try NeuG canonical first, then AWS alias
      for (const char* key : {
               S3ConfigOptionKeys::kAccessKeyCanonical,
               S3ConfigOptionKeys::kAccessKeyAws}) {
        auto it = options.find(key);
        if (it != options.end() && !it->second.empty()) {
          access_key = it->second;
          break;
        }
      }

      // Resolve secret key: try NeuG canonical first, then AWS alias
      for (const char* key : {
               S3ConfigOptionKeys::kSecretAccessKeyCanonical,
               S3ConfigOptionKeys::kSecretAccessKeyAws}) {
        auto it = options.find(key);
        if (it != options.end() && !it->second.empty()) {
          secret_key = it->second;
          break;
        }
      }

      if (access_key.empty() || secret_key.empty()) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "CREDENTIALS_KIND=Explicit requires credentials in options. "
            "Supported option keys are: "
            "OSS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID, and "
            "OSS_ACCESS_KEY_SECRET or AWS_SECRET_ACCESS_KEY.");
      }
      
      s3_options.ConfigureAccessKey(access_key, secret_key);
      LOG(INFO) << "Configured explicit credentials (access_key: " 
                << access_key.substr(0, 8) << "...)";
      break;
    }
    
    case arrow::fs::S3CredentialsKind::Anonymous:
      // Anonymous mode: no credentials (for public buckets)
      s3_options.ConfigureAnonymousCredentials();
      LOG(INFO) << "Configured anonymous credentials (for public buckets)";
      break;
    
    case arrow::fs::S3CredentialsKind::Default: {
      // Default mode with OSS compatibility:
      // - If endpoint is OSS AND OSS_ACCESS_KEY_ID + OSS_ACCESS_KEY_SECRET are set in env, use them explicitly
      // - Otherwise, delegate to Arrow's default credential chain (AWS_ACCESS_KEY_ID, ~/.aws/credentials, IAM, etc.)
      
      if (is_oss) {
        const char* oss_access_key_env = std::getenv(S3ConfigOptionKeys::kAccessKeyCanonical);
        const char* oss_secret_key_env = std::getenv(S3ConfigOptionKeys::kSecretAccessKeyCanonical);
        
        if (oss_access_key_env && oss_secret_key_env && 
            strlen(oss_access_key_env) > 0 && strlen(oss_secret_key_env) > 0) {
          // OSS endpoint + OSS-style env vars found, use them explicitly
          s3_options.ConfigureAccessKey(oss_access_key_env, oss_secret_key_env);
          LOG(INFO) << "Configured credentials from OSS environment variables (OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)";
          break;
        }
      }
      
      // Fall back to Arrow's default credential provider chain
      // Arrow will check: AWS_ACCESS_KEY_ID env → ~/.aws/credentials → IAM role → ECS task role
      s3_options.ConfigureDefaultCredentials();
      LOG(INFO) << "Using Arrow default credential chain (AWS_ACCESS_KEY_ID env -> ~/.aws/credentials -> IAM role)";
      break;
    }
    
    case arrow::fs::S3CredentialsKind::WebIdentity:
      // Should never reach here: 'WebIdentity' is rejected early in resolveCredentialsKind()
      THROW_INVALID_ARGUMENT_EXCEPTION("CREDENTIALS_KIND='WebIdentity' is not supported");
      break;
    
    case arrow::fs::S3CredentialsKind::Role:
      // Should never reach here: 'Role' is rejected early in resolveCredentialsKind()
      THROW_INVALID_ARGUMENT_EXCEPTION("CREDENTIALS_KIND='Role' is not supported");
      break;
    
    default:
      THROW_INVALID_ARGUMENT_EXCEPTION("Unknown CREDENTIALS_KIND");
  }
}

// ============================================================================
// OSS-Specific Handling
// ============================================================================

void S3OptionsBuilder::applyOSSSettings(arrow::fs::S3Options& s3_options, 
                                         const std::string& endpoint) const {
  if (isOSSEndpoint(endpoint)) {
    // OSS requires virtual hosted-style addressing
    // bucket.oss-cn-beijing.aliyuncs.com/key (NOT oss-cn-beijing.aliyuncs.com/bucket/key)
    s3_options.force_virtual_addressing = true;
    LOG(INFO) << "OSS endpoint detected, enabled force_virtual_addressing";
  }
}

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
