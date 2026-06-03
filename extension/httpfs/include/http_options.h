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

#include <string>

namespace neug {
namespace extension {
namespace http {

/**
 * Centralized definition of all HTTP-related configuration keys
 * 
 * These keys are used in FileSchema.options map
 */
struct HTTPConfigOptionKeys {
  // Authentication
  static constexpr const char* kBearerToken = "BEARER_TOKEN";
  static constexpr const char* kAuthorizationHeader = "AUTHORIZATION";
  
  // Custom headers (format: "Key1:Value1;Key2:Value2")
  static constexpr const char* kCustomHeaders = "HTTP_HEADERS";
  
  // TLS/SSL configuration
  static constexpr const char* kCACertFile = "CA_CERT_FILE";
  static constexpr const char* kVerifySSL = "VERIFY_SSL";  // "true" or "false"
  
  // Proxy configuration
  static constexpr const char* kHTTPProxy = "HTTP_PROXY";
  static constexpr const char* kHTTPProxyUsername = "HTTP_PROXY_USERNAME";
  static constexpr const char* kHTTPProxyPassword = "HTTP_PROXY_PASSWORD";
  
  // Timeouts (in seconds)
  static constexpr const char* kConnectTimeout = "CONNECT_TIMEOUT";
  static constexpr const char* kRequestTimeout = "REQUEST_TIMEOUT";
  
  // Retry configuration
  static constexpr const char* kMaxRetries = "MAX_RETRIES";
  static constexpr const char* kRetryDelay = "RETRY_DELAY_MS";  // milliseconds
};

/**
 * Default values for HTTP options
 */
struct HTTPConfigDefaults {
  static constexpr bool kVerifySSLDefault = true;
  static constexpr int kConnectTimeoutDefault = 30;   // 30 seconds
  static constexpr int kRequestTimeoutDefault = 300;  // 5 minutes
  static constexpr int kMaxRetriesDefault = 3;
  static constexpr int kRetryDelayDefault = 1000;  // 1 second
};

}  // namespace http
}  // namespace extension
}  // namespace neug
