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

#include "s3_client.h"

#include <curl/curl.h>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <mutex>
#include <sstream>
#include <thread>

#include "neug/utils/exception/exception.h"
#include "s3_sigv4.h"

namespace neug {
namespace extension {
namespace s3 {

// ============================================================================
// Global curl initialization
// ============================================================================

void EnsureCurlInitialized() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
      THROW_IO_EXCEPTION("Failed to initialize CURL globally: " +
                         std::string(curl_easy_strerror(res)));
    }
  });
}

// ============================================================================
// CURL callbacks
// ============================================================================

namespace {

size_t WriteToString(void* contents, size_t size, size_t nmemb, void* userp) {
  auto* out = static_cast<std::string*>(userp);
  // Never let an exception (e.g. std::bad_alloc from append) escape into
  // libcurl's C frames — that is undefined behavior. Return 0 so curl
  // aborts the transfer with CURLE_WRITE_ERROR.
  try {
    out->append(static_cast<char*>(contents), size * nmemb);
  } catch (...) { return 0; }
  return size * nmemb;
}

// Write response body into a fixed-capacity buffer (ranged GET).
struct BufferSink {
  void* buffer;
  int64_t capacity;
  int64_t written = 0;
};

size_t WriteToBuffer(void* contents, size_t size, size_t nmemb, void* userp) {
  auto* sink = static_cast<BufferSink*>(userp);
  int64_t incoming = static_cast<int64_t>(size * nmemb);
  int64_t space = sink->capacity - sink->written;
  if (incoming > space) {
    // More data than expected — abort the transfer instead of overflowing.
    LOG(ERROR) << "S3 response exceeded expected range size (incoming="
               << incoming << ", space=" << space << ")";
    return 0;
  }
  std::memcpy(static_cast<char*>(sink->buffer) + sink->written, contents,
              incoming);
  sink->written += incoming;
  return static_cast<size_t>(incoming);
}

// Collect response headers as lowercase (name, value) pairs.
size_t HeaderCollector(char* buffer, size_t size, size_t nitems, void* userp) {
  auto* headers =
      static_cast<std::vector<std::pair<std::string, std::string>>*>(userp);
  size_t total = size * nitems;
  std::string line(buffer, total);
  size_t colon = line.find(':');
  if (colon != std::string::npos) {
    std::string name = line.substr(0, colon);
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    size_t vstart = colon + 1;
    while (vstart < line.size() && line[vstart] == ' ') {
      ++vstart;
    }
    size_t vend = line.size();
    while (vend > vstart && (line[vend - 1] == '\r' || line[vend - 1] == '\n' ||
                             line[vend - 1] == ' ')) {
      --vend;
    }
    headers->emplace_back(name, line.substr(vstart, vend - vstart));
  }
  return total;
}

// Read callback feeding a memory buffer as request body (PUT/POST).
struct BodySource {
  const char* data;
  int64_t length;
  int64_t sent = 0;
};

size_t ReadFromBuffer(char* buffer, size_t size, size_t nmemb, void* userp) {
  auto* src = static_cast<BodySource*>(userp);
  int64_t max_copy = static_cast<int64_t>(size * nmemb);
  int64_t remaining = src->length - src->sent;
  int64_t to_copy = std::min(max_copy, remaining);
  if (to_copy > 0) {
    std::memcpy(buffer, src->data + src->sent, to_copy);
    src->sent += to_copy;
  }
  return static_cast<size_t>(to_copy);
}

// --- Lightweight XML helpers (no XML library needed) ---

std::string decodeXmlEntities(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size();) {
    if (s[i] == '&') {
      if (s.compare(i, 5, "&amp;") == 0) {
        out += '&';
        i += 5;
        continue;
      }
      if (s.compare(i, 4, "&lt;") == 0) {
        out += '<';
        i += 4;
        continue;
      }
      if (s.compare(i, 4, "&gt;") == 0) {
        out += '>';
        i += 4;
        continue;
      }
      if (s.compare(i, 6, "&quot;") == 0) {
        out += '"';
        i += 6;
        continue;
      }
      if (s.compare(i, 6, "&apos;") == 0) {
        out += '\'';
        i += 6;
        continue;
      }
    }
    out += s[i];
    ++i;
  }
  return out;
}

// First occurrence of <tag>value</tag>; empty string when absent.
std::string extractXmlTagValue(const std::string& xml, const std::string& tag) {
  std::string open = "<" + tag + ">";
  std::string close = "</" + tag + ">";
  size_t start = xml.find(open);
  if (start == std::string::npos) {
    return "";
  }
  start += open.size();
  size_t end = xml.find(close, start);
  if (end == std::string::npos) {
    return "";
  }
  return decodeXmlEntities(xml.substr(start, end - start));
}

// All <Key> values inside <Contents> blocks of a ListObjectsV2 response.
std::vector<std::string> extractContentsKeys(const std::string& xml) {
  std::vector<std::string> keys;
  size_t pos = 0;
  while (true) {
    size_t block_start = xml.find("<Contents>", pos);
    if (block_start == std::string::npos) {
      break;
    }
    size_t block_end = xml.find("</Contents>", block_start);
    if (block_end == std::string::npos) {
      break;
    }
    std::string block = xml.substr(block_start, block_end - block_start);
    std::string key = extractXmlTagValue(block, "Key");
    if (!key.empty()) {
      keys.push_back(std::move(key));
    }
    pos = block_end + 11;
  }
  return keys;
}

bool isTransientFailure(CURLcode curl_code, long http_code) {
  if (curl_code != CURLE_OK) {
    // Buffer overflow in our own write callback: retrying won't help.
    if (curl_code == CURLE_WRITE_ERROR) {
      return false;
    }
    return true;
  }
  return http_code == 429 || http_code >= 500;
}

}  // namespace

// ============================================================================
// S3Client
// ============================================================================

S3Client::S3Client(S3ClientConfig config)
    : config_(std::move(config)), curl_share_(std::make_shared<CurlShare>()) {
  EnsureCurlInitialized();
}

result<S3Client::HttpResponse> S3Client::request(
    const std::string& method, const std::string& bucket,
    const std::string& key,
    const std::vector<std::pair<std::string, std::string>>& query_params,
    const std::vector<std::pair<std::string, std::string>>& extra_headers,
    const void* body, int64_t body_len, bool capture_body, void* range_out,
    int64_t range_out_capacity, int64_t* received_bytes,
    long* out_http_code) const {
  // --- Addressing: host + canonical URI ---
  std::string base_host = config_.endpoint;
  if (base_host.empty()) {
    base_host = "s3." + config_.region + ".amazonaws.com";
  }
  std::string host;
  std::string canonical_uri;
  std::string encoded_key = UriEncode(key, false);
  if (config_.path_style) {
    host = base_host;
    canonical_uri = "/" + bucket;
    if (!key.empty()) {
      canonical_uri += "/" + encoded_key;
    }
  } else {
    host = bucket + "." + base_host;
    canonical_uri = key.empty() ? "/" : "/" + encoded_key;
  }

  // --- Payload hash ---
  std::string payload_hash;
  if (body != nullptr && body_len > 0) {
    payload_hash = SHA256Hex(body, static_cast<size_t>(body_len));
  } else {
    payload_hash = EmptyPayloadSHA256();
  }

  // --- Sign ---
  SigV4Credentials creds{config_.access_key, config_.secret_key};
  if (config_.anonymous) {
    creds = SigV4Credentials{};
  }
  SigV4Request sign_req;
  sign_req.method = method;
  sign_req.host = host;
  sign_req.canonical_uri = canonical_uri;
  sign_req.query_params = query_params;
  sign_req.extra_headers = extra_headers;
  sign_req.payload_hash = payload_hash;
  SigV4SignedRequest signed_req =
      SignSigV4(creds, config_.region, "s3", sign_req);

  // --- URL ---
  std::string url = config_.scheme + "://" + host + canonical_uri;
  if (!query_params.empty()) {
    std::vector<std::pair<std::string, std::string>> encoded;
    encoded.reserve(query_params.size());
    for (const auto& p : query_params) {
      encoded.emplace_back(UriEncode(p.first, true), UriEncode(p.second, true));
    }
    std::sort(encoded.begin(), encoded.end());
    url += '?';
    for (size_t i = 0; i < encoded.size(); ++i) {
      if (i > 0) {
        url += '&';
      }
      url += encoded[i].first + "=" + encoded[i].second;
    }
  }

  // --- Retry loop ---
  const int max_attempts = std::max(1, config_.max_retries + 1);
  std::string last_error;
  long last_http_code = 0;

  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    if (attempt > 0) {
      int backoff_ms = 100 * (1 << (attempt - 1));  // 100, 200, 400, ...
      LOG(WARNING) << "S3 request retry " << attempt << "/"
                   << (max_attempts - 1) << " after " << backoff_ms
                   << "ms: " << method << " " << url
                   << " (last error: " << last_error << ")";
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to initialize CURL handle");
    }
    // Reuse DNS/TLS/TCP connections across requests (thread-safe via the
    // share's lock callbacks).
    if (curl_share_ && curl_share_->handle()) {
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_->handle());
    }

    HttpResponse response;
    BufferSink sink{range_out, range_out_capacity};
    BodySource source{static_cast<const char*>(body), body_len};

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                     static_cast<long>(config_.connect_timeout));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                     static_cast<long>(config_.request_timeout));
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    // TLS
    if (config_.scheme == "https") {
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                       config_.verify_ssl ? 1L : 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                       config_.verify_ssl ? 2L : 0L);
      if (!config_.ca_cert_file.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, config_.ca_cert_file.c_str());
      }
    }

    // Request headers (signed)
    struct curl_slist* header_list = nullptr;
    for (const auto& h : signed_req.headers) {
      header_list =
          curl_slist_append(header_list, (h.first + ": " + h.second).c_str());
    }
    if (header_list) {
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
    }

    // Response handling
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCollector);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
    if (method == "HEAD") {
      curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    } else if (range_out != nullptr) {
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteToBuffer);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &sink);
    } else if (capture_body) {
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteToString);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
    } else {
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteToString);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
    }

    // Request body
    if (body != nullptr) {
      curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
      curl_easy_setopt(curl, CURLOPT_READFUNCTION, ReadFromBuffer);
      curl_easy_setopt(curl, CURLOPT_READDATA, &source);
      curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                       static_cast<curl_off_t>(body_len));
      if (method == "POST") {
        // CURLOPT_UPLOAD issues PUT; reset to POST with a read callback body.
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 0L);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                         static_cast<curl_off_t>(body_len));
      }
    } else if (method == "POST") {
      // Empty-body POST (e.g. CreateMultipartUpload)
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                       static_cast<curl_off_t>(0));
    }

    CURLcode res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.http_code);
    }
    if (received_bytes != nullptr) {
      *received_bytes = sink.written;
    }
    curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);

    if (res == CURLE_OK && response.http_code < 400) {
      return response;
    }

    if (!isTransientFailure(res, response.http_code)) {
      // Permanent failure: surface immediately (no retry).
      if (out_http_code != nullptr) {
        *out_http_code = response.http_code;
      }
      std::string detail;
      if (!response.body.empty()) {
        std::string code = extractXmlTagValue(response.body, "Code");
        std::string message = extractXmlTagValue(response.body, "Message");
        if (!code.empty()) {
          detail = " [" + code + (message.empty() ? "" : ": " + message) + "]";
        } else {
          detail = " [body: " + response.body.substr(0, 512) + "]";
        }
      }
      std::string err =
          "S3 request failed: " + method + " " + url + " (HTTP " +
          std::to_string(response.http_code) +
          (res != CURLE_OK ? std::string(", curl: ") + curl_easy_strerror(res)
                           : std::string()) +
          ")" + detail;
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR, err);
    }

    last_error = (res != CURLE_OK)
                     ? std::string(curl_easy_strerror(res))
                     : ("HTTP " + std::to_string(response.http_code));
    last_http_code = response.http_code;
  }

  if (out_http_code != nullptr) {
    *out_http_code = last_http_code;
  }
  RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                      "S3 request failed after " +
                          std::to_string(max_attempts) +
                          " attempts: " + method + " " + url +
                          " (last error: " + last_error + ")");
}

// ============================================================================
// Read operations
// ============================================================================

result<int64_t> S3Client::getObjectSize(const std::string& bucket,
                                        const std::string& key) const {
  auto resp =
      request("HEAD", bucket, key, {}, {}, nullptr, 0, false, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  for (const auto& h : resp->headers) {
    if (h.first == "content-length") {
      try {
        return std::stoll(h.second);
      } catch (...) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                            "Invalid Content-Length header: " + h.second);
      }
    }
  }
  RETURN_STATUS_ERROR(
      neug::StatusCode::ERR_IO_ERROR,
      "S3 HEAD response missing Content-Length for " + bucket + "/" + key);
}

result<bool> S3Client::objectExists(const std::string& bucket,
                                    const std::string& key) const {
  long http_code = 0;
  auto resp = request("HEAD", bucket, key, {}, {}, nullptr, 0, false, nullptr,
                      0, nullptr, &http_code);
  if (!resp) {
    // Treat 404 as "not found"; propagate other errors.
    if (http_code == 404) {
      return false;
    }
    return tl::unexpected(resp.error());
  }
  return true;
}

result<int64_t> S3Client::getObjectRange(const std::string& bucket,
                                         const std::string& key, int64_t offset,
                                         int64_t length, void* out) const {
  if (length == 0) {
    return int64_t{0};
  }
  if (offset < 0 || length < 0) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Invalid read range: offset=" + std::to_string(offset) +
                            ", length=" + std::to_string(length));
  }

  std::string range_value = "bytes=" + std::to_string(offset) + "-" +
                            std::to_string(offset + length - 1);
  std::vector<std::pair<std::string, std::string>> extra_headers = {
      {"range", range_value}};

  int64_t received = 0;
  auto resp = request("GET", bucket, key, {}, extra_headers, nullptr, 0, false,
                      out, length, &received);
  if (!resp) {
    // 416 Range Not Satisfiable -> offset at/beyond EOF -> 0 bytes read.
    if (resp.error().error_message().find("HTTP 416") != std::string::npos) {
      return int64_t{0};
    }
    return tl::unexpected(resp.error());
  }
  if (resp->http_code == 200) {
    RETURN_STATUS_ERROR(
        neug::StatusCode::ERR_IO_ERROR,
        "S3 server ignored the Range header (returned HTTP 200 instead of "
        "206) for " +
            bucket + "/" + key);
  }
  return received;
}

// ============================================================================
// List
// ============================================================================

result<std::vector<std::string>> S3Client::listObjects(
    const std::string& bucket, const std::string& prefix) const {
  std::vector<std::string> keys;
  std::string continuation_token;

  do {
    std::vector<std::pair<std::string, std::string>> query = {
        {"list-type", "2"},
        {"max-keys", "1000"},
    };
    if (!prefix.empty()) {
      query.emplace_back("prefix", prefix);
    }
    if (!continuation_token.empty()) {
      query.emplace_back("continuation-token", continuation_token);
    }

    auto resp =
        request("GET", bucket, "", query, {}, nullptr, 0, true, nullptr, 0);
    if (!resp) {
      return tl::unexpected(resp.error());
    }

    auto page_keys = extractContentsKeys(resp->body);
    keys.insert(keys.end(), page_keys.begin(), page_keys.end());

    std::string truncated = extractXmlTagValue(resp->body, "IsTruncated");
    if (truncated != "true") {
      break;
    }
    continuation_token =
        extractXmlTagValue(resp->body, "NextContinuationToken");
  } while (!continuation_token.empty());

  return keys;
}

// ============================================================================
// Write operations
// ============================================================================

result<void> S3Client::putObject(const std::string& bucket,
                                 const std::string& key, const void* data,
                                 int64_t length) const {
  auto resp =
      request("PUT", bucket, key, {}, {}, data, length, false, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  return {};
}

result<std::string> S3Client::createMultipartUpload(
    const std::string& bucket, const std::string& key) const {
  // CreateMultipartUpload is POST /{key}?uploads; without the `uploads`
  // sub-resource the server treats this as a plain POST Object and
  // rejects it.
  std::vector<std::pair<std::string, std::string>> query = {{"uploads", ""}};
  auto resp =
      request("POST", bucket, key, query, {}, nullptr, 0, true, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  std::string upload_id = extractXmlTagValue(resp->body, "UploadId");
  if (upload_id.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "CreateMultipartUpload response missing UploadId: " +
                            resp->body.substr(0, 512));
  }
  return upload_id;
}

result<std::string> S3Client::uploadPart(const std::string& bucket,
                                         const std::string& key,
                                         const std::string& upload_id,
                                         int part_number, const void* data,
                                         int64_t length) const {
  std::vector<std::pair<std::string, std::string>> query = {
      {"partNumber", std::to_string(part_number)},
      {"uploadId", upload_id},
  };
  auto resp =
      request("PUT", bucket, key, query, {}, data, length, false, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  for (const auto& h : resp->headers) {
    if (h.first == "etag") {
      return h.second;
    }
  }
  RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                      "UploadPart response missing ETag header (part " +
                          std::to_string(part_number) + ")");
}

result<void> S3Client::completeMultipartUpload(
    const std::string& bucket, const std::string& key,
    const std::string& upload_id,
    const std::vector<std::pair<int, std::string>>& parts) const {
  std::ostringstream xml;
  xml << "<CompleteMultipartUpload>";
  for (const auto& p : parts) {
    xml << "<Part><PartNumber>" << p.first << "</PartNumber><ETag>" << p.second
        << "</ETag></Part>";
  }
  xml << "</CompleteMultipartUpload>";
  std::string body = xml.str();

  std::vector<std::pair<std::string, std::string>> query = {
      {"uploadId", upload_id}};
  auto resp = request("POST", bucket, key, query, {}, body.data(),
                      static_cast<int64_t>(body.size()), true, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  // S3 may return HTTP 200 with an error document in the body.
  if (resp->body.find("<Error>") != std::string::npos) {
    std::string code = extractXmlTagValue(resp->body, "Code");
    std::string message = extractXmlTagValue(resp->body, "Message");
    RETURN_STATUS_ERROR(
        neug::StatusCode::ERR_IO_ERROR,
        "CompleteMultipartUpload failed: " + code + ": " + message);
  }
  return {};
}

result<void> S3Client::abortMultipartUpload(
    const std::string& bucket, const std::string& key,
    const std::string& upload_id) const {
  std::vector<std::pair<std::string, std::string>> query = {
      {"uploadId", upload_id}};
  auto resp =
      request("DELETE", bucket, key, query, {}, nullptr, 0, false, nullptr, 0);
  if (!resp) {
    return tl::unexpected(resp.error());
  }
  return {};
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
