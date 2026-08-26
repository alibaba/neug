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

#include <curl/curl.h>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace neug {
namespace extension {

/**
 * Thread-safe CURLSH wrapper that lets independent CURL easy handles share
 * DNS, TLS-session and connection caches.
 *
 * Each request still uses its own easy handle (easy handles are not
 * thread-safe), but with CURLOPT_SHARE pointing at this object libcurl
 * reuses established TCP/TLS connections instead of performing a fresh
 * handshake per request. The lock callbacks make concurrent use safe.
 */
class CurlShare {
 public:
  CurlShare();
  ~CurlShare();

  CurlShare(const CurlShare&) = delete;
  CurlShare& operator=(const CurlShare&) = delete;

  CURLSH* handle() const { return share_; }

 private:
  static void LockCallback(CURL* handle, curl_lock_data data,
                           curl_lock_access access, void* userptr);
  static void UnlockCallback(CURL* handle, curl_lock_data data, void* userptr);

  CURLSH* share_;
  // One mutex per curl_lock_data kind keeps unrelated cache lookups from
  // contending; indexed by the data enum value.
  std::vector<std::unique_ptr<std::mutex>> mutexes_;
};

/// Default readahead window for remote random-access streams. Small
/// sequential reads (e.g. streaming CSV/JSON imports) are served from one
/// ranged GET per window instead of one request per read.
static constexpr int64_t kRemoteReadaheadBytes = 4 * 1024 * 1024;

/**
 * Single-block readahead cache for random-access streams.
 *
 * Caches the most recently fetched block; reads fully contained in it are
 * served without any network round trip. Any read outside the block is a
 * miss and the caller fetches a new block (typically
 * max(requested, kRemoteReadaheadBytes) bytes).
 */
class ReadaheadCache {
 public:
  /// Serve [position, position + nbytes) from the cache. Returns the number
  /// of bytes copied into `out`, or -1 on a miss.
  int64_t tryServe(int64_t position, int64_t nbytes, void* out) const;

  /// Replace the cached block with `nbytes` bytes fetched at `position`.
  void store(int64_t position, const char* data, int64_t nbytes);

  void clear();

 private:
  std::vector<char> buffer_;
  int64_t offset_ = 0;
};

}  // namespace extension
}  // namespace neug
