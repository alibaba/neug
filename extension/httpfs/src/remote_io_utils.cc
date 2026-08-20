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

#include "remote_io_utils.h"
#include <cstring>

namespace neug {
namespace extension {

// Upper bound for indexing mutexes_ by curl_lock_data; comfortably above
// every CURL_LOCK_DATA_* value.
static constexpr int kMaxLockDataKinds = 16;

CurlShare::CurlShare() {
  for (int i = 0; i < kMaxLockDataKinds; ++i) {
    mutexes_.push_back(std::make_unique<std::mutex>());
  }
  share_ = curl_share_init();
  if (share_) {
    curl_share_setopt(share_, CURLSHOPT_LOCKFUNC, LockCallback);
    curl_share_setopt(share_, CURLSHOPT_UNLOCKFUNC, UnlockCallback);
    curl_share_setopt(share_, CURLSHOPT_USERDATA, this);
    curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
    curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
  }
}

CurlShare::~CurlShare() {
  if (share_) {
    curl_share_cleanup(share_);
    share_ = nullptr;
  }
}

void CurlShare::LockCallback(CURL* /*handle*/, curl_lock_data data,
                             curl_lock_access /*access*/, void* userptr) {
  auto* self = static_cast<CurlShare*>(userptr);
  self->mutexes_[static_cast<int>(data) % kMaxLockDataKinds]->lock();
}

void CurlShare::UnlockCallback(CURL* /*handle*/, curl_lock_data data,
                               void* userptr) {
  auto* self = static_cast<CurlShare*>(userptr);
  self->mutexes_[static_cast<int>(data) % kMaxLockDataKinds]->unlock();
}

int64_t ReadaheadCache::tryServe(int64_t position, int64_t nbytes,
                                 void* out) const {
  if (nbytes <= 0) {
    return 0;
  }
  const int64_t cached_len = static_cast<int64_t>(buffer_.size());
  if (position < offset_ || position + nbytes > offset_ + cached_len) {
    return -1;
  }
  std::memcpy(out, buffer_.data() + (position - offset_),
              static_cast<size_t>(nbytes));
  return nbytes;
}

void ReadaheadCache::store(int64_t position, const char* data, int64_t nbytes) {
  offset_ = position;
  buffer_.assign(data, data + nbytes);
}

void ReadaheadCache::clear() {
  buffer_.clear();
  offset_ = 0;
}

}  // namespace extension
}  // namespace neug
