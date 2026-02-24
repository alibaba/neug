/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/server/session_pool.h"

namespace neug {

inline size_t acquire_session_impl(std::deque<size_t>& available_sessions_,
                                   bthread_mutex_t& mutex_,
                                   bthread_cond_t& cond_, size_t session_num) {
  bthread_mutex_lock(&mutex_);

  while (available_sessions_.empty()) {
    bthread_cond_wait(&cond_, &mutex_);
  }

  size_t session_id = available_sessions_.front();
  available_sessions_.pop_front();
  assert(session_id < session_num && "Acquired session_id is out of range!");

  bthread_mutex_unlock(&mutex_);
  return session_id;
}

SessionGuard::~SessionGuard() {
  if (pool_ && session_) {
    pool_->ReleaseSession(session_id_);
  }
}

SessionGuard SessionPool::AcquireSession() {
  size_t session_id =
      acquire_session_impl(available_sessions_, mutex_, cond_, session_num_);
  return SessionGuard(&contexts_[session_id].session, this, session_id);
}

void SessionPool::ReleaseSession(size_t session_id) {
  assert(session_id < session_num_ && "Releasing session_id is out of range!");

  bthread_mutex_lock(&mutex_);
  available_sessions_.push_back(session_id);
  VLOG(10) << "Released session_id=" << session_id;
  bthread_cond_signal(&cond_);
  bthread_mutex_unlock(&mutex_);
}

}  // namespace neug
