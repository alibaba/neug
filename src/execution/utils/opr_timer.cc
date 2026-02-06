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

#include "neug/execution/utils/opr_timer.h"

namespace neug {

namespace execution {

OprTimer& OprTimer::operator+=(const OprTimer& other) {
  this->time_ += other.time_;
  this->numTuples_ += other.numTuples_;
  for (size_t i = 0; i < children_.size(); ++i) {
    *children_[i] += *other.children_[i];
  }
  if (other.next_) {
    *(this->next_) += *(other.next_);
  }
  return *this;
}

}  // namespace execution

}  // namespace neug
