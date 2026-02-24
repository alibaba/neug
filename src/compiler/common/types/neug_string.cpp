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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/types/neug_string.h"

namespace neug {
namespace common {

neug_string_t::neug_string_t(const char* value, uint64_t length)
    : len(length), prefix{} {
  if (isShortString(length)) {
    memcpy(prefix, value, length);
    return;
  }
  overflowPtr = (uint64_t) (value);
  memcpy(prefix, value, PREFIX_LENGTH);
}

void neug_string_t::set(const std::string& value) {
  set(value.data(), value.length());
}

void neug_string_t::set(const char* value, uint64_t length) {
  if (length <= SHORT_STR_LENGTH) {
    setShortString(value, length);
  } else {
    setLongString(value, length);
  }
}

void neug_string_t::set(const neug_string_t& value) {
  if (value.len <= SHORT_STR_LENGTH) {
    setShortString(value);
  } else {
    setLongString(value);
  }
}

std::string neug_string_t::getAsShortString() const {
  return std::string((char*) prefix, len);
}

std::string neug_string_t::getAsString() const {
  return std::string(getAsStringView());
}

std::string_view neug_string_t::getAsStringView() const {
  if (len <= SHORT_STR_LENGTH) {
    return std::string_view((char*) prefix, len);
  } else {
    return std::string_view(reinterpret_cast<char*>(overflowPtr), len);
  }
}

bool neug_string_t::operator==(const neug_string_t& rhs) const {
  // First compare the length and prefix of the strings.
  auto numBytesOfLenAndPrefix =
      sizeof(uint32_t) +
      std::min((uint64_t) len,
               static_cast<uint64_t>(neug_string_t::PREFIX_LENGTH));
  if (!memcmp(this, &rhs, numBytesOfLenAndPrefix)) {
    // If length and prefix of a and b are equal, we compare the overflow
    // buffer.
    return !memcmp(getData(), rhs.getData(), len);
  }
  return false;
}

bool neug_string_t::operator>(const neug_string_t& rhs) const {
  // Compare neug_string_t up to the shared length.
  // If there is a tie, we just need to compare the std::string lengths.
  auto sharedLen = std::min(len, rhs.len);
  auto memcmpResult = memcmp(prefix, rhs.prefix,
                             sharedLen <= neug_string_t::PREFIX_LENGTH
                                 ? sharedLen
                                 : neug_string_t::PREFIX_LENGTH);
  if (memcmpResult == 0 && len > neug_string_t::PREFIX_LENGTH) {
    memcmpResult = memcmp(getData(), rhs.getData(), sharedLen);
  }
  if (memcmpResult == 0) {
    return len > rhs.len;
  }
  return memcmpResult > 0;
}

}  // namespace common
}  // namespace neug
