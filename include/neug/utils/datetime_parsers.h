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
#pragma once

#include <cstddef>
#include <cstdint>

namespace neug {
namespace utils {

enum class TimestampUnit { kSecond, kMilli, kMicro, kNano };

/// Parse ISO-like timestamp strings into the requested unit.
bool parse_timestamp(const char* s, size_t length, TimestampUnit unit,
                     int64_t* out);

/// Parse epoch-seconds format: epoch seconds + 3-digit subseconds suffix.
bool parse_epoch_timestamp(const char* s, size_t length, TimestampUnit unit,
                           int64_t* out);

inline bool parse_timestamp_ms(const char* s, size_t length, int64_t* out_ms) {
  return parse_timestamp(s, length, TimestampUnit::kMilli, out_ms);
}

inline bool parse_epoch_timestamp_ms(const char* s, size_t length,
                                     int64_t* out_ms) {
  return parse_epoch_timestamp(s, length, TimestampUnit::kMilli, out_ms);
}

}  // namespace utils
}  // namespace neug
