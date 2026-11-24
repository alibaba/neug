/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/storages/index/art_tree/common.h"

namespace neug::art {
const char* StringUtil::Arg(const string& s) { return s.c_str(); }
string StringUtil::Format(const char* fmt) { return fmt; }

InternalException::InternalException(const std::string& message)
    : std::runtime_error(message) {}

string_t::string_t(const char* p, size_t n) : value(p, n) {}
string_t::string_t(const string& s) : value(s) {}
const char* string_t::GetData() const { return value.data(); }
idx_t string_t::GetSize() const { return value.size(); }

ValidityMask::ValidityMask(validity_t* p, idx_t) : data(p) {}
void ValidityMask::GetEntryIndex(idx_t i, idx_t& entry, idx_t& bit) {
  entry = i / 64;
  bit = i % 64;
}
bool ValidityMask::RowIsValid(validity_t mask, idx_t bit) {
  return mask & (uint64_t(1) << bit);
}
bool ValidityMask::RowIsValid(idx_t i) const {
  return RowIsValid(data[i / 64], i % 64);
}
validity_t* ValidityMask::GetData() { return data; }
void ValidityMask::SetInvalid(idx_t i) {
  data[i / 64] &= ~(uint64_t(1) << (i % 64));
}
void ValidityMask::SetAllValid(idx_t count) {
  if (count == 0)
    return;
  auto last_entry_index = (count + BITS_PER_VALUE - 1) / BITS_PER_VALUE - 1;
  for (idx_t i = 0; i < last_entry_index; i++)
    data[i] = std::numeric_limits<validity_t>::max();
  auto last_entry_bits = count % BITS_PER_VALUE;
  data[last_entry_index] |=
      last_entry_bits == 0
          ? std::numeric_limits<validity_t>::max()
          : ~(std::numeric_limits<validity_t>::max() << last_entry_bits);
}
void ValidityMask::SetValid(idx_t i) {
  data[i / 64] |= uint64_t(1) << (i % 64);
}
void ValidityMask::SetAllInvalid(idx_t count) {
  std::memset(data, 0, (count + 63) / 64 * sizeof(validity_t));
}
}  // namespace neug::art
