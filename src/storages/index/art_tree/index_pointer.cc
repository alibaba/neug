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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#include "neug/storages/index/art_tree/index_pointer.h"

namespace neug::art {
IndexPointer::IndexPointer() : data(0) {}
IndexPointer::IndexPointer(const uint32_t buffer_id, const uint32_t offset)
    : data(0) {
  auto shifted_offset = UnsafeNumericCast<idx_t>(offset) << SHIFT_OFFSET;
  data += shifted_offset;
  data += buffer_id;
}

idx_t IndexPointer::Get() const { return data; }
void IndexPointer::Set(const idx_t data_p) { data = data_p; }
bool IndexPointer::HasMetadata() const { return data & AND_METADATA; }
uint8_t IndexPointer::GetMetadata() const { return data >> SHIFT_METADATA; }
void IndexPointer::SetMetadata(const uint8_t metadata) {
  data &= ~AND_METADATA;
  data |= UnsafeNumericCast<idx_t>(metadata) << SHIFT_METADATA;
}
idx_t IndexPointer::GetOffset() const {
  auto offset = data >> SHIFT_OFFSET;
  return offset & AND_OFFSET;
}
idx_t IndexPointer::GetBufferId() const { return data & AND_BUFFER_ID; }
void IndexPointer::Clear() { data = 0; }
bool IndexPointer::operator==(const IndexPointer& ptr) const {
  return data == ptr.data;
}
}  // namespace neug::art
