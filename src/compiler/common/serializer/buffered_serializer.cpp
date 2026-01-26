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

#include "neug/compiler/common/serializer/buffered_serializer.h"

#include <cstring>

namespace neug {
namespace common {

BufferedSerializer::BufferedSerializer(uint64_t maximumSize)
    : BufferedSerializer(std::make_unique<uint8_t[]>(maximumSize),
                         maximumSize) {}

BufferedSerializer::BufferedSerializer(std::unique_ptr<uint8_t[]> data,
                                       uint64_t size)
    : maximumSize(size), data(data.get()) {
  blob.size = 0;
  blob.data = std::move(data);
}

void BufferedSerializer::write(const uint8_t* buffer, uint64_t len) {
  if (blob.size + len >= maximumSize) {
    do {
      maximumSize *= 2;
    } while (blob.size + len > maximumSize);
    auto new_data = std::make_unique<uint8_t[]>(maximumSize);
    memcpy(new_data.get(), data, blob.size);
    data = new_data.get();
    blob.data = std::move(new_data);
  }

  memcpy(data + blob.size, buffer, len);
  blob.size += len;
}

}  // namespace common
}  // namespace neug
