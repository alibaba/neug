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

#pragma once

#include <cstring>

#include "neug/compiler/common/serializer/reader.h"

namespace neug {
namespace common {

struct BufferReader final : Reader {
  BufferReader(uint8_t* data, size_t dataSize)
      : data(data), dataSize(dataSize), readSize(0) {}

  void read(uint8_t* outputData, uint64_t size) override {
    memcpy(outputData, data + readSize, size);
    readSize += size;
  }

  bool finished() override { return readSize >= dataSize; }

  uint8_t* data;
  size_t dataSize;
  size_t readSize;
};

}  // namespace common
}  // namespace neug
