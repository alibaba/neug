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

#include "types.h"

namespace neug {
namespace common {

struct neug_list_t {
 public:
  neug_list_t() : size{0}, overflowPtr{0} {}
  neug_list_t(uint64_t size, uint64_t overflowPtr)
      : size{size}, overflowPtr{overflowPtr} {}

  void set(const uint8_t* values, const LogicalType& dataType) const;

 private:
  void set(const std::vector<uint8_t*>& parameters, LogicalTypeID childTypeId);

 public:
  uint64_t size;
  uint64_t overflowPtr;
};

}  // namespace common
}  // namespace neug
