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

#include "neug/compiler/common/serializer/deserializer.h"

namespace neug {
namespace common {

template <>
void Deserializer::deserializeValue(std::string& value) {
  uint64_t valueLength = 0;
  deserializeValue(valueLength);
  value.resize(valueLength);
  reader->read(reinterpret_cast<uint8_t*>(value.data()), valueLength);
}

void Deserializer::validateDebuggingInfo(std::string& value,
                                         const std::string& expectedVal) {
#if defined(NEUG_DESER_DEBUG) && \
    (defined(NEUG_RUNTIME_CHECKS) || !defined(NDEBUG))
  deserializeValue<std::string>(value);
  NEUG_ASSERT(value == expectedVal);
#endif
  // DO NOTHING
  NEUG_UNUSED(value);
  NEUG_UNUSED(expectedVal);
}

}  // namespace common
}  // namespace neug
