/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the DAF project
 * (https://github.com/SNUCSE-CTA/DAF) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

#ifndef GLOBAL_GLOBAL_H_
#define GLOBAL_GLOBAL_H_

#include <cassert>
#include <cstdint>
#include <limits>

#include "../global/log.h"

namespace daf {
using Size = uint32_t;
using Vertex = uint32_t;
using Label = uint32_t;
using QueryDegree = uint8_t;

constexpr Size INVALID_SZ = std::numeric_limits<Size>::max();
constexpr Vertex INVALID_VTX = std::numeric_limits<Vertex>::max();
constexpr Label INVALID_LB = std::numeric_limits<Label>::max();
}  // namespace daf

#endif  // GLOBAL_GLOBAL_H_
