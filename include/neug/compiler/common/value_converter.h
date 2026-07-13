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

#pragma once

#include <cstdint>

#include "neug/common/types/value.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {

::neug::Value convertToExecutionValue(const compiler_impl::Value& value,
                                      const DataType& type);

compiler_impl::Value convertToCompilerValue(const ::neug::Value& value,
                                            const DataType& type);

int64_t normalizeTimestampMillis(compiler_impl::timestamp_ms_t value);

}  // namespace common
}  // namespace neug
