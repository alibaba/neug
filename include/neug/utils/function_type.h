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

#include <cstddef>
#include <memory>
#include <vector>

#include "neug/common/types/value.h"

namespace neug {
class IStorageInterface;

namespace execution {
using neug::Value;

using neug_func_exec_t = Value (*)(const std::vector<Value>&);
// Context-aware functions receive the storage used to bind their argument
// expressions. Storage may be null for constant binding.
using neug_context_func_exec_t = Value (*)(const std::vector<Value>&,
                                           const IStorageInterface*);

}  // namespace execution
}  // namespace neug
