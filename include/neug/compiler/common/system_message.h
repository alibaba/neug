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

#include <cerrno>
#include <string>
#include <system_error>

#include "neug/utils/api.h"

namespace neug {
namespace common {

inline std::string systemErrMessage(int code) {
  // System errors are unexpected. For anything expected, we should catch it
  // explicitly and provide a better error message to the user. LCOV_EXCL_START
  return std::system_category().message(code);
  // LCOV_EXCL_STOP
}

inline std::string posixErrMessage() {
  // LCOV_EXCL_START
  return systemErrMessage(errno);
  // LCOV_EXCL_STOP
}

NEUG_API std::string dlErrMessage();

}  // namespace common
}  // namespace neug
