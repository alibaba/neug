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

#include "neug/compiler/common/system_message.h"

#ifdef _WIN32
#include "windows.h"
#else
#include <dlfcn.h>
#endif

namespace neug {
namespace common {

std::string dlErrMessage() {
#ifdef _WIN32
  DWORD errorMessageID = GetLastError();
  if (errorMessageID == 0) {
    return std::string();
  }

  LPSTR messageBuffer = nullptr;
  auto size = FormatMessageA(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPSTR) &messageBuffer, 0, NULL);

  std::string message(messageBuffer, size);

  // Free the buffer.
  LocalFree(messageBuffer);

  return message;
#else
  return dlerror();  // NOLINT(concurrency-mt-unsafe): load can only be executed
                     // in single thread.
#endif
}

}  // namespace common
}  // namespace neug
