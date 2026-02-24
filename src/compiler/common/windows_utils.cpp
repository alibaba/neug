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

#if defined(_WIN32)
#include "neug/compiler/common/windows_utils.h"

#include <memory>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

std::wstring WindowsUtils::utf8ToUnicode(const char* input) {
  uint32_t result;

  result = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
  if (result == 0) {
    THROW_IO_EXCEPTION("Failure in MultiByteToWideChar");
  }
  auto buffer = std::make_unique<wchar_t[]>(result);
  result = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result);
  if (result == 0) {
    THROW_IO_EXCEPTION("Failure in MultiByteToWideChar");
  }
  return std::wstring(buffer.get(), result);
}

std::string WindowsUtils::unicodeToUTF8(LPCWSTR input) {
  uint64_t resultSize;

  resultSize = WideCharToMultiByte(CP_UTF8, 0, input, -1, 0, 0, 0, 0);
  if (resultSize == 0) {
    THROW_IO_EXCEPTION("Failure in WideCharToMultiByte");
  }
  auto buffer = std::make_unique<char[]>(resultSize);
  resultSize = WideCharToMultiByte(CP_UTF8, 0, input, -1, buffer.get(),
                                   resultSize, 0, 0);
  if (resultSize == 0) {
    THROW_IO_EXCEPTION("Failure in WideCharToMultiByte");
  }
  return std::string(buffer.get(), resultSize - 1);
}

}  // namespace common
}  // namespace neug
#endif
