/** Copyright 2020 Alibaba Group Holding Limited.
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
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "neug/utils/result.h"

namespace neug {
namespace io {

class OutputStream {
 public:
  virtual ~OutputStream() = default;
  virtual neug::Status Write(const uint8_t* data, int64_t nbytes) = 0;
  virtual neug::Status Close() = 0;
};

/// Opens a local file for writing (strips optional file:// prefix).
std::unique_ptr<OutputStream> openLocalOutputStream(const std::string& path);

}  // namespace io
}  // namespace neug
