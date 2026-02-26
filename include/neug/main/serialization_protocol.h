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

#include <cstddef>
#include <string>

namespace neug {

enum class SerializationProtocol : uint8_t {
  kArrowIPC = 0,
  kNeugSerial = 1,  // Not implemented yet, reserved for future use
  kUnknown = 255,
};

inline SerializationProtocol ParseSerializationProtocol(
    const std::string& protocol_str) {
  if (protocol_str.empty()) {
    return SerializationProtocol::kArrowIPC;
  }
  std::string lower;
  lower.reserve(protocol_str.size());
  for (char c : protocol_str) {
    lower.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  if (lower == "arrow" || lower == "arrow_ipc") {
    return SerializationProtocol::kArrowIPC;
  } else if (lower == "neug_serial" || lower == "neug") {
    return SerializationProtocol::kNeugSerial;
  } else {
    THROW_RUNTIME_ERROR("Unsupported response protocol: " + protocol_str);
  }
}

inline SerializationProtocol ParseSerializationProtocol(int c) {
  switch (c) {
  case 0:
    return SerializationProtocol::kArrowIPC;
  case 1:
    return SerializationProtocol::kNeugSerial;
  default:
    THROW_RUNTIME_ERROR("Unsupported response protocol: " + std::to_string(c));
  }
  return SerializationProtocol::kUnknown;
}

}  // namespace neug

namespace std {
inline std::string to_string(const neug::SerializationProtocol& protocol) {
  switch (protocol) {
  case neug::SerializationProtocol::kArrowIPC:
    return "arrow_ipc";
  case neug::SerializationProtocol::kNeugSerial:
    return "neug_serial";
  default:
    return "unknown";
  }
}

}  // namespace std