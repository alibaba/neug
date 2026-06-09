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
#include "neug/utils/uuid.h"

#include <cstdint>
#include <iomanip>
#include <random>
#include <sstream>

namespace neug {

namespace {

// Each thread owns its own mt19937 + uniform distribution.  random_device is
// invoked exactly once per thread (on first call) to seed the engine, instead
// of on every UUID generation.
struct UUIDState {
  std::mt19937 gen;
  std::uniform_int_distribution<uint32_t> dis;
  UUIDState() : gen(std::random_device{}()), dis(0, 255) {}
};

UUIDState& thread_state() {
  thread_local UUIDState state;
  return state;
}

}  // namespace

std::string UUIDGenerator::Generate() {
  auto& state = thread_state();

  uint8_t bytes[16];
  for (int i = 0; i < 16; ++i) {
    bytes[i] = static_cast<uint8_t>(state.dis(state.gen));
  }
  // RFC4122 v4: set the version (4) and variant (10xx) bits.
  bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0F) | 0x40);
  bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3F) | 0x80);

  std::ostringstream ss;
  ss << std::hex << std::setfill('0');
  for (int i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      ss << '-';
    }
    ss << std::setw(2) << static_cast<unsigned>(bytes[i]);
  }
  return ss.str();
}

}  // namespace neug
