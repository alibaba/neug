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

#include "int128_t.h"

namespace neug {

namespace regex {
class RE2;
}

namespace common {

class RandomEngine;

// Note: uuid_t is a reserved keyword in MSVC, we have to use neug_uuid_t
// instead.
struct neug_uuid_t {
  int128_t value;
};

struct UUID {
  static constexpr const uint8_t UUID_STRING_LENGTH = 36;
  static constexpr const char HEX_DIGITS[] = "0123456789abcdef";
  static void byteToHex(char byteVal, char* buf, uint64_t& pos);
  static unsigned char hex2Char(char ch);
  static bool isHex(char ch);
  static bool fromString(std::string str, int128_t& result);

  static int128_t fromString(std::string str);
  static int128_t fromCString(const char* str, uint64_t len);
  static void toString(int128_t input, char* buf);
  static std::string toString(int128_t input);
  static std::string toString(neug_uuid_t val);

  static neug_uuid_t generateRandomUUID(RandomEngine* engine);

  static const regex::RE2& regexPattern();
};

}  // namespace common
}  // namespace neug
