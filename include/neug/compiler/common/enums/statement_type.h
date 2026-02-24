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

#include <cstdint>

namespace neug {
namespace common {

enum class StatementType : uint8_t {
  QUERY = 0,
  CREATE_TABLE = 1,
  DROP = 2,
  ALTER = 3,
  COPY_TO = 19,
  COPY_FROM = 20,
  STANDALONE_CALL = 21,
  STANDALONE_CALL_FUNCTION = 22,
  EXPLAIN = 23,
  CREATE_MACRO = 24,
  TRANSACTION = 30,
  EXTENSION = 31,
  EXPORT_DATABASE = 32,
  IMPORT_DATABASE = 33,
  ATTACH_DATABASE = 34,
  DETACH_DATABASE = 35,
  USE_DATABASE = 36,
  CREATE_SEQUENCE = 37,
  CREATE_TYPE = 39,
};

}  // namespace common
}  // namespace neug
