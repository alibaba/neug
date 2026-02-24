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

#include "neug/compiler/storage/wal/wal.h"
#include "neug/compiler/transaction/transaction.h"

namespace neug {
class Constants {
 public:
  static inline uint64_t MAX_UPPER_BOUND = INT32_MAX;
  static inline uint64_t VARCHAR_MAX_LENGTH = 65536;
  static inline uint64_t ARRAY_MAX_LENGTH = 256;
  static inline neug::transaction::Transaction DEFAULT_TRANSACTION =
      neug::transaction::Transaction(
          neug::transaction::TransactionType::DUMMY,
          neug::transaction::Transaction::DUMMY_TRANSACTION_ID,
          common::INVALID_TRANSACTION);
};
}  // namespace neug