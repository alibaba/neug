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

#include <stddef.h>
#include <string>

#include "neug/transaction/wal/wal.h"

namespace neug {
/**
 * @brief DummyWalWriter is a no-op implementation of the IWalWriter interface.
 * It is used when write-ahead logging is disabled or not required.
 */
class DummyWalWriter : public IWalWriter {
 public:
  DummyWalWriter() {}
  ~DummyWalWriter() {}

  std::string type() const override;

  void open() override;

  void close() override;
  bool append(const char* data, size_t length) override;
};
}  // namespace neug
