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
#include <memory>
#include <string>

#include "neug/transaction/wal/wal.h"

namespace neug {

class LocalWalWriter : public IWalWriter {
 public:
  static std::unique_ptr<IWalWriter> Make(const std::string& wal_uri,
                                          int thread_id);

  static constexpr size_t TRUNC_SIZE = 1ul << 30;
  LocalWalWriter(const std::string& wal_uri, int thread_id)
      : wal_uri_(wal_uri),
        thread_id_(thread_id),
        fd_(-1),
        file_size_(0),
        file_used_(0) {}
  ~LocalWalWriter() { close(); }

  void open() override;
  void close() override;
  bool append(const char* data, size_t length) override;
  std::string type() const override { return "file"; }

 private:
  std::string wal_uri_;
  int thread_id_;
  int fd_;
  size_t file_size_;
  size_t file_used_;

  static const bool registered_;
};

}  // namespace neug
