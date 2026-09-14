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
  static constexpr size_t kInitialFileSize = 64ULL * 1024 * 1024;
  static constexpr size_t kFileGrowthSize = 64ULL * 1024 * 1024;

  static std::unique_ptr<IWalWriter> Make(const std::string& wal_uri,
                                          int slot_id);

  LocalWalWriter(const std::string& wal_uri, int slot_id,
                 size_t initial_file_size = kInitialFileSize,
                 size_t file_growth_size = kFileGrowthSize)
      : wal_uri_(wal_uri),
        slot_id_(slot_id),
        fd_(-1),
        file_size_(0),
        file_used_(0),
        initial_file_size_(initial_file_size),
        file_growth_size_(file_growth_size),
        opened_(false) {}
  ~LocalWalWriter() noexcept override;

  void open(const std::string& wal_uri) override;
  void close() override;
  bool append(const char* data, size_t length) override;
  std::string type() const override { return "file"; }

 private:
  void create_file();
  void ensure_file_size(size_t required_size);

  std::string wal_uri_;
  int slot_id_;
  int fd_;
  size_t file_size_;
  size_t file_used_;
  size_t initial_file_size_;
  size_t file_growth_size_;
  bool opened_;

  static const bool registered_;
};

}  // namespace neug
