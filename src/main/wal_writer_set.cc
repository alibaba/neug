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

#include "neug/main/wal_writer_set.h"

#include <glog/logging.h>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {

WalWriterSet::WalWriterSet(size_t slot_num, DBMode mode,
                           const std::string& wal_uri)
    : mode_(mode), writers_(slot_num) {
  if (slot_num == 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("WAL writer set cannot be empty");
  }
  WalWriterFactory::Init();
  writers_[0] = CreateWriter(0, wal_uri);
}

WalWriterSet::~WalWriterSet() noexcept = default;

std::unique_ptr<IWalWriter> WalWriterSet::CreateWriter(
    size_t slot_id, const std::string& wal_uri) const {
  auto writer = mode_ == DBMode::READ_WRITE
                    ? WalWriterFactory::CreateWalWriter(
                          wal_uri, static_cast<int>(slot_id))
                    : WalWriterFactory::CreateDummyWalWriter();
  CHECK(writer != nullptr);
  if (mode_ == DBMode::READ_WRITE) {
    writer->open(wal_uri);
  }
  return writer;
}

IWalWriter& WalWriterSet::DirectWriter() noexcept {
  CHECK(!writers_.empty());
  CHECK(writers_[0] != nullptr);
  return *writers_[0];
}

IWalWriter& WalWriterSet::WriterFor(size_t slot_id) {
  if (slot_id >= writers_.size() || writers_[slot_id] == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("WAL writer slot is not active: " +
                                     std::to_string(slot_id));
  }
  return *writers_[slot_id];
}

void WalWriterSet::ActivateTransactional(const std::string& wal_uri) {
  for (size_t slot_id = 1; slot_id < writers_.size(); ++slot_id) {
    CHECK(writers_[slot_id] == nullptr)
        << "Transactional WAL writers are already active";
  }
  try {
    for (size_t slot_id = 1; slot_id < writers_.size(); ++slot_id) {
      writers_[slot_id] = CreateWriter(slot_id, wal_uri);
    }
  } catch (...) {
    DeactivateTransactional();
    throw;
  }
}

void WalWriterSet::DeactivateTransactional() noexcept {
  for (size_t slot_id = 1; slot_id < writers_.size(); ++slot_id) {
    writers_[slot_id].reset();
  }
}

void WalWriterSet::RotateActive(const std::string& wal_uri) {
  if (mode_ != DBMode::READ_WRITE) {
    return;
  }
  for (auto& writer : writers_) {
    if (writer) {
      writer->close();
    }
  }
  for (auto& writer : writers_) {
    if (writer) {
      writer->open(wal_uri);
    }
  }
}

}  // namespace neug
