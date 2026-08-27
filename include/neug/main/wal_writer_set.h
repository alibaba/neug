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
#include <memory>
#include <string>
#include <vector>

#include "neug/config.h"

namespace neug {

class IWalWriter;

/**
 * Database-owned physical WAL writers keyed by logical execution slot id.
 *
 * Slot 0 exists for the whole lifetime of an open database and is shared by
 * direct AP execution. The direct write guard serializes every WAL-producing
 * AP operation, so that shared writer needs no separate writer-local lock.
 * TP activation adds writers for the remaining logical slots; the service
 * pool borrows them and must be destroyed before they are deactivated.
 */
class WalWriterSet {
 public:
  WalWriterSet(size_t slot_num, DBMode mode, const std::string& wal_uri);
  ~WalWriterSet() noexcept;

  WalWriterSet(const WalWriterSet&) = delete;
  WalWriterSet& operator=(const WalWriterSet&) = delete;

  IWalWriter& DirectWriter() noexcept;
  IWalWriter& WriterFor(size_t slot_id);

  /// Activate the writers borrowed by TP execution slots.
  void ActivateTransactional(const std::string& wal_uri);

  /// Retain direct slot 0 and retire all TP-only writers.
  void DeactivateTransactional() noexcept;

  /// Rotate every currently active writer while checkpoint admission is held.
  void RotateActive(const std::string& wal_uri);

 private:
  std::unique_ptr<IWalWriter> CreateWriter(size_t slot_id,
                                           const std::string& wal_uri) const;

  DBMode mode_;
  std::vector<std::unique_ptr<IWalWriter>> writers_;
};

}  // namespace neug
