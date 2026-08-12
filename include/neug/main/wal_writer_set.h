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

/** @brief Database-owned physical WAL writers keyed by logical slot id. */
class WalWriterSet {
 public:
  WalWriterSet(size_t slot_num, DBMode mode, const std::string& wal_uri);
  ~WalWriterSet() noexcept;

  WalWriterSet(const WalWriterSet&) = delete;
  WalWriterSet& operator=(const WalWriterSet&) = delete;

  /**
   * Direct-mode connections all share logical writer slot 0. No writer-local
   * mutex is required: every WAL-producing direct transaction holds the
   * database VersionManager's exclusive write admission from before mutation
   * through WAL append and publication. Direct reads never access the writer,
   * and unlogged bulk operations do not append WAL.
   *
   * A concurrently admitted path such as transactional InsertTransaction must
   * use its own logical-slot writer rather than relying on this invariant.
   */
  IWalWriter& DirectWriter() noexcept;

  IWalWriter& WriterFor(size_t slot_id);

  /** Open one writer per logical slot after direct execution has drained. */
  void ActivateTransactional(const std::string& wal_uri);

  /** Retain writer 0 and retire the additional transactional writers. */
  void DeactivateTransactional() noexcept;

  /**
   * Rotate every currently active writer after transaction quiescence.
   * Failure after close begins is a fail-stop checkpoint activation error; the
   * caller must not reopen transaction admission on a partially rotated set.
   */
  void RotateActive(const std::string& wal_uri);

  size_t SlotNum() const noexcept { return writers_.size(); }

 private:
  std::unique_ptr<IWalWriter> CreateWriter(size_t slot_id,
                                           const std::string& wal_uri) const;

  DBMode mode_;
  std::vector<std::unique_ptr<IWalWriter>> writers_;
};

}  // namespace neug
