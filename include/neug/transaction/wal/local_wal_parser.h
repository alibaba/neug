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
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "neug/transaction/wal/wal.h"

namespace neug {

/**
 * Local WAL parser implementing the P1-2 recovery protocol:
 * validate first, then order, then replay.
 *
 * open() validates every file header, frame header, frame checksum and
 * commit trailer of the given wal_dir() against the v1 protocol. Only the
 * last candidate frame of a file may be dropped as crash residue when it is
 * truncated by EOF; every other inconsistency rejects the whole recovery
 * with a typed WalRecoveryException. After all files are validated, frames
 * are merged into a strictly timestamp-ascending sequence and duplicate
 * timestamps are rejected. No graph mutation happens here.
 *
 * Validated files stay memory-mapped for the parser's lifetime; replay unit
 * payloads are zero-copy views into those mappings. The mappings are
 * released by close() or destruction, which invalidates the units.
 */
class LocalWalParser : public IWalParser {
 public:
  static std::unique_ptr<IWalParser> Make(const std::string& wal_dir) {
    return std::unique_ptr<IWalParser>(new LocalWalParser(wal_dir));
  }

  explicit LocalWalParser(const std::string& wal_uri);
  ~LocalWalParser() override;

  void open(const std::string& wal_uri) override;
  void close() override;

  uint32_t last_ts() const override;
  const std::vector<WalReplayUnit>& replay_units() const override;

 private:
  /// Source files kept mapped so replay unit payload views stay valid.
  /// WalReplayUnit::file_index indexes this mapping table, which stays
  /// opaque here because its element type is an implementation detail.
  struct MappedFiles;
  std::unique_ptr<MappedFiles> mapped_files_;
  std::vector<WalReplayUnit> replay_units_;
  uint32_t last_ts_{0};

  static const bool registered_;
};

}  // namespace neug
