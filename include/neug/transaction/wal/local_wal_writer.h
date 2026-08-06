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

#include "neug/transaction/wal/wal.h"

namespace neug {

/**
 * Append-only local WAL writer producing the v1 framed format.
 *
 * The file holds a real logical EOF: no ftruncate() preallocation and no
 * zero-byte terminator. open() persists and syncs the file header first;
 * frames are only accepted afterwards. Each append_frame() writes the frame
 * header and payload, then persists the commit trailer separately so the
 * marker is never present before the full payload.
 */
class LocalWalWriter : public IWalWriter {
 public:
  static std::unique_ptr<IWalWriter> Make(const std::string& wal_uri,
                                          int slot_id);

  LocalWalWriter(const std::string& wal_uri, int slot_id)
      : wal_uri_(wal_uri),
        slot_id_(slot_id),
        fd_(-1),
        append_offset_(0),
        write_phase_(WalWritePhase::kIdle) {}
  ~LocalWalWriter() noexcept override;

  void open(const std::string& wal_uri) override;
  void close() override;
  bool append_frame(uint32_t commit_timestamp, WalRecordKind kind,
                    const char* payload, size_t length) override;
  WalWritePhase write_phase() const override { return write_phase_; }
  std::string type() const override { return "file"; }

  /// Test seam: fail the next write_all() call at the given phase. The
  /// injection is one-shot and cleared after it fires.
  enum class FailNextWrite { kNone, kHeader, kPayload, kTrailer };
  void fail_next_write(FailNextWrite phase) { fail_next_write_ = phase; }

 private:
  /// Writes every byte of @p buffer, handling short writes. Throws
  /// IOException on error. Returns false only when the write failure was
  /// injected via fail_next_write().
  bool write_all(const char* buffer, size_t length, FailNextWrite phase);
  void sync_file();
  /// Restores the clean logical EOF at @p offset, discarding the bytes of a
  /// frame whose write failed. Returns false and marks the writer failed if
  /// the file cannot be restored; a failed writer rejects further frames.
  bool restore_clean_eof(size_t offset);

  std::string wal_uri_;
  std::string path_;
  int slot_id_;
  int fd_;
  size_t append_offset_;
  WalWritePhase write_phase_;
  FailNextWrite fail_next_write_{FailNextWrite::kNone};
  bool failed_{false};

  static const bool registered_;
};

}  // namespace neug
