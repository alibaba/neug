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

#include "neug/transaction/wal/local_wal_writer.h"

#include "neug/utils/exception/exception.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <string.h>
#include <unistd.h>
#include <exception>
#include <filesystem>
#include <ostream>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/likely.h"

namespace neug {

std::unique_ptr<IWalWriter> LocalWalWriter::Make(const std::string& wal_uri,
                                                 int slot_id) {
  return std::unique_ptr<IWalWriter>(new LocalWalWriter(wal_uri, slot_id));
}

LocalWalWriter::~LocalWalWriter() noexcept {
  try {
    close();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to close WAL writer during destruction: " << e.what();
  } catch (...) {
    LOG(ERROR) << "Failed to close WAL writer during destruction.";
  }
}

void LocalWalWriter::open(const std::string& wal_uri) {
  close();
  wal_uri_ = wal_uri;
  auto prefix = get_wal_uri_path(wal_uri);
  if (!std::filesystem::exists(prefix)) {
    std::filesystem::create_directories(prefix);
  }
  path_.clear();
  const int max_version = 65536;
  for (int version = 0; version != max_version; ++version) {
    // Keep the historical on-disk prefix for WAL replay compatibility. The
    // numeric component now identifies a logical execution slot, not a
    // physical pthread.
    std::string path = prefix + "/thread_" + std::to_string(slot_id_) + "_" +
                       std::to_string(version) + ".wal";
    if (std::filesystem::exists(path)) {
      continue;
    }
    path_ = path;
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    break;
  }
  if (fd_ == -1) {
    THROW_IO_EXCEPTION("Failed to open wal file " +
                       std::string(strerror(errno)));
  }

  // Persist the v1 file header before any frame is accepted. The file
  // belongs to the wal_dir() it is created in; checkpoint ownership is
  // guaranteed by checkpoint rotation, and the writer identity is already
  // carried by the file name.
  const WalFileHeader header;
  const auto encoded = EncodeWalFileHeader(header);
  if (!write_all(reinterpret_cast<const char*>(encoded.data()), encoded.size(),
                 FailNextWrite::kHeader)) {
    close();
    THROW_IO_EXCEPTION("Injected write failure while writing wal file header");
  }
  sync_file();
  append_offset_ = encoded.size();
}

void LocalWalWriter::close() {
  if (fd_ != -1) {
    // Retire the descriptor before calling close(). Retrying close() after an
    // error is unsafe because the descriptor may already have been released
    // and reused by another thread.
    const int fd = fd_;
    fd_ = -1;
    path_.clear();
    append_offset_ = 0;
    failed_ = false;
    if (::close(fd) != 0) {
      THROW_IO_EXCEPTION("Failed to close file" + std::string(strerror(errno)));
    }
  }
}

bool LocalWalWriter::restore_clean_eof(size_t offset) {
  if (fd_ == -1) {
    failed_ = true;
    return false;
  }
  if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0 ||
      ::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) ==
          static_cast<off_t>(-1)) {
    LOG(ERROR) << "Failed to restore wal file " << path_ << " to offset "
               << offset << ": " << strerror(errno);
    failed_ = true;
    return false;
  }
  append_offset_ = offset;
  return true;
}

bool LocalWalWriter::write_all(const char* buffer, size_t length,
                               FailNextWrite phase) {
  if (fail_next_write_ != FailNextWrite::kNone && fail_next_write_ == phase) {
    fail_next_write_ = FailNextWrite::kNone;
    return false;
  }
  size_t written = 0;
  while (written < length) {
    const ssize_t ret = ::write(fd_, buffer + written, length - written);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      THROW_IO_EXCEPTION("Failed to write wal file " + path_ + ": " +
                         std::string(strerror(errno)));
    }
    written += static_cast<size_t>(ret);
  }
  append_offset_ += length;
  return true;
}

void LocalWalWriter::sync_file() {
  // Keep the current synchronous durability strategy as the transitional
  // implementation: one full sync per frame.
#ifdef F_FULLFSYNC
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
    THROW_IO_EXCEPTION("Failed to fcntl sync wal file " + path_ + ": " +
                       std::string(strerror(errno)));
  }
#else
  if (fdatasync(fd_) != 0) {
    THROW_IO_EXCEPTION("Failed to fsync wal file " + path_ + ": " +
                       std::string(strerror(errno)));
  }
#endif
}

bool LocalWalWriter::append_frame(uint32_t commit_timestamp, WalRecordKind kind,
                                  const char* payload, size_t length) {
  if (NEUG_UNLIKELY(fd_ == -1 || failed_)) {
    return false;
  }
  if (length > kWalMaxPayloadLength) {
    THROW_INVALID_ARGUMENT_EXCEPTION("WAL frame payload too large: " +
                                     std::to_string(length));
  }

  const auto* payload_bytes = reinterpret_cast<const uint8_t*>(payload);
  // Clean EOF before this attempt; a failed frame is rolled back to here so
  // its residue can never be buried mid-file by a later successful append.
  const size_t frame_start = append_offset_;
  const auto rollback_failed_frame = [&](const char* phase) {
    LOG(ERROR) << "Injected write failure at " << phase
               << ", wal file: " << path_;
    restore_clean_eof(frame_start);
    return false;
  };

  // 1) Frame header. The checksum is computed over the payload before any
  // byte is written, so a persisted header always describes the frame that
  // follows it; recovery decides completeness purely from the checksum.
  WalFrameHeader header;
  header.record_kind = kind;
  header.commit_timestamp = commit_timestamp;
  header.payload_length = static_cast<uint32_t>(length);
  const auto encoded_header =
      EncodeWalFrameHeader(header, payload_bytes, length);

  try {
    // 2) Header, then payload. A crash between or during the two writes
    // leaves a short frame at EOF, which recovery discards as torn residue.
    if (!write_all(reinterpret_cast<const char*>(encoded_header.data()),
                   encoded_header.size(), FailNextWrite::kHeader)) {
      return rollback_failed_frame("frame header");
    }
    if (length > 0 && !write_all(payload, length, FailNextWrite::kPayload)) {
      return rollback_failed_frame("frame payload");
    }
  } catch (...) {
    // Real I/O error: roll back the partial frame before rethrowing so the
    // file keeps a clean logical EOF.
    restore_clean_eof(frame_start);
    throw;
  }

  // The frame is persisted; post-write sync failures belong to the commit
  // durability decision and must not roll the frame back.
  sync_file();
  return true;
}

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace neug
