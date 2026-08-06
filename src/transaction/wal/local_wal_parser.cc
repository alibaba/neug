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

#include "neug/transaction/wal/local_wal_parser.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <ostream>
#include <utility>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

[[noreturn]] void ThrowRecovery(WalRecoveryErrorKind kind,
                                const std::string& message) {
  THROW_WAL_RECOVERY_EXCEPTION(std::string("[") +
                               WalRecoveryErrorKindName(kind) + "] " + message);
}

/// RAII guard for one mmapped WAL file. Ownership is transferred to the
/// parser once the file is validated, so replay unit payload views stay
/// valid until the parser is closed.
class MappedWalFile {
 public:
  MappedWalFile(const std::string& path, size_t size) : size_(size) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ == -1) {
      THROW_IO_EXCEPTION("Failed to open wal file: " + path + ": " +
                         strerror(errno));
    }
    mapped_ = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (mapped_ == MAP_FAILED) {
      ::close(fd_);
      fd_ = -1;
      THROW_IO_EXCEPTION("Failed to mmap wal file: " + path + ": " +
                         strerror(errno));
    }
    path_ = path;
  }

  MappedWalFile(const MappedWalFile&) = delete;
  MappedWalFile& operator=(const MappedWalFile&) = delete;

  MappedWalFile(MappedWalFile&& other) noexcept
      : fd_(other.fd_),
        mapped_(other.mapped_),
        size_(other.size_),
        path_(std::move(other.path_)) {
    other.fd_ = -1;
    other.mapped_ = nullptr;
    other.size_ = 0;
  }

  ~MappedWalFile() {
    if (mapped_ != nullptr) {
      ::munmap(mapped_, size_);
    }
    if (fd_ != -1) {
      ::close(fd_);
    }
  }

  const std::string& path() const { return path_; }
  const uint8_t* data() const { return static_cast<const uint8_t*>(mapped_); }
  size_t size() const { return size_; }

 private:
  int fd_{-1};
  void* mapped_{nullptr};
  size_t size_;
  std::string path_;
};

/// Validates one WAL file against the v1 protocol and appends every complete
/// frame to @p units as zero-copy views into the mapping, which is kept
/// alive by @p mapped_files. Only the last candidate frame may be skipped as
/// crash residue when EOF truncates it; any other inconsistency throws a
/// typed recovery error.
void ValidateAndCollect(
    const std::string& path, std::vector<WalReplayUnit>& units,
    std::vector<std::unique_ptr<MappedWalFile>>& mapped_files) {
  const size_t file_size = std::filesystem::file_size(path);
  if (file_size == 0) {
    return;
  }
  MappedWalFile file(path, file_size);
  const uint8_t* data = file.data();
  // A committed frame occupies at least a header and a trailer; this bounds
  // the per-file unit growth.
  units.reserve(units.size() +
                file_size / (kWalFrameHeaderSize + kWalFrameTrailerSize));

  // ---- File header -----------------------------------------------------
  if (file_size < kWalFileHeaderSize) {
    ThrowRecovery(
        WalRecoveryErrorKind::kUnsupportedFormat,
        "wal file " + path + " has " + std::to_string(file_size) +
            " bytes, fewer than the " + std::to_string(kWalFileHeaderSize) +
            "-byte v1 file header; legacy or torn formats are not parsed");
  }
  WalFileHeader file_header;
  size_t consumed = 0;
  const auto header_status =
      DecodeWalFileHeader(data, file_size, file_header, consumed);
  switch (header_status) {
  case WalDecodeStatus::kOk:
    break;
  case WalDecodeStatus::kBadMagic:
    ThrowRecovery(
        WalRecoveryErrorKind::kUnsupportedFormat,
        "wal file " + path +
            " does not carry the v1 file magic; the legacy pre-v1 format "
            "is rejected instead of being silently parsed. Complete a "
            "checkpoint on the old binary so the wal directory is empty "
            "before upgrading.");
  case WalDecodeStatus::kBadVersion:
    ThrowRecovery(WalRecoveryErrorKind::kUnsupportedFormat,
                  "wal file " + path + " has unsupported format version " +
                      std::to_string(file_header.format_version) +
                      ", expected " + std::to_string(kWalFormatVersion));
  default:
    ThrowRecovery(WalRecoveryErrorKind::kCorruptedFrame,
                  "wal file " + path + " header is corrupted at offset 0: " +
                      WalDecodeStatusName(header_status));
  }

  // ---- Frames ------------------------------------------------------------
  size_t offset = kWalFileHeaderSize;
  while (offset < file_size) {
    const size_t remaining = file_size - offset;
    if (remaining < kWalFrameHeaderSize) {
      // Torn header write of the last frame: crash residue at EOF.
      break;
    }
    WalFrameHeader frame_header;
    size_t header_consumed = 0;
    const auto frame_status = DecodeWalFrameHeader(
        data + offset, remaining, frame_header, header_consumed);
    if (frame_status == WalDecodeStatus::kTruncated) {
      break;  // unreachable given the remaining check above
    }
    if (frame_status != WalDecodeStatus::kOk) {
      const WalRecoveryErrorKind kind =
          frame_status == WalDecodeStatus::kUnknownRecordKind
              ? WalRecoveryErrorKind::kUnknownRecordKind
              : WalRecoveryErrorKind::kCorruptedFrame;
      ThrowRecovery(kind, "wal file " + path + " frame at offset " +
                              std::to_string(offset) +
                              " failed header validation: " +
                              WalDecodeStatusName(frame_status));
    }

    const uint64_t frame_total =
        static_cast<uint64_t>(frame_header.header_size) +
        frame_header.payload_length + kWalFrameTrailerSize;
    if (frame_total > remaining) {
      // The last candidate frame is truncated by EOF: header, payload or
      // trailer never completed. This is crash residue, not corruption; the
      // transaction has no commit marker and is not replayed.
      break;
    }

    const uint8_t* header_bytes = data + offset;
    const uint8_t* payload = header_bytes + frame_header.header_size;
    const uint8_t* trailer_bytes = payload + frame_header.payload_length;

    WalFrameTrailer trailer;
    size_t trailer_consumed = 0;
    const auto trailer_status = DecodeWalFrameTrailer(
        trailer_bytes, kWalFrameTrailerSize, trailer, trailer_consumed);
    if (trailer_status != WalDecodeStatus::kOk) {
      ThrowRecovery(WalRecoveryErrorKind::kCorruptedFrame,
                    "wal file " + path + " frame at offset " +
                        std::to_string(offset) +
                        " has an invalid commit trailer: " +
                        WalDecodeStatusName(trailer_status));
    }
    const auto validate_status =
        ValidateWalFrame(frame_header, trailer, header_bytes, payload);
    if (validate_status != WalDecodeStatus::kOk) {
      ThrowRecovery(
          WalRecoveryErrorKind::kCorruptedFrame,
          "wal file " + path + " frame at offset " + std::to_string(offset) +
              " failed checksum/marker validation: " +
              WalDecodeStatusName(validate_status) + ", commit_timestamp=" +
              std::to_string(frame_header.commit_timestamp));
    }

    // Kind/payload constraints are enforced uniformly here, not inferred
    // from length or flags at the call sites.
    if (frame_header.record_kind == WalRecordKind::kCompact &&
        frame_header.payload_length != 0) {
      ThrowRecovery(WalRecoveryErrorKind::kCorruptedFrame,
                    "wal file " + path + " frame at offset " +
                        std::to_string(offset) +
                        ": kCompact frame must carry an empty payload, got " +
                        std::to_string(frame_header.payload_length) + " bytes");
    }
    if (frame_header.record_kind != WalRecordKind::kCompact &&
        frame_header.payload_length == 0) {
      ThrowRecovery(WalRecoveryErrorKind::kCorruptedFrame,
                    "wal file " + path + " frame at offset " +
                        std::to_string(offset) +
                        ": empty transactions never write frames, got an "
                        "empty-payload " +
                        WalRecordKindName(frame_header.record_kind) + " frame");
    }

    WalReplayUnit unit;
    unit.commit_timestamp = frame_header.commit_timestamp;
    unit.kind = frame_header.record_kind;
    unit.payload =
        std::string_view(reinterpret_cast<const char*>(payload),
                         static_cast<size_t>(frame_header.payload_length));
    unit.file_index = static_cast<uint32_t>(mapped_files.size());
    unit.source_offset = offset;
    units.push_back(std::move(unit));

    offset += static_cast<size_t>(frame_total);
  }

  // The file validated; keep it mapped so the payload views above outlive
  // this scope.
  mapped_files.push_back(std::make_unique<MappedWalFile>(std::move(file)));
}

}  // namespace

/// Mapping table kept by the parser so replay unit payload views stay valid.
struct LocalWalParser::MappedFiles {
  std::vector<std::unique_ptr<MappedWalFile>> files;
};

LocalWalParser::LocalWalParser(const std::string& wal_uri)
    : mapped_files_(std::make_unique<MappedFiles>()) {
  LocalWalParser::open(wal_uri);
}

LocalWalParser::~LocalWalParser() { close(); }

void LocalWalParser::open(const std::string& wal_uri) {
  close();
  auto wal_dir = get_wal_uri_path(wal_uri);
  if (!std::filesystem::exists(wal_dir)) {
    std::filesystem::create_directory(wal_dir);
  }

  // Collect regular files first; replay order is decided by commit
  // timestamp, never by directory enumeration order. Sorting only makes
  // error reporting deterministic.
  std::vector<std::string> paths;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file()) {
      paths.push_back(entry.path().string());
    }
  }
  std::sort(paths.begin(), paths.end());

  std::vector<WalReplayUnit> units;
  for (const auto& path : paths) {
    ValidateAndCollect(path, units, mapped_files_->files);
  }

  // All files are validated before anything is merged. Order strictly by
  // commit timestamp; gaps are allowed, duplicates are always rejected.
  std::sort(units.begin(), units.end(),
            [](const WalReplayUnit& lhs, const WalReplayUnit& rhs) {
              return lhs.commit_timestamp < rhs.commit_timestamp;
            });
  for (size_t i = 1; i < units.size(); ++i) {
    if (units[i].commit_timestamp == units[i - 1].commit_timestamp) {
      ThrowRecovery(
          WalRecoveryErrorKind::kDuplicateTimestamp,
          "duplicate commit timestamp " +
              std::to_string(units[i].commit_timestamp) + " found in " +
              mapped_files_->files[units[i - 1].file_index]->path() +
              " (offset " + std::to_string(units[i - 1].source_offset) +
              ") and " + mapped_files_->files[units[i].file_index]->path() +
              " (offset " + std::to_string(units[i].source_offset) + ")");
    }
  }

  if (!units.empty()) {
    last_ts_ = units.back().commit_timestamp;
  }
  replay_units_ = std::move(units);
}

void LocalWalParser::close() {
  // Release the units first: their payload views reference the mappings.
  replay_units_.clear();
  mapped_files_->files.clear();
  last_ts_ = 0;
}

uint32_t LocalWalParser::last_ts() const { return last_ts_; }

const std::vector<WalReplayUnit>& LocalWalParser::replay_units() const {
  return replay_units_;
}

const bool LocalWalParser::registered_ = WalParserFactory::RegisterWalParser(
    "file", static_cast<WalParserFactory::wal_parser_initializer_t>(
                &LocalWalParser::Make));

}  // namespace neug
